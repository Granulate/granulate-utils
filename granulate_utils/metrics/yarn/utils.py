import glob
import logging
import os
import re
import socket
import subprocess
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import psutil

REGEX_YARN_VAR = re.compile(r"\${([^}]+)}")
RM_HIGH_AVAILABILITY_ENABLED_PROPERTY_KEY = "yarn.resourcemanager.ha.enabled"
RM_HIGH_AVAILABILITY_IDS_PROPERTY_KEY = "yarn.resourcemanager.ha.rm-ids"
RM_HOSTNAME_PROPERTY_KEY = "yarn.resourcemanager.hostname"
RM_WEB_ADDRESS_PROPERTY_KEY = "yarn.resourcemanager.webapp.address"
RM_DEFAULTS = {
    RM_HOSTNAME_PROPERTY_KEY: "0.0.0.0",
    RM_WEB_ADDRESS_PROPERTY_KEY: "${yarn.resourcemanager.hostname}:8088",
    RM_HIGH_AVAILABILITY_ENABLED_PROPERTY_KEY: "false",
}

RM_DEFAULT_ADDRESS = "http://0.0.0.0:8088"
WORKER_ADDRESS = "http://0.0.0.0:8042"

YARN_HOME_DIR_KEY = "yarn.home.dir="
YARN_HOME_DIR_KEY_LEN = len(YARN_HOME_DIR_KEY)
HADOOP_YARN_HOME_ENV_VAR = "HADOOP_YARN_HOME"

RELATIVE_YARN_SITE_XML_PATH = "./etc/hadoop/yarn-site.xml"
SENSITIVE_KEYS = ("password", "secret", "keytab", "principal")
MASK = "*****"


def mask_sensitive_value(key: str, value: Any) -> Any:
    """
    Mask sensitive info
    """
    key = key.lower()
    return MASK if any(k in key for k in SENSITIVE_KEYS) else value


class YarnConfigError(Exception):
    pass


class YarnNodeNotAResourceManagerError(Exception):
    pass


@dataclass(frozen=True)
class YarnNodeInfo:
    """YARN node information

    Args:
        config: YARN config from yarn-site.xml.

        resource_manager_webapp_addresses:
          When high-availability mode is enabled list of all ResourceManager webapp addresses,
          otherwise list with single RM address.

        resource_manager_index:
          Set only if node is a ResourceManager.
    """

    config: Dict[str, str]
    resource_manager_webapp_addresses: List[str]
    resource_manager_index: Optional[int] = None

    @cached_property
    def is_resource_manager(self) -> bool:
        return self.resource_manager_index is not None

    def get_own_resource_manager_webapp_address(self) -> str:
        if self.resource_manager_index is not None:
            return self.resource_manager_webapp_addresses[self.resource_manager_index]
        raise YarnNodeNotAResourceManagerError("This node is not a resource manager")

    @cached_property
    def is_first_resource_manager(self) -> bool:
        return self.resource_manager_index == 0

    @cached_property
    def first_resource_manager_webapp_address(self) -> str:
        return self.resource_manager_webapp_addresses[0]


def get_yarn_node_info(
    *,
    logger: Union[logging.Logger, logging.LoggerAdapter],
    yarn_config: Optional[Dict[str, str]] = None,
    hostname: Optional[str] = None,
    ip: Optional[str] = None,
) -> Optional[YarnNodeInfo]:
    """
    If running on YARN return YARN node information
    """
    config = detect_yarn_config(logger=logger) if yarn_config is None else yarn_config
    if config is None:
        return None
    if rm_addresses := get_resource_manager_addresses(config, logger=logger):
        return YarnNodeInfo(
            resource_manager_index=get_rm_index(rm_addresses, logger=logger, hostname=hostname, ip=ip),
            resource_manager_webapp_addresses=rm_addresses,
            config=config,
        )
    return None


def get_rm_index(
    rm_addresses: List[str],
    *,
    logger: Union[logging.Logger, logging.LoggerAdapter],
    hostname: Optional[str] = None,
    ip: Optional[str] = None,
) -> Optional[int]:
    """
    Return ResourceManager index for given hostname or ip

    If hostname or ip are not provided, local hostname and ip are used.
    """
    _hostname = hostname or os.uname()[1]
    _ip = ip or _get_local_ip(logger=logger)
    for i, address in enumerate(rm_addresses):
        rm_host = address.rsplit(":", 1)[0]
        if rm_host in ("0.0.0.0", _hostname, _ip) or rm_host.startswith(f"{_hostname}."):
            return i
    return None


def get_resource_manager_addresses(
    yarn_config: Dict[str, str], *, logger: Union[logging.Logger, logging.LoggerAdapter]
) -> Optional[List[str]]:
    """
    Return all ResourceManager addresses from YARN config

    If high-availability mode is disabled always returns single address.
    """
    try:
        config = {**RM_DEFAULTS, **yarn_config}
        # multiple RMs in high-availability mode
        if config.get(RM_HIGH_AVAILABILITY_ENABLED_PROPERTY_KEY) == "true":
            logger.debug("high availability enabled, looking for RM addresses")
            return get_all_rm_addresses(config)
        # single RM
        elif rm_address := config.get(RM_WEB_ADDRESS_PROPERTY_KEY):
            return [resolve_variables(config, rm_address)]
    except YarnConfigError as e:
        logger.error("YARN config error", extra={"error": str(e)})
    return None


def detect_yarn_config(*, logger: Union[logging.Logger, logging.LoggerAdapter]) -> Optional[Dict[str, str]]:
    """
    Look for yarn-site.xml and return YARN config when found
    """
    if yarn_home_dir := find_yarn_home_dir(logger=logger):
        logger.debug(f"found YARN home dir: {yarn_home_dir}")
        try:
            yarn_site_xml_file = glob.glob(f"{yarn_home_dir}/**/yarn-site.xml", recursive=True)[0]
        except IndexError:
            return None
        yarn_site_xml_file = Path(yarn_site_xml_file)
        if not yarn_site_xml_file.is_file():
            return None
        return read_config_file(yarn_site_xml_file, logger=logger)
    return None


def find_yarn_home_dir(*, logger: Union[logging.Logger, logging.LoggerAdapter]) -> Optional[str]:
    """
    Find YARN home directory from command line arguments
    """
    logger.debug("looking for yarn home dir")

    lines = subprocess.run(["ps", "-ax"], capture_output=True, text=True).stdout.split(" -D")
    for line in lines:
        if line.startswith(YARN_HOME_DIR_KEY) and (home_dir := line[YARN_HOME_DIR_KEY_LEN:].strip()):
            if Path(home_dir).is_dir():
                return home_dir

    # fallback to search yarn home dir in environment variables
    for process in psutil.process_iter():
        try:
            for env_key, env_val in process.environ().items():
                if HADOOP_YARN_HOME_ENV_VAR == env_key:
                    if Path(env_val).is_dir():
                        return env_val
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            pass

    logger.error("Could not find yarn home dir")
    return None


def read_config_file(
    xml_file: Path, *, logger: Union[logging.Logger, logging.LoggerAdapter]
) -> Optional[Dict[str, str]]:
    """
    Read YARN config from file
    """
    try:
        with open(xml_file, "r") as f:
            logger.debug(f"reading {xml_file}")
            return parse_config_xml(f.read())
    except FileNotFoundError:
        logger.error(f"file not found: {xml_file}")
    return None


def parse_config_xml(xml: str) -> Dict[str, str]:
    """
    Parse YARN config from XML string
    """
    root = ET.fromstring(xml)
    result = {}
    for p in root.findall("./property"):
        name = p.find("name")
        value = p.find("value")
        if name is not None and value is not None and (key := name.text):
            result[key] = value.text or ""
    return result


def get_all_rm_addresses(yarn_config: Dict[str, Any]) -> List[str]:
    """
    Return all ResourceManager addresses from high-availability mode configuration
    """
    rm_ids = yarn_config.get(RM_HIGH_AVAILABILITY_IDS_PROPERTY_KEY)
    if not rm_ids:
        raise YarnConfigError(f"no {RM_HIGH_AVAILABILITY_IDS_PROPERTY_KEY} found")
    result = []
    for rm_id in map(str.strip, rm_ids.split(",")):
        webapp_address = yarn_config.get(
            f"{RM_WEB_ADDRESS_PROPERTY_KEY}.{rm_id}",
            f"${{{RM_HOSTNAME_PROPERTY_KEY}.{rm_id}}}:8088",
        )
        result.append(resolve_variables(yarn_config, webapp_address))
    return result


def resolve_variables(yarn_config: Dict[str, Any], value: str) -> str:
    """
    Resolve all variables in YARN config value

    e.g.
        properties = {
            "yarn.resourcemanager.hostname": "0.0.0.0",
        }
        value = "${yarn.resourcemanager.hostname}:8088"
        assert resolve_config_value(properties, value) == "0.0.0.0:8088"
    """
    while m := REGEX_YARN_VAR.search(value):
        key = m.group(1)
        if (val := yarn_config.get(key)) is None:
            raise YarnConfigError(f"could not resolve variable: {key}")
        value = value.replace(f"${{{key}}}", val)
    return value


def get_yarn_properties(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return only YARN properties
    """
    resource_names = ("yarn-site.xml", "programmatically", "Dataproc Cluster Properties")
    return {
        "properties": filter_properties(
            config,
            lambda x: x["resource"] in resource_names and x["key"].startswith("yarn."),
        )
    }


def get_all_properties(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return all properties
    """
    return {"properties": filter_properties(config, lambda x: True)}


def filter_properties(
    config: Dict[str, Any],
    predicate: Callable[[Dict[str, Any]], bool],
) -> List[Dict[str, Any]]:
    """
    Return only properties that match the predicate
    """
    result = []
    for prop in config["properties"]:
        if predicate(prop):
            key = prop["key"]
            result.append(
                {
                    "key": key,
                    "value": mask_sensitive_value(key, prop["value"]),
                    "resource": prop["resource"],
                }
            )
    return result


def _get_local_ip(*, logger: Union[logging.Logger, logging.LoggerAdapter]) -> str:
    """
    Returns local IP address

    No packets are sent.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.settimeout(60)
        s.connect(("8.8.8.8", 53))
        local_ip: str = s.getsockname()[0]
        return local_ip
    except socket.error:
        logger.exception("Failed retrieving the local ip")
        return "unknown"
    finally:
        s.close()

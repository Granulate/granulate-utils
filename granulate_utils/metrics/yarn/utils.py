import logging
import re
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from granulate_utils.config_feeder.core.utils import mask_sensitive_value

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


class YarnConfigError(Exception):
    pass


def detect_resource_manager_addresses(*, logger: Union[logging.Logger, logging.LoggerAdapter]) -> Optional[List[str]]:
    """
    Look for ResourceManager address in yarn-site.xml

    If high-availability mode is enabled returns all addresses, otherwise returns single address
    """
    if yarn_home_dir := find_yarn_home_dir(logger=logger):
        logger.debug(f"found YARN home dir: {yarn_home_dir}")
        yarn_site_xml_file = Path(yarn_home_dir).joinpath("./etc/hadoop/yarn-site.xml")
        logger.debug(f"looking for {RM_WEB_ADDRESS_PROPERTY_KEY} in {yarn_site_xml_file}")
        config = read_config_file(yarn_site_xml_file, logger=logger)
        try:
            # multiple RMs in high-availability mode
            if config.get(RM_HIGH_AVAILABILITY_ENABLED_PROPERTY_KEY) == "true":
                logger.debug("high availability enabled, looking for RM addresses")
                return get_all_rm_addresses(config, logger=logger)
            # single RM
            elif rm_address := config.get(RM_WEB_ADDRESS_PROPERTY_KEY):
                return [resolve_variables(config, rm_address)]
        except YarnConfigError as e:
            logger.error("YARN config error", extra={"error": str(e)})
    return None


def find_yarn_home_dir(*, logger: Union[logging.Logger, logging.LoggerAdapter]) -> Optional[str]:
    """
    Find YARN home directory from command line arguments
    """
    logger.debug("looking for running YARN processes")
    lines = subprocess.run(["ps", "-ax"], capture_output=True, text=True).stdout.split(" -D")
    home_dir_key = "yarn.home.dir="
    for line in lines:
        if line.startswith(home_dir_key) and (home_dir := line[len(home_dir_key) :].strip()):
            return home_dir
    logger.error("no YARN processes found")
    return None


def read_config_file(xml_file: Path, *, logger: Union[logging.Logger, logging.LoggerAdapter]) -> Dict[str, str]:
    """
    Read YARN config from file
    """
    try:
        result = {**RM_DEFAULTS}
        with open(xml_file, "r") as f:
            root = ET.fromstring(f.read())
            for p in root.findall("./property"):
                name = p.find("name")
                value = p.find("value")
                if name is not None and value is not None and (key := name.text):
                    result[key] = value.text or ""
            return result
    except FileNotFoundError:
        logger.error(f"file not found: {xml_file}")
    return {}


def get_all_rm_addresses(
    yarn_config: Dict[str, Any], *, logger: Union[logging.Logger, logging.LoggerAdapter]
) -> List[str]:
    """
    Return all RM addresses in high-availability mode
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
    Resolve variables in YARN config value

    e.g.
        properties = {
            "yarn.resourcemanager.hostname": "0.0.0.0",
        }
        value = "${yarn.resourcemanager.hostname}:8088"
        assert resolve_config_value(properties, value) == "0.0.0.0:8088"
    """
    while m := REGEX_YARN_VAR.search(value):
        key = m.group(1)
        val = yarn_config.get(key)
        if not val:
            raise YarnConfigError(f"could not resolve variable: {key}")
        value = value.replace(f"${{{key}}}", val)
    return value


def get_yarn_properties(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return only YARN properties
    """
    resource_names = ("yarn-site.xml", "programmatically")
    return {
        "properties": _get_properties(
            config,
            lambda x: x["resource"] in resource_names and x["key"].startswith("yarn."),
        )
    }


def _get_properties(
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

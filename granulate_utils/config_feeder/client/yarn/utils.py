import asyncio
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from granulate_utils.config_feeder.client.logging import get_logger

REGEX_YARN_VAR = re.compile(r"\${([^}]+)}")

RM_DEFAULTS = {
    "yarn.resourcemanager.hostname": "0.0.0.0",
    "yarn.resourcemanager.webapp.address": "${yarn.resourcemanager.hostname}:8088",
}
RM_DEFAULT_ADDRESS = "http://localhost:8088"
WORKER_ADDRESS = "http://0.0.0.0:8042"

RM_ADDRESS_PROPERTY_KEY = "yarn.resourcemanager.webapp.address"

SENSITIVE_KEYS = ("password", "secret", "keytab", "principal")
MASK = "*****"

logger = get_logger()


async def detect_resource_manager_address() -> Optional[str]:
    """
    Look for ResourceManager address in yarn-site.xml
    """
    if yarn_home_dir := await _find_yarn_home_dir():
        logger.debug(f"found YARN home dir: {yarn_home_dir}")
        yarn_site_xml_file = Path(yarn_home_dir).joinpath("./etc/hadoop/yarn-site.xml")
        logger.debug(f"looking for {RM_ADDRESS_PROPERTY_KEY} in {yarn_site_xml_file}")
        config = _read_config_file(yarn_site_xml_file)
        if rm_host := config.get(RM_ADDRESS_PROPERTY_KEY):
            return _resolve_variables(config, rm_host)
    return None


async def _find_yarn_home_dir() -> Optional[str]:
    """
    Find YARN home directory from command line arguments
    """
    logger.debug("looking for running YARN processes")

    process = await asyncio.create_subprocess_shell(
        "ps -ax", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await process.communicate()
    lines = stdout.decode().split("-Dyarn.")
    home_dir_key = "home.dir="
    for line in lines:
        if line.startswith(home_dir_key):
            return line[len(home_dir_key) :].strip()
    logger.error("no YARN processes found")
    return None


def _read_config_file(xml_file: Path) -> Dict[str, str]:
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


def _resolve_variables(config: Dict[str, Any], value: str) -> str:
    """
    Resolve variables in config value

    e.g.
        properties = {
            "yarn.resourcemanager.hostname": "0.0.0.0",
        }
        value = "${yarn.resourcemanager.hostname}:8088"
        assert resolve_config_value(properties, value) == "0.0.0.0:8088"
    """
    while m := REGEX_YARN_VAR.search(value):
        key = m.group(1)
        val = config.get(key)
        if not val:
            logger.warning(f"could not resolve variable: {key}")
            return value
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


def _get_properties(config: Dict[str, Any], predicate: Callable[[Dict[str, Any]], bool]) -> List[Dict[str, Any]]:
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
                    "value": _mask_sensitive_value(key, prop["value"]),
                    "resource": prop["resource"],
                }
            )
    return result


def _mask_sensitive_value(key: str, value: Any) -> Any:
    """
    Mask sensitive info
    """
    return MASK if any(k in key for k in SENSITIVE_KEYS) else value

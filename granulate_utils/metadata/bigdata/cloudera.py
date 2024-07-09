import logging
from typing import TYPE_CHECKING, List, Optional

from ...linux.ns import resolve_host_root_links

VERSION_KEY = "version="

if TYPE_CHECKING:
    _LoggerAdapter = logging.LoggerAdapter[logging.Logger]
else:
    _LoggerAdapter = logging.LoggerAdapter


def _get_agent_properties() -> Optional[List[str]]:
    try:
        with open(resolve_host_root_links("/opt/cloudera/cm-agent/cm_version.properties"), "r") as f:
            return f.readlines()
    except (FileNotFoundError, PermissionError):
        pass
    return None


def get_cloudera_version() -> Optional[str]:
    if line := next((x for x in _get_agent_properties() or [] if x.startswith(VERSION_KEY)), None):
        return line[len(VERSION_KEY) :].strip()
    return None

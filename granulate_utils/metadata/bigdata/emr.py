import json
import logging
from typing import TYPE_CHECKING, Dict, Optional, Union

from ...linux.ns import resolve_host_root_links

if TYPE_CHECKING:
    _LoggerAdapter = logging.LoggerAdapter[logging.Logger]
else:
    _LoggerAdapter = logging.LoggerAdapter


def _get_instance_data() -> Optional[Dict[str, str]]:
    try:
        with open(resolve_host_root_links("/proc/1/root/mnt/var/lib/info/extraInstanceData.json"), "r") as f:
            obj = json.loads(f.read())
            if isinstance(obj, dict):
                return obj
    except FileNotFoundError:
        pass
    return None


def get_emr_version() -> Optional[str]:
    if (data := _get_instance_data()) is not None:
        release = data.get("releaseLabel")
        if isinstance(release, str):
            return release
    return None


def get_hadoop_version(logger: Optional[Union[logging.Logger, _LoggerAdapter]]) -> Optional[str]:
    """
    Get the running hadoop version.

    Sample value from extraInstanceData.json: "Hadoop_3_2_1"
    """
    if (data := _get_instance_data()) is not None:
        hadoop_version = data.get("hadoopVersion")
        if isinstance(hadoop_version, str):
            try:
                return hadoop_version.split("_", 1)[1].replace("_", ".")
            except IndexError:
                if logger:
                    logger.error("Failed to get hadoop version", exc_info=True)
    return None

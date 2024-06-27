from typing import List, Optional

from granulate_utils.metadata.bigdata.shared import get_hadoop_version

__all__ = ["get_dataproc_version", "get_hadoop_version"]

VERSION_KEY = "DATAPROC_IMAGE_VERSION="


def _get_environment_info() -> Optional[List[str]]:
    try:
        with open("/etc/environment", "r") as f:
            return f.readlines()
    except FileNotFoundError:
        pass
    return None


def get_dataproc_version() -> Optional[str]:
    if line := next((x for x in _get_environment_info() or [] if x.startswith(VERSION_KEY)), None):
        return line[len(VERSION_KEY) :].strip()
    return None

import logging
import subprocess
from functools import partial
from typing import TYPE_CHECKING, List, Optional, Union, cast

from granulate_utils.linux.ns import resolve_host_root_links, run_in_ns

VERSION_KEY = "DATAPROC_IMAGE_VERSION="
HADOOP_VERSION_CMD = "hadoop version"

if TYPE_CHECKING:
    _LoggerAdapter = logging.LoggerAdapter[logging.Logger]
else:
    _LoggerAdapter = logging.LoggerAdapter


def _get_environment_info() -> Optional[List[str]]:
    try:
        with open(resolve_host_root_links("/etc/environment"), "r") as f:
            return f.readlines()
    except (FileNotFoundError, PermissionError):
        pass
    return None


def get_hadoop_version(logger: Optional[Union[logging.Logger, _LoggerAdapter]]) -> Optional[str]:
    """
    Get the running hadoop version.

    Sample cmd output:
    >> Hadoop 2.9.2
    >> Subversion https://bigdataoss-internal.googlesource.com/third_party/apache/hadoop -r b3e52921b6aaf2c68af220021eab42975114f7cb # noqa 501
    >> Compiled by bigtop on 2021-07-12T21:30Z
    >> Compiled with protoc 2.5.0
    >> From source with checksum 3ea0fd8f2b9af855fb9f66c2e3130e3
    >> This command was run using /usr/lib/hadoop/hadoop-common-2.9.2.jar

    Extract the version from the first line.
    """
    try:
        to_run = partial(subprocess.check_output, [HADOOP_VERSION_CMD], shell=True, stderr=subprocess.STDOUT)
        version_output = cast(bytes, run_in_ns(["mnt"], to_run)).splitlines()[0].decode("utf-8")
        return version_output.split(" ")[1]
    except (subprocess.CalledProcessError, IndexError):
        if logger:
            logger.error("Failed to get hadoop version", exc_info=True)
    return None


def get_dataproc_version() -> Optional[str]:
    if line := next((x for x in _get_environment_info() or [] if x.startswith(VERSION_KEY)), None):
        return line[len(VERSION_KEY) :].strip()
    return None

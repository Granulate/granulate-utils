import subprocess
from typing import List, Optional

VERSION_KEY = "DATAPROC_IMAGE_VERSION="
HADOOP_VERSION_CMD = "hadoop version"


def _get_environment_info() -> Optional[List[str]]:
    try:
        with open("/etc/environment", "r") as f:
            return f.readlines()
    except FileNotFoundError:
        pass
    return None


def get_hadoop_version() -> Optional[str]:
    """
    Get the running hadoop version.

    Sample cmd output:
    >> Hadoop 2.9.2
    >> Subversion https://bigdataoss-internal.googlesource.com/third_party/apache/hadoop -r b3e52921b6aaf2c68af220021eab42975114f7cb
    >> Compiled by bigtop on 2021-07-12T21:30Z
    >> Compiled with protoc 2.5.0
    >> From source with checksum 3ea0fd8f2b9af855fb9f66c2e3130e3
    >> This command was run using /usr/lib/hadoop/hadoop-common-2.9.2.jar

    Extract the version from the first line.
    """
    try:
        version_output = (
            subprocess.check_output([HADOOP_VERSION_CMD], shell=True, stderr=subprocess.STDOUT)
            .splitlines()[0]
            .decode("utf-8")
        )
        return version_output.split(" ")[1]
    except IndexError:
        pass
    return None


def get_dataproc_version() -> Optional[str]:
    if line := next((x for x in _get_environment_info() or [] if x.startswith(VERSION_KEY)), None):
        return line[len(VERSION_KEY) :].strip()
    return None

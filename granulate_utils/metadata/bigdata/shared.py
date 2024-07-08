import subprocess
from typing import Optional

from granulate_utils.metadata.bigdata.logging import LoggerOrAdapter

HADOOP_VERSION_CMD = "hadoop version"


def get_hadoop_version(logger: Optional[LoggerOrAdapter]) -> Optional[str]:
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
        version_output = (
            subprocess.check_output([HADOOP_VERSION_CMD], shell=True, stderr=subprocess.STDOUT)
            .splitlines()[0]
            .decode("utf-8")
        )
        return version_output.split(" ")[1]
    except (subprocess.CalledProcessError, IndexError):
        if logger:
            logger.error("Failed to get hadoop version", exc_info=True)
    return None

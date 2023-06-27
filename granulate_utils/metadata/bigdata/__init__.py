from dataclasses import dataclass
from typing import Optional

from granulate_utils.metadata.bigdata.databricks import get_databricks_version
from granulate_utils.metadata.bigdata.dataproc import get_dataproc_version
from granulate_utils.metadata.bigdata.emr import get_emr_version


@dataclass
class BigDataInfo:
    provider: str
    provider_version: str


def get_bigdata_info() -> Optional[BigDataInfo]:
    """
    Before applying any change, please consider that this function should be non-blocking, and fairly quick.
    """
    if emr_version := get_emr_version():
        return BigDataInfo("emr", emr_version)
    elif databricks_version := get_databricks_version():
        return BigDataInfo("databricks", databricks_version)
    elif dataproc_version := get_dataproc_version():
        return BigDataInfo("dataproc", dataproc_version)
    return None

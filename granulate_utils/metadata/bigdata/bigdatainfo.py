from typing import Optional

from granulate_utils.metadata.bigdata.cloudera import get_cloudera_version
from granulate_utils.metadata.bigdata.databricks import get_databricks_version, is_databricks
from granulate_utils.metadata.bigdata.dataproc import get_dataproc_version
from granulate_utils.metadata.bigdata.emr import get_emr_version
from granulate_utils.metadata.bigdata.interfaces import BigDataInfo


def get_bigdata_info() -> Optional[BigDataInfo]:
    """
    Before applying any change, please consider that this function should be non-blocking, and fairly quick.
    """
    if emr_version := get_emr_version():
        return BigDataInfo("emr", emr_version)
    elif databricks_version := get_databricks_version():
        return BigDataInfo("databricks", databricks_version)
    elif is_databricks():
        return BigDataInfo("databricks", "unknown")
    elif dataproc_version := get_dataproc_version():
        return BigDataInfo("dataproc", dataproc_version)
    elif cloudera_version := get_cloudera_version():
        return BigDataInfo("cloudera", cloudera_version)
    return None

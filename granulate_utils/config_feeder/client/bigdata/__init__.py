from functools import lru_cache
from typing import Optional

from granulate_utils.config_feeder.client.bigdata.databricks import get_databricks_node_info
from granulate_utils.config_feeder.client.bigdata.dataproc import get_dataproc_node_info
from granulate_utils.config_feeder.client.bigdata.emr import get_emr_node_info
from granulate_utils.config_feeder.core.models.node import NodeInfo

__all__ = ["get_node_info"]


@lru_cache(maxsize=None)
def get_node_info() -> Optional[NodeInfo]:
    if emr_node_info := get_emr_node_info():
        return emr_node_info
    if databricks_node_info := get_databricks_node_info():
        return databricks_node_info
    if dataproc_node_info := get_dataproc_node_info():
        return dataproc_node_info
    return None

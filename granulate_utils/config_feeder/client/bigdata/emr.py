import json
from typing import Dict, Optional

from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo
from granulate_utils.metadata.bigdata import get_emr_version
from granulate_utils.metadata.bigdata.emr import get_hadoop_version


def get_emr_node_info() -> Optional[NodeInfo]:
    """
    Returns EMR node info

    tested on EMR 5.20.1, 6.11.0
    """
    if emr_info := _get_emr_job_info():
        return NodeInfo(
            external_id=_get_instance_id(),
            external_cluster_id=emr_info["jobFlowId"],
            is_master=_get_is_master(),
            provider=CloudProvider.AWS,
            bigdata_platform=BigDataPlatform.EMR,
            bigdata_platform_version=get_emr_version(),
            hadoop_version=get_hadoop_version(),
        )
    return None


def _get_emr_job_info() -> Optional[Dict[str, str]]:
    try:
        with open("/mnt/var/lib/info/job-flow.json", "r") as f:
            obj = json.load(f)
            if isinstance(obj, dict):
                return obj
    except FileNotFoundError:
        pass
    return None


def _get_instance_id() -> str:
    with open("/mnt/var/lib/cloud/data/instance-id", "r") as f:
        return f.read().strip()


def _get_is_master() -> bool:
    with open("/mnt/var/lib/info/instance.json", "r") as f:
        obj = json.load(f)
        result: bool = obj["isMaster"]
        return result

import json
from typing import Dict, Optional

from granulate_utils.config_feeder.core.models.node import NodeInfo


def get_emr_node_info() -> Optional[NodeInfo]:
    if emr_info := _get_emr_job_info():
        return NodeInfo(
            external_id=_get_instance_id(),
            external_cluster_id=emr_info["jobFlowId"],
            is_master=_get_is_master(),
            provider="aws",
            bigdata_platform="emr",
        )
    return None


def _get_emr_job_info() -> Optional[Dict[str, str]]:
    try:
        with open("/mnt/var/lib/info/job-flow.json", "r") as f:
            obj = json.loads(f.read())
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
        obj = json.loads(f.read())
        result: bool = obj["isMaster"]
        return result

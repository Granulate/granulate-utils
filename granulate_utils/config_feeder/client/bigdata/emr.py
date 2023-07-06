import asyncio
import json
import logging
from typing import Any, Dict, Optional, Union

from granulate_utils.config_feeder.core.models.autoscaling import AutoScalingConfig, AutoScalingMode
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo


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
        )
    return None


async def get_emr_autoscaling_config(
    node: NodeInfo, *, logger: Union[logging.Logger, logging.LoggerAdapter]
) -> Optional[AutoScalingConfig]:
    result = {}
    if (cluster_info := await _run_emr_command(node, "describe-cluster", logger=logger)) is not None:
        for group in cluster_info.get("Cluster", {}).get("InstanceGroups", []):
            if "AutoScalingPolicy" in group:
                policy = group["AutoScalingPolicy"]
                result[group["Id"]] = {
                    "instance_group_type": group["InstanceGroupType"],
                    "constraints": policy["Constraints"],
                    "rules": policy["Rules"],
                }
    else:
        logger.error("failed to get EMR cluster info")
        return None
    if result:
        return AutoScalingConfig(mode=AutoScalingMode.CUSTOM, config=result)
    if managed_policy := await _run_emr_command(node, "get-managed-scaling-policy", logger=logger):
        return AutoScalingConfig(mode=AutoScalingMode.MANAGED, config=managed_policy)
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


async def _run_emr_command(
    node: NodeInfo, command: str, *, logger: Union[logging.Logger, logging.LoggerAdapter]
) -> Optional[Dict[str, Any]]:
    cmd = f"aws emr {command} --cluster-id {node.external_cluster_id}"
    process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        logger.error("failed to run EMR command", extra={"command": command, "stderr": stderr.decode()})
        return None
    return json.loads(stdout.decode().strip())

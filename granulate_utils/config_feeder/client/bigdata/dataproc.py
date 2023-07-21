import asyncio
import json
import logging
from typing import Any, Dict, Optional, Union

import requests
from requests.exceptions import ConnectionError, JSONDecodeError

from granulate_utils.config_feeder.core.models.autoscaling import AutoScalingConfig, AutoScalingMode
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo
from granulate_utils.metadata.bigdata import get_dataproc_version


def get_dataproc_node_info(logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None) -> Optional[NodeInfo]:
    """
    Returns Dataproc node info

    https://cloud.google.com/compute/docs/metadata/querying-metadata
    https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/metadata

    tested on 1.4, 2.0
    """
    try:
        metadata = _get_metadata()
        attributes = metadata["attributes"]
        properties = {
            "region": attributes["dataproc-region"],
            "cluster_name": attributes["dataproc-cluster-name"],
        }
        return NodeInfo(
            external_id=str(metadata["id"]),
            external_cluster_id=attributes["dataproc-cluster-uuid"],
            is_master=attributes["dataproc-role"] == "Master",
            provider=CloudProvider.GCP,
            bigdata_platform=BigDataPlatform.DATAPROC,
            bigdata_platform_version=get_dataproc_version(),
            properties=properties,
        )
    except JSONDecodeError:
        if logger:
            logger.error("got invalid dataproc metadata JSON")
    except KeyError as e:
        if logger:
            logger.error("expected dataproc metadata key was not found", extra={"key": e.args[0]})
    except ConnectionError:
        pass
    return None


async def get_dataproc_autoscaling_config(
    node: NodeInfo, *, logger: Union[logging.Logger, logging.LoggerAdapter]
) -> Optional[AutoScalingConfig]:
    if (
        cluster_info := await _run_gcloud_command(
            node,
            f"dataproc clusters describe {node.properties['cluster_name']}",
            logger=logger,
        )
    ) is None:
        logger.error("failed to get cluster info")
        return None
    if policy_url := cluster_info.get("config", {}).get("autoscalingConfig", {}).get("policyUri"):
        if (
            policy := await _run_gcloud_command(
                node, f"dataproc autoscaling-policies describe {policy_url}", logger=logger
            )
        ) is not None:
            return AutoScalingConfig(mode=AutoScalingMode.CUSTOM, config=policy)
        else:
            logger.error("failed to get autoscaling policy")
    return None


def _get_metadata() -> Dict[str, Any]:
    url = "http://metadata.google.internal/computeMetadata/v1/instance/?recursive=true"  # noqa: E501
    headers = {"Metadata-Flavor": "Google"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


async def _run_gcloud_command(
    node: NodeInfo, command: str, *, logger: Union[logging.Logger, logging.LoggerAdapter]
) -> Optional[Dict[str, Any]]:
    cmd = f"gcloud {command} --region={node.properties['region']} --format=json"
    process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        logger.error("failed to run gcloud command", extra={"command": command, "stderr": stderr.decode()})
        return None
    return json.loads(stdout.decode().strip())

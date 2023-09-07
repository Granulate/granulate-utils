import logging
import os
import re
from typing import Dict, List, Optional, Union

from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo
from granulate_utils.config_feeder.core.utils import mask_sensitive_value
from granulate_utils.metadata.bigdata import get_databricks_version
from granulate_utils.metadata.bigdata.databricks import get_hadoop_version

INSTANCE_KEY_PREFIX = "databricks.instance.metadata"
CLUSTER_KEY_PREFIX = "spark.databricks.clusterUsageTags.cluster"
DRIVER_KEY_PREFIX = "spark.databricks.clusterUsageTags.driver"

KEY_CLOUD_PROVIDER = f"{INSTANCE_KEY_PREFIX}.cloudProvider"
KEY_INSTANCE_ID = f"{INSTANCE_KEY_PREFIX}.instanceId"
KEY_CLUSTER_ID = f"{CLUSTER_KEY_PREFIX}Id"
KEY_DRIVER_INSTANCE_ID = f"{DRIVER_KEY_PREFIX}InstanceId"

REGEX_CLUSTER_ID = r"(\d{4}-\d{6}-\w{8})"


def get_databricks_node_info(
    logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None
) -> Optional[NodeInfo]:
    """
    Returns Databricks node info
    """
    if properties := _get_deploy_conf():
        instance_id = properties[KEY_INSTANCE_ID]
        driver_instance_id = properties.get(KEY_DRIVER_INSTANCE_ID, "")
        provider = _resolve_cloud_provider(properties.get(KEY_CLOUD_PROVIDER, "unknown"))
        if external_cluster_id := _resolve_cluster_id(properties):
            return NodeInfo(
                external_id=instance_id,
                external_cluster_id=external_cluster_id,
                is_master=(instance_id == driver_instance_id),
                provider=provider,
                bigdata_platform=BigDataPlatform.DATABRICKS,
                bigdata_platform_version=get_databricks_version(),
                hadoop_version=get_hadoop_version(),
                properties=_exclude_keys(properties, [KEY_CLOUD_PROVIDER, KEY_INSTANCE_ID, KEY_CLUSTER_ID]),
            )
        elif logger:
            logger.error("cannot resolve cluster id")
    return None


def _get_deploy_conf() -> Optional[Dict[str, str]]:
    """
    Reads dataproc properties from deploy.conf

    e.g.

      spark.databricks.clusterUsageTags.clusterId = "0523-113117-1f8u0192"
      databricks.instance.metadata.instanceId = "i-000e86ee86c521650"
    """
    try:
        with open("/databricks/common/conf/deploy.conf", "r") as f:
            result = {}
            for line in f.readlines():
                line = line.strip()
                if (
                    line.startswith(INSTANCE_KEY_PREFIX)
                    or line.startswith(CLUSTER_KEY_PREFIX)
                    or line.startswith(DRIVER_KEY_PREFIX)
                ):
                    key, value = line.split("=")
                    result[key.strip()] = mask_sensitive_value(key, value.strip('" '))
            return result
    except FileNotFoundError:
        pass
    return None


def _resolve_cluster_id(properties: Dict[str, str]) -> Optional[str]:
    """
    If clusterId is not available in deploy.conf, try to extract it from hostname

    e.g. 0817-103940-91u12104-10-26-238-244 -> 0817-103940-91u12104
    """
    if KEY_CLUSTER_ID in properties:
        return properties[KEY_CLUSTER_ID]
    if match := re.search(REGEX_CLUSTER_ID, os.uname()[1]):
        return match.group(1)
    return None


def _resolve_cloud_provider(provider: str) -> CloudProvider:
    if provider == "AWS":
        return CloudProvider.AWS
    elif provider == "GCP":
        return CloudProvider.GCP
    return CloudProvider.UNKNOWN


def _exclude_keys(properties: Dict[str, str], keys: List[str]) -> Dict[str, str]:
    return {k: v for k, v in properties.items() if k not in keys}

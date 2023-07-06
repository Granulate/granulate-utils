from typing import Dict, List, Optional

from granulate_utils.config_feeder.core.models.autoscaling import AutoScalingConfig, AutoScalingMode
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo
from granulate_utils.config_feeder.core.utils import mask_sensitive_value

INSTANCE_KEY_PREFIX = "databricks.instance.metadata"
CLUSTER_KEY_PREFIX = "spark.databricks.clusterUsageTags.cluster"
DRIVER_KEY_PREFIX = "spark.databricks.clusterUsageTags.driver"

KEY_CLOUD_PROVIDER = f"{INSTANCE_KEY_PREFIX}.cloudProvider"
KEY_INSTANCE_ID = f"{INSTANCE_KEY_PREFIX}.instanceId"
KEY_CLUSTER_ID = f"{CLUSTER_KEY_PREFIX}Id"
KEY_DRIVER_INSTANCE_ID = f"{DRIVER_KEY_PREFIX}InstanceId"


def get_databricks_node_info() -> Optional[NodeInfo]:
    """
    Returns Databricks node info
    """
    if properties := _get_deploy_conf():
        instance_id = properties[KEY_INSTANCE_ID]
        driver_instance_id = properties.get(KEY_DRIVER_INSTANCE_ID, "")
        provider = _resolve_cloud_provider(properties.get(KEY_CLOUD_PROVIDER, "unknown"))
        external_cluster_id = properties[KEY_CLUSTER_ID]
        return NodeInfo(
            external_id=instance_id,
            external_cluster_id=external_cluster_id,
            is_master=(instance_id == driver_instance_id),
            provider=provider,
            bigdata_platform=BigDataPlatform.DATABRICKS,
            properties=_exclude_keys(properties, [KEY_CLOUD_PROVIDER, KEY_INSTANCE_ID, KEY_CLUSTER_ID]),
        )
    return None


async def get_databricks_autoscaling_config(
    node: NodeInfo,
) -> Optional[AutoScalingConfig]:
    if node.properties.get("spark.databricks.clusterUsageTags.clusterScalingType") == "autoscaling":
        return AutoScalingConfig(
            mode=AutoScalingMode.MANAGED,
            config={
                "min_workers": int(node.properties.get("spark.databricks.clusterUsageTags.clusterMinWorkers", -1)),
                "max_workers": int(node.properties.get("spark.databricks.clusterUsageTags.clusterMaxWorkers", -1)),
            },
        )
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


def _resolve_cloud_provider(provider: str) -> CloudProvider:
    if provider == "AWS":
        return CloudProvider.AWS
    elif provider == "GCP":
        return CloudProvider.GCP
    return CloudProvider.UNKNOWN


def _exclude_keys(properties: Dict[str, str], keys: List[str]) -> Dict[str, str]:
    return {k: v for k, v in properties.items() if k not in keys}

from typing import Dict, Optional

from granulate_utils.config_feeder.core.models.cluster import CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo

INSTANCE_KEY_PREFIX = "databricks.instance.metadata"
CLUSTER_KEY_PREFIX = "spark.databricks.clusterUsageTags"


def get_databricks_node_info() -> Optional[NodeInfo]:
    if properties := _get_deploy_conf():
        instance_id = properties["databricks.instance.metadata.instanceId"]
        driver_instance_id = properties.get("spark.databricks.clusterUsageTags.driverInstanceId")
        provider = _resolve_cloud_provider(properties.get("databricks.instance.metadata.cloudProvider", "unknown"))
        return NodeInfo(
            external_id=instance_id,
            external_cluster_id=properties["spark.databricks.clusterUsageTags.clusterId"],
            is_master=instance_id == driver_instance_id,
            provider=provider,
            bigdata_platform="databricks",
            properties=properties,
        )
    return None


def _get_deploy_conf() -> Optional[Dict[str, str]]:
    try:
        with open("/databricks/common/conf/deploy.conf", "r") as f:
            result = {}
            for line in f.readlines():
                line = line.strip()
                if line.startswith(INSTANCE_KEY_PREFIX) or line.startswith(CLUSTER_KEY_PREFIX):
                    key, value = line.split("=")
                    result[key.strip()] = value.strip()[1:-1]
            return result
    except FileNotFoundError:
        pass
    return None


def _resolve_cloud_provider(provider: str) -> CloudProvider:
    if provider == "AWS":
        return "aws"
    elif provider == "GCP":
        return "gcp"
    return "unknown"

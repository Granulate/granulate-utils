from __future__ import annotations

from granulate_utils.config_feeder.core.models.cluster import CloudProvider
from tests.granulate_utils.config_feeder.fixtures.base import NodeMockBase


class DatabricksNodeMock(NodeMockBase):
    def __init__(
        self,
        *,
        provider: CloudProvider = CloudProvider.AWS,
        cluster_id: str = "",
        hostname: str = "",
        instance_id: str = "",
        is_master: bool = False,
        version: str = "11.3",
    ) -> None:
        super().__init__()
        driver_instance_id = instance_id if is_master else "aaa"
        properties = {
            "databricks.instance.metadata.cloudProvider": provider.upper(),
            "databricks.instance.metadata.instanceId": instance_id,
            "spark.databricks.clusterUsageTags.clusterSomeSecretPassword": "password123",
        }

        if cluster_id:
            properties["spark.databricks.clusterUsageTags.clusterId"] = cluster_id

        if is_master:
            properties["spark.databricks.clusterUsageTags.driverInstanceId"] = driver_instance_id

        config = "\n".join([f'{k} = "{v}"' for k, v in properties.items()])
        self.mock_file(
            "/databricks/common/conf/deploy.conf",
            f"""all-projects {{
                {config}
            }}""",
        )

        self.mock_file("/databricks/DBR_VERSION", version)

        if hostname:
            self.mock_hostname(hostname)

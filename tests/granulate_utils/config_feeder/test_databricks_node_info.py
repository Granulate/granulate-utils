import pytest

from granulate_utils.config_feeder.client import get_node_info
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo
from tests.granulate_utils.config_feeder.fixtures.databricks import DatabricksNodeMock


@pytest.mark.asyncio
async def test_should_collect_node_info() -> None:
    instance_id = "i-000e86ee86c521650"
    cluster_id = "0523-113117-1f8u0192"
    with DatabricksNodeMock(
        provider=CloudProvider.AWS,
        cluster_id=cluster_id,
        instance_id=instance_id,
        is_master=True,
    ):
        assert get_node_info() == NodeInfo(
            provider=CloudProvider.AWS,
            bigdata_platform=BigDataPlatform.DATABRICKS,
            external_id=instance_id,
            external_cluster_id=cluster_id,
            is_master=True,
            properties={
                "spark.databricks.clusterUsageTags.driverInstanceId": "i-000e86ee86c521650",
                "spark.databricks.clusterUsageTags.clusterSomeSecretPassword": "*****",
            },
        )

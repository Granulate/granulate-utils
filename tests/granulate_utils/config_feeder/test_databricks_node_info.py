from unittest.mock import Mock

import pytest
from requests_mock.exceptions import NoMockAddress

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
            bigdata_platform_version="11.3",
            external_id=instance_id,
            external_cluster_id=cluster_id,
            is_master=True,
            properties={
                "spark.databricks.clusterUsageTags.driverInstanceId": "i-000e86ee86c521650",
                "spark.databricks.clusterUsageTags.clusterSomeSecretPassword": "*****",
            },
        )


@pytest.mark.asyncio
async def test_should_extract_cluster_id_from_hostname() -> None:
    instance_id = "i-000e86ee86c521650"
    hostname = "0817-103940-91u12104-10-26-238-244"
    with DatabricksNodeMock(
        provider=CloudProvider.AWS,
        hostname=hostname,
        instance_id=instance_id,
        is_master=False,
    ):
        assert get_node_info() == NodeInfo(
            provider=CloudProvider.AWS,
            bigdata_platform=BigDataPlatform.DATABRICKS,
            bigdata_platform_version="11.3",
            external_id=instance_id,
            external_cluster_id="0817-103940-91u12104",
            is_master=False,
            properties={
                "spark.databricks.clusterUsageTags.clusterSomeSecretPassword": "*****",
            },
        )


@pytest.mark.asyncio
async def test_should_log_cannot_resolve_cluster_id() -> None:
    logger = Mock()
    with DatabricksNodeMock(hostname="foo"):
        with pytest.raises(NoMockAddress):
            assert get_node_info(logger) is None
        logger.error.assert_called_with("cannot resolve cluster id")

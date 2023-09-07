from unittest.mock import Mock

import pytest

from granulate_utils.config_feeder.client import get_node_info
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo
from tests.granulate_utils.config_feeder.fixtures.dataproc import DataprocNodeMock


@pytest.mark.asyncio
async def test_should_collect_node_info() -> None:
    instance_id = "3344294988448254828"
    cluster_uuid = "824afc19-cf18-4b23-99b0-51a6b20b35d"

    with DataprocNodeMock(
        cluster_uuid=cluster_uuid,
        cluster_name="my-cluster",
        instance_id=instance_id,
        is_master=True,
    ):
        assert get_node_info() == NodeInfo(
            provider=CloudProvider.GCP,
            bigdata_platform=BigDataPlatform.DATAPROC,
            bigdata_platform_version="2.0",
            hadoop_version="3.2.1",
            external_id=instance_id,
            external_cluster_id=cluster_uuid,
            is_master=True,
            properties={
                "region": "us-central1",
                "cluster_name": "my-cluster",
            },
        )


@pytest.mark.asyncio
async def test_should_log_missing_metadata_key() -> None:
    logger = Mock()
    with DataprocNodeMock(metadata_response="{}"):
        assert get_node_info(logger) is None
        logger.error.assert_called_with("expected dataproc metadata key was not found", extra={"key": "attributes"})


@pytest.mark.asyncio
async def test_should_log_invalid_metadata_json() -> None:
    logger = Mock()
    with DataprocNodeMock(metadata_response="{"):
        assert get_node_info(logger) is None
        logger.error.assert_called_with("got invalid dataproc metadata JSON")

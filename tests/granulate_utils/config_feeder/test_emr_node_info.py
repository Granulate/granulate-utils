import pytest

from granulate_utils.config_feeder.client import get_node_info
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo
from tests.granulate_utils.config_feeder.fixtures.emr import EmrNodeMock


@pytest.mark.asyncio
async def test_should_collect_node_info() -> None:
    job_flow_id = "j-1234567890"
    instance_id = "i-06828639fa954e04c"

    with EmrNodeMock(
        job_flow_id=job_flow_id,
        instance_id=instance_id,
        is_master=True,
    ):
        assert get_node_info() == NodeInfo(
            provider=CloudProvider.AWS,
            bigdata_platform=BigDataPlatform.EMR,
            bigdata_platform_version="emr-6.9.0",
            external_id=instance_id,
            external_cluster_id=job_flow_id,
            is_master=True,
            properties={},
        )

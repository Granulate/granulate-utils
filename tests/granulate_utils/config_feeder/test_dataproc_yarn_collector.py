import pytest

from granulate_utils.config_feeder.client.yarn.collector import YarnConfigCollector
from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from tests.granulate_utils.config_feeder.fixtures.yarn import YarnNodeMock


@pytest.mark.asyncio
async def test_collect_from_master_node() -> None:
    instance_id = "7203450965080656748"
    cluster_uuid = "824afc19-cf18-4b23-99b0-51a6b20b35d"

    with YarnNodeMock(
        provider="gcp",
        cluster_uuid=cluster_uuid,
        instance_id=instance_id,
        is_master=True,
    ) as mock:
        assert await YarnConfigCollector().collect(mock.node_info) == YarnConfig(
            config=mock.expected_config,
        )


@pytest.mark.asyncio
async def test_collect_from_worker_noder() -> None:
    instance_id = "3344294988448254828"
    cluster_uuid = "824afc19-cf18-4b23-99b0-51a6b20b35d"

    with YarnNodeMock(
        provider="gcp",
        cluster_uuid=cluster_uuid,
        instance_id=instance_id,
        is_master=False,
        web_address="http://0.0.0.0:8042",
    ) as mock:
        assert await YarnConfigCollector().collect(mock.node_info) == YarnConfig(
            config=mock.expected_config,
        )

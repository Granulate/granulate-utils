import logging

import pytest

from granulate_utils.config_feeder.client.autoscaling.collector import AutoScalingConfigCollector
from granulate_utils.config_feeder.core.models.cluster import CloudProvider
from tests.granulate_utils.config_feeder.fixtures.databricks import DatabricksNodeMock


@pytest.mark.asyncio
async def test_should_collect_autoscaling_policy(logger: logging.Logger) -> None:
    instance_id = "i-000e86ee86c521650"
    cluster_id = "0523-113117-1f8u0192"
    autoscaling_policy = {
        "min_workers": 2,
        "max_workers": 8,
    }

    with DatabricksNodeMock(
        provider=CloudProvider.AWS,
        cluster_id=cluster_id,
        instance_id=instance_id,
        is_master=True,
        autoscaling_policy=autoscaling_policy,
    ) as m:
        node_config = await AutoScalingConfigCollector(logger=logger).collect(m.node_info)
        assert node_config is not None
        assert node_config.mode == "managed"
        assert node_config.config == autoscaling_policy

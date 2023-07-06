import logging
from typing import Any, Dict

import pytest

from granulate_utils.config_feeder.client.autoscaling.collector import AutoScalingConfigCollector
from tests.granulate_utils.config_feeder.fixtures.dataproc import DataprocNodeMock


@pytest.mark.asyncio
async def test_should_collect_custom_policy(logger: logging.Logger) -> None:
    instance_id = "7203450965080656748"
    cluster_uuid = "824afc19-cf18-4b23-99b0-51a6b20b35d"
    autoscaling_policy = get_sample_autoscaling_policy()
    cluster_info = {
        "config": {
            "autoscalingConfig": {
                "policyUri": autoscaling_policy["name"],
            },
        },
    }

    with DataprocNodeMock(
        cluster_uuid=cluster_uuid,
        cluster_name="cluster-e323",
        cluster_info=cluster_info,
        instance_id=instance_id,
        is_master=True,
        autoscaling_policy=autoscaling_policy,
    ) as m:
        node_config = await AutoScalingConfigCollector(logger=logger).collect(m.node_info)
        assert node_config is not None
        assert node_config.mode == "custom"
        assert node_config.config == autoscaling_policy


def get_sample_autoscaling_policy() -> Dict[str, Any]:
    return {
        "basicAlgorithm": {
            "cooldownPeriod": "240s",
            "yarnConfig": {
                "gracefulDecommissionTimeout": "3600s",
                "scaleDownFactor": 1.0,
                "scaleUpFactor": 0.05,
            },
        },
        "id": "auto1",
        "name": "projects/radiant-arcanum-378713/regions/us-central1/autoscalingPolicies/auto1",  # noqa: E501
        "secondaryWorkerConfig": {"maxInstances": 2, "weight": 1},
        "workerConfig": {"maxInstances": 2, "minInstances": 2, "weight": 1},
    }

import logging
from typing import Any, Dict

import pytest

from granulate_utils.config_feeder.client.autoscaling.collector import AutoScalingConfigCollector
from tests.granulate_utils.config_feeder.fixtures.emr import EmrNodeMock


@pytest.mark.asyncio
async def test_should_collect_custom_policy(logger: logging.Logger) -> None:
    autoscaling_policy = get_sample_autoscaling_policy()

    instance_groups = [
        {
            "Id": "ig-WP2TQYR8P0TC",
            "InstanceGroupType": "MASTER",
            "InstanceType": "m4.large",
        },
        {
            "Id": "ig-16ZL5OII561M1",
            "InstanceGroupType": "TASK",
            "InstanceType": "m4.large",
            "AutoScalingPolicy": autoscaling_policy,
        },
    ]

    with EmrNodeMock(
        job_flow_id="j-1PBPNNYXXSRYL",
        instance_id="i-06828639fa954e04c",
        is_master=True,
        cluster_info={"Cluster": {"InstanceGroups": instance_groups}},
    ) as m:
        node_config = await AutoScalingConfigCollector(logger=logger).collect(m.node_info)
        assert node_config is not None
        assert node_config.mode == "custom"
        assert node_config.config["ig-16ZL5OII561M1"] == {
            "instance_group_type": "TASK",
            "constraints": autoscaling_policy["Constraints"],
            "rules": autoscaling_policy["Rules"],
        }


@pytest.mark.asyncio
async def test_should_collect_managed_policy(logger: logging.Logger) -> None:
    managed_policy = get_sample_managed_policy()

    with EmrNodeMock(
        job_flow_id="j-1PBPNNYXXSRYL",
        instance_id="i-06828639fa954e04c",
        is_master=True,
        managed_policy=managed_policy,
    ) as m:
        node_config = await AutoScalingConfigCollector(logger=logger).collect(m.node_info)
        assert node_config is not None
        assert node_config.mode == "managed"
        assert node_config.config == managed_policy


@pytest.mark.asyncio
async def test_should_not_collect_on_worker(logger: logging.Logger) -> None:
    with EmrNodeMock(
        job_flow_id="j-1PBPNNYXXSRYL",
        instance_id="i-06828639fa954e04c",
        is_master=False,
    ) as m:
        assert await AutoScalingConfigCollector(logger=logger).collect(m.node_info) is None


def get_sample_managed_policy() -> Dict[str, Any]:
    return {
        "ComputeLimits": {
            "UnitType": "Instances",
            "MinimumCapacityUnits": 2,
            "MaximumCapacityUnits": 4,
            "MaximumOnDemandCapacityUnits": 4,
            "MaximumCoreCapacityUnits": 3,
        }
    }


def get_sample_autoscaling_policy() -> Dict[str, Any]:
    return {
        "Status": {"State": "ATTACHED", "StateChangeReason": {"Message": ""}},
        "Constraints": {"MinCapacity": 2, "MaxCapacity": 3},
        "Rules": [
            {
                "Name": "Default-scale-out",
                "Description": "",
                "Action": {
                    "SimpleScalingPolicyConfiguration": {
                        "AdjustmentType": "CHANGE_IN_CAPACITY",
                        "ScalingAdjustment": 1,
                        "CoolDown": 300,
                    }
                },
                "Trigger": {
                    "CloudWatchAlarmDefinition": {
                        "ComparisonOperator": "LESS_THAN",
                        "EvaluationPeriods": 1,
                        "MetricName": "YARNMemoryAvailablePercentage",
                        "Namespace": "AWS/ElasticMapReduce",
                        "Period": 300,
                        "Statistic": "AVERAGE",
                        "Threshold": 15.0,
                        "Unit": "PERCENT",
                        "Dimensions": [{"Key": "JobFlowId", "Value": "j-1PBPNNYXXSRYL"}],
                    }
                },
            },
            {
                "Name": "Default-scale-in",
                "Description": "",
                "Action": {
                    "SimpleScalingPolicyConfiguration": {
                        "AdjustmentType": "CHANGE_IN_CAPACITY",
                        "ScalingAdjustment": -1,
                        "CoolDown": 300,
                    }
                },
                "Trigger": {
                    "CloudWatchAlarmDefinition": {
                        "ComparisonOperator": "GREATER_THAN",
                        "EvaluationPeriods": 1,
                        "MetricName": "YARNMemoryAvailablePercentage",
                        "Namespace": "AWS/ElasticMapReduce",
                        "Period": 300,
                        "Statistic": "AVERAGE",
                        "Threshold": 0.75,
                        "Unit": "PERCENT",
                        "Dimensions": [{"Key": "JobFlowId", "Value": "j-1PBPNNYXXSRYL"}],
                    }
                },
            },
        ],
    }

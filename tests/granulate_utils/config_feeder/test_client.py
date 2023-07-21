import json
import logging
from typing import Any
from unittest.mock import Mock

import pytest
from requests.exceptions import ConnectionError

from granulate_utils.config_feeder.client.client import ConfigFeederClient
from granulate_utils.config_feeder.client.collector import ConfigFeederCollector
from granulate_utils.config_feeder.client.exceptions import APIError, ClientError
from granulate_utils.config_feeder.client.http_client import DEFAULT_API_SERVER_ADDRESS as API_URL
from granulate_utils.config_feeder.client.http_client import AuthCredentials
from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from granulate_utils.config_feeder.core.errors import InvalidTokenException
from granulate_utils.config_feeder.core.models.autoscaling import AutoScalingConfig, AutoScalingMode
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.collection import CollectionResult
from granulate_utils.config_feeder.core.models.node import NodeInfo
from tests.granulate_utils.config_feeder.fixtures.api import ApiMock


def test_should_send_config_only_once_when_not_changed(auth: AuthCredentials, logger: logging.Logger) -> None:
    with ApiMock(
        collect_yarn_config=mock_yarn_config,
        collect_autoscaling_config=mock_autoscailing_config,
    ) as mock:
        client = ConfigFeederClient(auth, "service1", logger=logger)

        client.collect()
        client.collect()
        client.collect()

        requests = mock.requests

        assert len(requests[f"{API_URL}/clusters"]) == 1
        assert requests[f"{API_URL}/clusters"][0].json() == {
            "cluster": {
                "collector_type": "sagent",
                "service": "service1",
                "provider": "aws",
                "bigdata_platform": "emr",
                "properties": None,
                "external_id": "j-1234567890",
            },
            "allow_existing": True,
        }

        assert len(requests[f"{API_URL}/clusters/cluster-1/nodes"]) == 1
        assert requests[f"{API_URL}/clusters/cluster-1/nodes"][0].json() == {
            "node": {
                "collector_type": "sagent",
                "external_id": "i-1234567890",
                "is_master": True,
            },
            "allow_existing": True,
        }

        assert len(requests[f"{API_URL}/nodes/node-1/configs"]) == 1
        assert requests[f"{API_URL}/nodes/node-1/configs"][0].json() == {
            "yarn_config": {"collector_type": "sagent", "config_json": json.dumps(mock_yarn_config().config)},
            "autoscaling_config": {
                "collector_type": "sagent",
                "config_json": json.dumps(mock_autoscailing_config().dict()),
            },
        }


def test_should_send_config_only_when_changed(auth: AuthCredentials, logger: logging.Logger) -> None:
    yarn_configs = [
        mock_yarn_config(thread_count=128),
        mock_yarn_config(thread_count=128),
        mock_yarn_config(thread_count=64),
    ]

    with ApiMock(collect_yarn_config=lambda _: yarn_configs.pop()) as mock:
        client = ConfigFeederClient(auth, "service1", logger=logger)

        client.collect()
        client.collect()
        client.collect()

        requests = mock.requests

        assert len(requests[f"{API_URL}/nodes/node-1/configs"]) == 2


def test_should_always_register_cluster_on_master_node(auth: AuthCredentials, logger: logging.Logger) -> None:
    with ApiMock() as mock:
        client = ConfigFeederClient(auth, "service1", yarn=False, logger=logger)

        client.collect()
        client.collect()
        client.collect()

        requests = mock.requests

        assert len(requests) == 1
        assert requests[f"{API_URL}/clusters"][0].json() == {
            "cluster": {
                "collector_type": "sagent",
                "service": "service1",
                "provider": "aws",
                "bigdata_platform": "emr",
                "properties": None,
                "external_id": "j-1234567890",
            },
            "allow_existing": True,
        }


def test_should_not_register_cluster_on_worker_node(auth: AuthCredentials, logger: logging.Logger) -> None:
    node_info = NodeInfo(
        external_cluster_id="j-1234567890",
        external_id="i-1234567890",
        is_master=False,
        provider=CloudProvider.GCP,
        bigdata_platform=BigDataPlatform.DATABRICKS,
    )
    with ApiMock(node_info=node_info) as mock:
        client = ConfigFeederClient(auth, "service1", yarn=True, logger=logger)

        client.collect()
        client.collect()
        client.collect()

        requests = mock.requests

        assert len(requests) == 0


def test_should_not_send_anything_if_not_big_data_platform(auth: AuthCredentials, logger: logging.Logger) -> None:
    with ApiMock(node_info=None) as mock:
        client = ConfigFeederClient(auth, "service1", yarn=False, logger=logger)

        client.collect()
        client.collect()
        client.collect()

        requests = mock.requests

        assert len(requests) == 0


def test_should_call_external_collector(auth: AuthCredentials, logger: logging.Logger) -> None:
    with ApiMock():
        collect_mock = Mock()

        def some_collector(_):
            class SomeCollector(ConfigFeederCollector):
                name = "some_collector"

                async def collect(self, _) -> CollectionResult:
                    collect_mock()
                    return CollectionResult(config=None)

            return SomeCollector(_)

        client = ConfigFeederClient(auth, "service1", yarn=False, logger=logger, collector_factories=[some_collector])

        client.collect()

        collect_mock.assert_called_once()


def test_should_have_authorization_header(auth: AuthCredentials, logger: logging.Logger) -> None:
    with ApiMock() as mock:
        client = ConfigFeederClient(auth, "service1", yarn=False, logger=logger)

        client.collect()

        headers = mock.requests[f"{API_URL}/clusters"][0].headers
        assert headers["Authorization"] == f"{auth.scheme} {auth.credentials}"


def test_should_fail_with_client_error(auth: AuthCredentials, logger: logging.Logger) -> None:
    with ApiMock(
        collect_yarn_config=mock_yarn_config,
        register_cluster_response={"exc": ConnectionError("Connection refused")},
    ):
        with pytest.raises(ClientError, match=f"could not connect to {API_URL}"):
            ConfigFeederClient(auth, "service1", logger=logger).collect()


def test_should_fail_with_invalid_token_exception(auth: AuthCredentials, logger: logging.Logger) -> None:
    with ApiMock(
        collect_yarn_config=mock_yarn_config,
        register_cluster_response={
            "status_code": 401,
            "json": {"error": {"code": "INVALID_TOKEN", "message": "Invalid token"}},
        },
    ):
        with pytest.raises(InvalidTokenException, match="Invalid token"):
            ConfigFeederClient(auth, "service1", logger=logger).collect()


def test_should_fail_with_api_error(auth: AuthCredentials, logger: logging.Logger) -> None:
    with ApiMock(
        collect_yarn_config=mock_yarn_config,
        register_cluster_response={"status_code": 400, "text": "unexpected error"},
    ):
        with pytest.raises(APIError, match="400 unexpected error /clusters"):
            ConfigFeederClient(auth, "service1", logger=logger).collect()


def mock_yarn_config(*args: Any, thread_count: int = 64) -> YarnConfig:
    return YarnConfig(
        config={
            "properties": [
                {
                    "key": "yarn.resourcemanager.resource-tracker.client.thread-count",
                    "value": str(thread_count),
                    "resource": "yarn-site.xml",
                }
            ]
        },
    )


def mock_autoscailing_config(*args: Any) -> AutoScalingConfig:
    return AutoScalingConfig(
        mode=AutoScalingMode.MANAGED,
        config={
            "ComputeLimits": {
                "UnitType": "Instances",
                "MinimumCapacityUnits": 2,
                "MaximumCapacityUnits": 4,
                "MaximumOnDemandCapacityUnits": 4,
                "MaximumCoreCapacityUnits": 3,
            }
        },
    )

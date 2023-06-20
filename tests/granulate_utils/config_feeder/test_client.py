import json
import logging
from typing import Any

import pytest
from requests.exceptions import ConnectionError

from granulate_utils.config_feeder.client.client import DEFAULT_API_SERVER_ADDRESS as API_URL
from granulate_utils.config_feeder.client.client import ConfigFeederClient
from granulate_utils.config_feeder.client.exceptions import APIError, ClientError
from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from granulate_utils.config_feeder.core.errors import InvalidTokenException
from tests.granulate_utils.config_feeder.fixtures.api import ApiMock


def test_should_send_config_only_once_when_not_changed() -> None:
    with ApiMock(
        collect_yarn_config=mock_yarn_config,
    ) as mock:
        client = ConfigFeederClient("token1", "service1")

        client.collect()
        client.collect()
        client.collect()

        requests = mock.requests

        assert len(requests[f"{API_URL}/clusters"]) == 1
        assert requests[f"{API_URL}/clusters"][0].json() == {
            "cluster": {
                "service": "service1",
                "provider": "aws",
                "external_id": "j-1234567890",
            },
            "allow_existing": True,
        }

        assert len(requests[f"{API_URL}/clusters/cluster-1/nodes"]) == 1
        assert requests[f"{API_URL}/clusters/cluster-1/nodes"][0].json() == {
            "node": {
                "external_id": "i-1234567890",
                "is_master": True,
            },
            "allow_existing": True,
        }

        assert len(requests[f"{API_URL}/nodes/node-1/configs"]) == 1
        assert requests[f"{API_URL}/nodes/node-1/configs"][0].json() == {
            "yarn_config": {"config_json": json.dumps(mock_yarn_config().config)},
        }


def test_should_send_config_only_when_changed() -> None:
    yarn_configs = [
        mock_yarn_config(thread_count=128),
        mock_yarn_config(thread_count=128),
        mock_yarn_config(thread_count=64),
    ]

    with ApiMock(collect_yarn_config=lambda _: yarn_configs.pop()) as mock:
        client = ConfigFeederClient("token1", "service1")

        client.collect()
        client.collect()
        client.collect()

        requests = mock.requests

        assert len(requests[f"{API_URL}/nodes/node-1/configs"]) == 2


def test_should_not_send_anything() -> None:
    with ApiMock(collect_yarn_config=mock_yarn_config) as mock:
        client = ConfigFeederClient("token1", "service1", yarn=False)

        client.collect()
        client.collect()
        client.collect()

        requests = mock.requests

        assert len(requests) == 0


def test_should_fail_with_client_error() -> None:
    with ApiMock(
        collect_yarn_config=mock_yarn_config,
        register_cluster_response={"exc": ConnectionError("Connection refused")},
    ):
        with pytest.raises(ClientError, match=f"could not connect to {API_URL}"):
            ConfigFeederClient("token1", "service1").collect()


def test_should_fail_with_invalid_token_exception() -> None:
    with ApiMock(
        collect_yarn_config=mock_yarn_config,
        register_cluster_response={
            "status_code": 401,
            "json": {"error": {"code": "INVALID_TOKEN", "message": "Invalid token"}},
        },
    ):
        with pytest.raises(InvalidTokenException, match="Invalid token"):
            ConfigFeederClient("token1", "service1").collect()


def test_should_fail_with_api_error() -> None:
    with ApiMock(
        collect_yarn_config=mock_yarn_config,
        register_cluster_response={"status_code": 400, "text": "unexpected error"},
    ):
        with pytest.raises(APIError, match="400 unexpected error /clusters"):
            ConfigFeederClient("token1", "service1").collect()


def test_should_have_logger_with_null_handler() -> None:
    client = ConfigFeederClient("token1", "service1")

    assert len(client.logger.handlers) == 1
    assert isinstance(client.logger.handlers[0], type(logging.NullHandler()))


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

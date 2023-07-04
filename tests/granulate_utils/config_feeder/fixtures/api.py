from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, ContextManager, List, Tuple
from unittest.mock import patch

from requests_mock.mocker import Mocker
from requests_mock.request import _RequestObjectProxy

from granulate_utils.config_feeder.client.client import DEFAULT_API_SERVER_ADDRESS
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo


class ApiMock:
    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self._api_mock: Mocker | None = None

    @property
    def requests(self) -> defaultdict[str, List[_RequestObjectProxy]]:
        result = defaultdict(list)
        assert self._api_mock is not None
        for r in self._api_mock.request_history:
            result[r.url].append(r)
        return result

    def _configure_node_info(self, mock: Any) -> None:
        mock.return_value = self.kwargs.get(
            "node_info",
            NodeInfo(
                external_cluster_id="j-1234567890",
                external_id="i-1234567890",
                is_master=True,
                provider=CloudProvider.AWS,
                bigdata_platform=BigDataPlatform.EMR,
            ),
        )

    def _configure_yarn_collector_mock(self, mock: Any) -> None:
        instance = mock.return_value
        instance.collect.side_effect = self.kwargs.get("collect_yarn_config", lambda node_info: None)

    def _configure_api_mock(self, mock: Any) -> None:
        register_cluster_response = self.kwargs.get(
            "register_cluster_response",
            {
                "json": {
                    "cluster": {
                        "id": "cluster-1",
                        "collector": "sagent",
                        "provider": "aws",
                        "bigdata_platform": "emr",
                        "external_id": "j-1234567890",
                        "ts": "2021-10-01T00:00:00Z",
                    }
                }
            },
        )
        mock.post(f"{DEFAULT_API_SERVER_ADDRESS}/clusters", **register_cluster_response)

        register_node_response = self.kwargs.get(
            "register_node_response",
            {
                "json": {
                    "node": {
                        "id": "node-1",
                        "collector": "sagent",
                        "external_id": "i-1234567890",
                        "ts": "2021-10-01T00:00:00Z",
                    }
                }
            },
        )
        mock.post(
            f"{DEFAULT_API_SERVER_ADDRESS}/clusters/cluster-1/nodes",
            **register_node_response,
        )

        register_yarn_config_response = self.kwargs.get("register_yarn_config_response", {"json": {"yarn_config": {}}})
        mock.post(
            f"{DEFAULT_API_SERVER_ADDRESS}/nodes/node-1/yarn_configs",
            **register_yarn_config_response,
        )

        register_node_configs_response = self.kwargs.get(
            "register_node_configs_response",
            {
                "json": {
                    "yarn_config": {
                        "node_id": "node-1",
                        "yarn_config_id": "yarn-config-1",
                        "config_hash": "1234567890",
                        "config_json": {},
                        "ts": "2021-10-01T00:00:00Z",
                    },
                }
            },
        )
        mock.post(
            f"{DEFAULT_API_SERVER_ADDRESS}/nodes/node-1/configs",
            **register_node_configs_response,
        )

        self._api_mock = mock

    def __enter__(self) -> ApiMock:
        self.contexts: List[Tuple[ContextManager[Any], Callable[[Any], None]]] = [
            (
                patch(
                    "granulate_utils.config_feeder.client.client.get_node_info",
                ),
                self._configure_node_info,
            ),
            (
                patch(
                    "granulate_utils.config_feeder.client.client.YarnConfigCollector",
                    autospec=True,
                ),
                self._configure_yarn_collector_mock,
            ),
            (Mocker(), self._configure_api_mock),
        ]

        for ctx, fn in self.contexts:
            value = ctx.__enter__()
            if fn is not None:
                fn(value)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_tb: Any) -> None:
        for ctx, _ in self.contexts:
            ctx.__exit__(None, None, None)

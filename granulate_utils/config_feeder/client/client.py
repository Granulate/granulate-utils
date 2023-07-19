import asyncio
import logging
from collections import defaultdict
from functools import reduce
from typing import Callable, DefaultDict, Dict, List, Optional, Union

from granulate_utils.config_feeder.client.bigdata import get_node_info
from granulate_utils.config_feeder.client.cluster_client import ClusterClient
from granulate_utils.config_feeder.client.collector import ConfigFeederCollector, ConfigFeederCollectorParams
from granulate_utils.config_feeder.client.exceptions import ClientError
from granulate_utils.config_feeder.client.http_client import HttpClient
from granulate_utils.config_feeder.client.models import CollectionResult
from granulate_utils.config_feeder.client.yarn_config_feeder_collector import YarnConfigFeederCollector
from granulate_utils.config_feeder.core.models.aggregation import NodeResourceConfigCreate
from granulate_utils.config_feeder.core.models.collection import CollectorType
from granulate_utils.config_feeder.core.models.node import NodeInfo


class ConfigFeederClient:
    def __init__(
        self,
        token: str,
        service: str,
        *,
        logger: Union[logging.Logger, logging.LoggerAdapter],
        server_address: Optional[str] = None,
        yarn: bool = True,
        collector_type=CollectorType.SAGENT,
        collector_factories: List[Callable[[ConfigFeederCollectorParams], ConfigFeederCollector]] = [],
    ) -> None:
        if not token or not service:
            raise ClientError("Token and service must be provided")

        def yarn_collector(params: ConfigFeederCollectorParams) -> YarnConfigFeederCollector:
            return YarnConfigFeederCollector(params, yarn=yarn)

        self.logger = logger
        self._service = service
        self._collector_type = collector_type
        self._is_yarn_enabled = yarn
        self._http_client = HttpClient(token, server_address)
        self._cluster_client = ClusterClient(logger, self._http_client, collector_type, service)
        self._collectors = self._create_collectors([yarn_collector, *collector_factories])
        self._last_hash: DefaultDict[str, str] = defaultdict()

    def collect(self) -> None:
        if (node_info := get_node_info(self.logger)) is None:
            self.logger.warning("not a Big Data host, skipping")
            return None

        asyncio.run(self._collect(node_info))

    async def _collect(self, node_info: NodeInfo) -> None:
        collection_result = await self._run_collectors(node_info)
        self._cluster_client.register_cluster_if_needed(node_info, collection_result)
        self._submit_configs_if_needed(node_info, collection_result)

    def _submit_configs_if_needed(self, node_info: NodeInfo, collection_result: Dict[str, CollectionResult]):
        requests = self._get_config_requests(collection_result)

        if not requests:
            self.logger.info(f"skipping node {node_info.external_id}, configs are up to date")
            return None

        response = self._cluster_client.submit_node_configs(node_info, requests)

        for name, _ in response.items():
            assert collection_result.get(name, None) is not None
            self._last_hash[name] = collection_result[name].config_hash

    async def _run_collectors(self, node_info: NodeInfo) -> Dict[str, CollectionResult]:
        async def run_collector(c: ConfigFeederCollector):
            collection_result = await c.collect(node_info)

            if collection_result.is_empty:
                self.logger.info(f"{c.name} has no configs to submit")
                return {}

            return {c.name: collection_result}

        result = await asyncio.gather(*list(map(run_collector, self._collectors)))
        return reduce(lambda x, y: {**x, **y}, result, {})

    def _create_collectors(
        self, collector_factories: List[Callable[[ConfigFeederCollectorParams], ConfigFeederCollector]]
    ):
        params: ConfigFeederCollectorParams = {"logger": self.logger}
        return list(map(lambda fac: fac(params), collector_factories))

    def _get_config_requests(self, configs: Dict[str, CollectionResult]) -> Dict[str, NodeResourceConfigCreate]:
        outdated: Dict[str, NodeResourceConfigCreate] = {}

        for name, result in configs.items():
            if self._last_hash.get(name, None) == result.config_hash:
                self.logger.debug(f"{name} config is up to date")
                continue
            outdated[name] = NodeResourceConfigCreate(
                collector_type=self._collector_type, config_json=result.serialized
            )

        return outdated

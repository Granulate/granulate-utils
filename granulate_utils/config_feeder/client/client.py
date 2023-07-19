import asyncio
import logging
from typing import Callable, List, Optional, Union

from granulate_utils.config_feeder.client.bigdata import get_node_info
from granulate_utils.config_feeder.client.collector import ConfigFeederCollector, ConfigFeederCollectorParams
from granulate_utils.config_feeder.client.exceptions import ClientError
from granulate_utils.config_feeder.client.http_client import HttpClient
from granulate_utils.config_feeder.client.yarn_config_feeder_collector import YarnConfigFeederCollector
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
        self._collectors = self._create_collectors([yarn_collector, *collector_factories])

    def collect(self) -> None:
        if (node_info := get_node_info(self.logger)) is None:
            self.logger.warning("not a Big Data host, skipping")
            return None

        asyncio.run(self._collect(node_info))

    async def _collect(self, node_info: NodeInfo) -> None:
        await asyncio.gather(*list(map(lambda c: c.collect(node_info), self._collectors)))

    def _create_collectors(
        self, collector_factories: List[Callable[[ConfigFeederCollectorParams], ConfigFeederCollector]]
    ):
        params: ConfigFeederCollectorParams = {
            "logger": self.logger,
            "http_client": self._http_client,
            "service": self._service,
            "collector_type": self._collector_type,
        }
        return list(map(lambda fac: fac(params), collector_factories))

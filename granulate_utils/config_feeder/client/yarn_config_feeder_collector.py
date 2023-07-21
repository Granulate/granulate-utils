from typing import Optional

from granulate_utils.config_feeder.client.collector import ConfigFeederCollector, ConfigFeederCollectorParams
from granulate_utils.config_feeder.client.yarn.collector import YarnConfigCollector
from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from granulate_utils.config_feeder.core.models.collection import CollectionResult
from granulate_utils.config_feeder.core.models.node import NodeInfo


class YarnConfigFeederCollector(ConfigFeederCollector):
    name = "yarn_config"

    def __init__(
        self,
        params: ConfigFeederCollectorParams,
    ) -> None:
        super().__init__(params)
        self._yarn_collector = YarnConfigCollector(logger=self.logger)

    async def collect(self, node_info: NodeInfo) -> CollectionResult:
        results = await self._collect_yarn_config(node_info)
        config = results.config if results is not None else None
        return CollectionResult(config=config)

    async def _collect_yarn_config(self, node_info: NodeInfo) -> Optional[YarnConfig]:
        self.logger.info("YARN config collection starting")
        yarn_config = await self._yarn_collector.collect(node_info)
        self.logger.info("YARN config collection finished")
        return yarn_config

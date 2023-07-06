import logging
from typing import Any, Optional, Union

from requests.exceptions import ConnectionError

from granulate_utils.config_feeder.client.base import ConfigCollectorBase
from granulate_utils.config_feeder.client.spark.utils import SPARK_HISTORY_SERVER_DEFAULT_ADDRESS


class SparkConfigCollector(ConfigCollectorBase):
    def __init__(self, *, max_retries: int = 20, logger: Union[logging.Logger, logging.LoggerAdapter]) -> None:
        super().__init__(max_retries=max_retries, logger=logger)
        self._history_server_address = SPARK_HISTORY_SERVER_DEFAULT_ADDRESS

    async def history_request(self, path: str) -> Optional[Any]:
        try:
            return await self._fetch(self._history_server_address, path)
        except ConnectionError:
            self.logger.warning(f"failed to connect to {self._history_server_address}")
        return None

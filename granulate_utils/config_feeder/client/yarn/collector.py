import logging
from typing import Any, Dict, Optional, Union

from requests.exceptions import ConnectionError

from granulate_utils.config_feeder.client.base import ConfigCollectorBase
from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from granulate_utils.config_feeder.client.yarn.utils import (
    RM_DEFAULT_ADDRESS,
    WORKER_ADDRESS,
    detect_resource_manager_address,
    get_yarn_properties,
)
from granulate_utils.config_feeder.core.models.node import NodeInfo


class YarnConfigCollector(ConfigCollectorBase):
    def __init__(self, *, max_retries: int = 20, logger: Union[logging.Logger, logging.LoggerAdapter]) -> None:
        super().__init__(max_retries=max_retries, logger=logger)
        self._resource_manager_address = RM_DEFAULT_ADDRESS
        self._is_address_detected = False

    async def collect(self, node_info: NodeInfo) -> Optional[YarnConfig]:
        if config := await (self._get_master_config() if node_info.is_master else self._get_worker_config()):
            return YarnConfig(
                config=config,
            )

        self.logger.error("failed to collect node config", extra=node_info.__dict__)
        return None

    async def rm_request(self, path: str) -> Optional[Dict[str, Any]]:
        result: Optional[Dict[str, Any]] = None
        try:
            result = await self._fetch(f"{self._resource_manager_address}{path}")
        except ConnectionError:
            if self._is_address_detected:
                self.logger.error(f"could not connect to {self._resource_manager_address}")
                return None
            self.logger.warning(f"ResourceManager not found at {self._resource_manager_address}")
            if address := await detect_resource_manager_address(logger=self.logger):
                self._is_address_detected = True
                self._resource_manager_address = address
                self.logger.debug(f"found ResourceManager address: {address}")
                result = await self._fetch(f"{address}{path}")
            else:
                self.logger.error("could not resolve ResourceManager address")
        return result

    async def node_request(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            return await self._fetch(f"{WORKER_ADDRESS}{path}")
        except ConnectionError:
            self.logger.warning(f"failed to connect to {WORKER_ADDRESS}")
        return None

    async def _get_master_config(self) -> Optional[Dict[str, Any]]:
        """
        Get running ResourceManager configuration

        most recent config is returned

        supported version: 2.9.0+
        """
        try:
            config: Optional[Dict[str, Any]] = await self.rm_request("/conf")
            return get_yarn_properties(config) if config else None
        except Exception:
            self.logger.error("failed to get ResourceManager config")
            raise

    async def _get_worker_config(self) -> Optional[Dict[str, Any]]:
        """
        Get running node configuration
        """
        try:
            config: Optional[Dict[str, Any]] = await self.node_request("/conf")
            return get_yarn_properties(config) if config else None
        except Exception:
            self.logger.error("failed to get node config")
            raise

import logging
from typing import Any, Dict, Optional, Union

from requests.exceptions import ConnectionError

from granulate_utils.config_feeder.client.base import ConfigCollectorBase
from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform
from granulate_utils.config_feeder.core.models.node import NodeInfo
from granulate_utils.metrics.yarn.utils import (
    RM_DEFAULT_ADDRESS,
    WORKER_ADDRESS,
    get_all_properties,
    get_yarn_node_info,
    get_yarn_properties,
)


class YarnConfigCollector(ConfigCollectorBase):
    def __init__(
        self,
        *,
        max_retries: int = 20,
        logger: Union[logging.Logger, logging.LoggerAdapter],
        resourcemanager_address: str = RM_DEFAULT_ADDRESS,
        worker_address: str = WORKER_ADDRESS,
        all_properties: bool = False,
    ) -> None:
        super().__init__(max_retries=max_retries, logger=logger)
        self._resource_manager_address = resourcemanager_address
        self._worker_address = worker_address
        self._is_address_detected = False
        self._all_properties = all_properties

    async def collect(self, node_info: NodeInfo) -> Optional[YarnConfig]:
        if node_info.bigdata_platform == BigDataPlatform.DATABRICKS:
            self.logger.debug(f"{node_info.bigdata_platform} is not supported, skipping")
            return None

        if config := await (self._get_master_config() if node_info.is_master else self._get_worker_config()):
            return YarnConfig(
                config=config,
            )

        self.logger.error("failed to collect node config", extra=node_info.__dict__)
        return None

    async def rm_request(self, path: str) -> Optional[Dict[str, Any]]:
        result: Optional[Dict[str, Any]] = None
        try:
            result = await self._fetch(self._resource_manager_address, path)
        except ConnectionError:
            if self._is_address_detected:
                self.logger.error(f"could not connect to {self._resource_manager_address}")
                return None
            self.logger.warning(f"ResourceManager not found at {self._resource_manager_address}")
            if self._detect_resource_manager_address():
                result = await self._fetch(self._resource_manager_address, path)
        return result

    async def node_request(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            return await self._fetch(self._worker_address, path)
        except ConnectionError:
            self.logger.warning(f"failed to connect to {self._worker_address}")
        return None

    async def _get_master_config(self) -> Optional[Dict[str, Any]]:
        """
        Get running ResourceManager configuration

        most recent config is returned

        supported version: 2.8.3+
        """
        try:
            config: Optional[Dict[str, Any]] = await self.rm_request("/conf")
            return self._get_properties(config) if config else None
        except Exception:
            self.logger.error("failed to get ResourceManager config")
            raise

    async def _get_worker_config(self) -> Optional[Dict[str, Any]]:
        """
        Get running node configuration
        """
        try:
            config: Optional[Dict[str, Any]] = await self.node_request("/conf")
            return self._get_properties(config) if config else None
        except Exception:
            self.logger.error("failed to get node config")
            raise

    def _get_properties(self, config: Dict[str, Any]) -> Dict[str, Any]:
        if self._all_properties:
            return get_all_properties(config)
        return get_yarn_properties(config)

    def _detect_resource_manager_address(self) -> bool:
        if (yarn_node_info := get_yarn_node_info(logger=self.logger)) and yarn_node_info.is_resource_manager:
            self._is_address_detected = True
            self._resource_manager_address = yarn_node_info.get_own_resource_manager_webapp_address()
            self.logger.debug(f"found ResourceManager address: {self._resource_manager_address}")
            return True
        else:
            self.logger.error("could not resolve ResourceManager address")
        return False

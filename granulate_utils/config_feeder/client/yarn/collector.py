import asyncio
import contextvars
import functools
import logging
from typing import Any, Dict, Optional, Union, cast

from requests import Session
from requests.exceptions import ConnectionError

from granulate_utils.config_feeder.client.exceptions import MaximumRetriesExceeded
from granulate_utils.config_feeder.client.logging import get_logger
from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from granulate_utils.config_feeder.client.yarn.utils import (
    RM_DEFAULT_ADDRESS,
    WORKER_ADDRESS,
    detect_resource_manager_address,
    get_yarn_properties,
)
from granulate_utils.config_feeder.core.models.node import NodeInfo


class YarnConfigCollector:
    def __init__(self, *, max_retries: int = 20, logger: Union[logging.Logger, logging.LoggerAdapter] = None) -> None:
        self.logger = logger or get_logger()
        self._resource_manager_address = RM_DEFAULT_ADDRESS
        self._is_address_detected = False
        self._max_retries = max_retries
        self._failed_connections = 0
        self._init_session()

    def _init_session(self) -> None:
        self._session = Session()
        self._session.headers.update({"Accept": "application/json"})

    async def collect(self, node_info: NodeInfo) -> Optional[YarnConfig]:
        if self._failed_connections >= self._max_retries:
            raise MaximumRetriesExceeded("maximum number of failed connections reached", self._max_retries)

        if config := await (self._get_master_config() if node_info.is_master else self._get_worker_config()):
            self._failed_connections = 0
            return YarnConfig(
                config=config,
            )

        self.logger.error("failed to collect node config", extra=node_info.__dict__)
        return None

    async def rm_request(self, path: str) -> Optional[Dict[str, Any]]:
        if self._failed_connections >= self._max_retries:
            raise MaximumRetriesExceeded("maximum number of failed connections reached", self._max_retries)

        result: Optional[Dict[str, Any]] = None
        try:
            result = await self._fetch(f"{self._resource_manager_address}{path}")
        except ConnectionError:
            self._failed_connections += 1
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

    async def _get_master_config(self) -> Optional[Dict[str, Any]]:
        config: Optional[Dict[str, Any]] = await self.rm_request("/conf")
        return get_yarn_properties(config) if config else None

    async def _get_worker_config(self) -> Optional[Dict[str, Any]]:
        try:
            return get_yarn_properties(await self._fetch(f"{WORKER_ADDRESS}/conf"))
        except ConnectionError:
            self._failed_connections += 1
            self.logger.warning(f"failed to connect to {WORKER_ADDRESS}")
        return None

    async def _fetch(self, url: str) -> Dict[str, Any]:
        if not url.startswith("http"):
            url = f"http://{url}"
        self.logger.debug(f"fetching {url}")
        coro = to_thread(self._session.request, "GET", url)
        resp = await asyncio.create_task(coro)
        resp.raise_for_status()
        return cast(Dict[str, Any], resp.json())


# taken from 3.9 because it is not available in Python 3.8
async def to_thread(func, /, *args, **kwargs):  # type: ignore
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)

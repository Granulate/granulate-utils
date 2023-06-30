import asyncio
import contextvars
import functools
import logging
from abc import ABC
from typing import TypeVar, Union, cast

from requests import Session

from granulate_utils.config_feeder.client.exceptions import MaximumRetriesExceeded
from granulate_utils.config_feeder.client.logging import get_logger

T = TypeVar("T")


class ConfigCollectorBase(ABC):
    def __init__(self, *, max_retries: int = 20, logger: Union[logging.Logger, logging.LoggerAdapter] = None) -> None:
        self.logger = logger or get_logger()
        self._max_retries = max_retries
        self._failed_requests = 0
        self._init_session()

    def _init_session(self) -> None:
        self._session = Session()
        self._session.headers.update({"Accept": "application/json"})

    async def _fetch(self, url: str) -> T:
        if self._failed_requests >= self._max_retries:
            raise MaximumRetriesExceeded("maximum number of failed requests reached", self._max_retries)
        try:
            if not url.startswith("http"):
                url = f"http://{url}"
            self.logger.debug(f"fetching {url}")
            coro = to_thread(self._session.request, "GET", url)
            resp = await asyncio.create_task(coro)
            resp.raise_for_status()
            result = cast(T, resp.json())
            self._failed_requests = 0
            return result
        except Exception:
            self._failed_requests += 1
            raise


# taken from 3.9 because it is not available in Python 3.8
async def to_thread(func, /, *args, **kwargs):  # type: ignore
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)

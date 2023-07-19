from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TypedDict, Union

from granulate_utils.config_feeder.client.http_client import HttpClient
from granulate_utils.config_feeder.core.models.collection import CollectorType


class ConfigFeederCollectorParams(TypedDict):
    logger: Union[logging.Logger, logging.LoggerAdapter]
    http_client: HttpClient
    service: str
    collector_type: CollectorType


class ConfigFeederCollector(ABC):
    def __init__(self, params: ConfigFeederCollectorParams) -> None:
        self.logger = params["logger"]
        self._http_client = params["http_client"]
        self._service = params["service"]
        self._collector_type = params["collector_type"]

    @abstractmethod
    async def collect(self) -> None:
        pass

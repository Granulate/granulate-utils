from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TypedDict, Union

from granulate_utils.config_feeder.core.models.collection import CollectionResult
from granulate_utils.config_feeder.core.models.node import NodeInfo


class ConfigFeederCollectorParams(TypedDict):
    logger: Union[logging.Logger, logging.LoggerAdapter]


class ConfigFeederCollector(ABC):
    def __init__(self, params: ConfigFeederCollectorParams) -> None:
        self.logger = params["logger"]

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    async def collect(self, node_info: NodeInfo) -> CollectionResult:
        pass

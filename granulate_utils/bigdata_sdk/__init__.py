import logging
from functools import cached_property
from typing import Union

from granulate_utils.bigdata_sdk.yarn import Yarn, YarnConfig


class BigDataSDK:
    def __init__(self, *, logger: Union[logging.Logger, logging.LoggerAdapter], yarn: YarnConfig = None):
        self._logger = logger
        self._yarn_config = yarn

    @cached_property
    def yarn(self):
        return Yarn(logger=self._logger, yarn_config=self._yarn_config)

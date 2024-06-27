import logging
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    _LoggerAdapter = logging.LoggerAdapter[logging.Logger]
else:
    _LoggerAdapter = logging.LoggerAdapter

LoggerOrAdapter = Union[logging.Logger, _LoggerAdapter]

import logging
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    _LoggerAdapter = logging.LoggerAdapter[logging.Logger]
else:
    _LoggerAdapter = logging.LoggerAdapter


def get_databricks_version() -> Optional[str]:
    try:
        with open("/databricks/DBR_VERSION", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def get_hadoop_version(logger: Optional[Union[logging.Logger, _LoggerAdapter]]) -> Optional[str]:
    try:
        with open("/databricks/spark/HADOOP_VERSION", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        logger.error("Failed to get hadoop version", exc_info=True)
    return None

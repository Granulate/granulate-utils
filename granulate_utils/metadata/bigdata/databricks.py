import logging
import os
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    _LoggerAdapter = logging.LoggerAdapter[logging.Logger]
else:
    _LoggerAdapter = logging.LoggerAdapter

DATABRICKS_METRICS_PROP_PATH = "/databricks/spark/conf/metrics.properties"


def get_databricks_version() -> Optional[str]:
    try:
        with open("/databricks/DBR_VERSION", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def is_databricks() -> bool:
    """
    In some Databricks versions / images, /databricks/DBR_VERSION is missing but this file exists.
    """
    return os.path.exists(DATABRICKS_METRICS_PROP_PATH)


def get_hadoop_version(logger: Optional[Union[logging.Logger, _LoggerAdapter]]) -> Optional[str]:
    try:
        with open("/databricks/spark/HADOOP_VERSION", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        if logger:
            logger.error("Failed to get hadoop version", exc_info=True)
    return None

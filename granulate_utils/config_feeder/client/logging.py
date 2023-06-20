import logging
from functools import lru_cache

LOGGER_NAME = "config-feeder-client"


class Extra(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "extra"):
            record.extra = {}
        return True


@lru_cache(maxsize=None)
def get_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.addFilter(Extra())
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.NullHandler())
    return logger

import logging

import pytest

from granulate_utils.config_feeder.client.bigdata import get_node_info
from granulate_utils.config_feeder.client.http_client import AuthCredentials


@pytest.fixture(autouse=True)
def clear_cache() -> None:
    get_node_info.cache_clear()


@pytest.fixture(scope="session")
def logger() -> logging.Logger:
    logger = logging.getLogger("test-logger")
    logger.addHandler(logging.NullHandler())
    return logger


@pytest.fixture(scope="session")
def auth() -> AuthCredentials:
    return AuthCredentials(scheme="Bearer", credentials="token1")

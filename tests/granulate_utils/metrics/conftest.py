import logging

import pytest


@pytest.fixture(scope="session")
def logger() -> logging.Logger:
    logger = logging.getLogger("test-logger")
    logger.addHandler(logging.NullHandler())
    return logger

import logging
from typing import Any, Dict

import pytest


@pytest.fixture(scope="session")
def logger() -> logging.Logger:
    logger = logging.getLogger("test-logger")
    logger.addHandler(logging.NullHandler())
    return logger


@pytest.fixture
def yarn_config() -> Dict[str, Any]:
    return {
        "properties": [
            {
                "key": "yarn.nodemanager.resource.memory-mb",
                "value": "125872",
                "isFinal": False,
                "resource": "Dataproc Cluster Properties",
            },
            {
                "key": "yarn.resourcemanager.address",
                "value": "host-32-m.internal:8032",
                "isFinal": False,
                "resource": "programmatically",
            },
            {
                "key": "yarn.federation.state-store.sql.password",
                "value": "password1",
                "isFinal": False,
                "resource": "yarn-site.xml",
            },
            {
                "key": "mapreduce.map.memory.mb",
                "value": "2048",
                "isFinal": False,
                "resource": "mapred-site.xml",
            },
        ]
    }

import re
from typing import Any, Dict

import pytest

from granulate_utils.metrics.yarn.utils import filter_properties, get_yarn_properties


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


def test_should_get_only_yarn_config_properties(yarn_config: dict) -> None:
    assert get_yarn_properties(yarn_config)["properties"] == [
        {
            "key": "yarn.nodemanager.resource.memory-mb",
            "value": "125872",
            "resource": "Dataproc Cluster Properties",
        },
        {
            "key": "yarn.resourcemanager.address",
            "value": "host-32-m.internal:8032",
            "resource": "programmatically",
        },
        {
            "key": "yarn.federation.state-store.sql.password",
            "value": "*****",
            "resource": "yarn-site.xml",
        },
    ]


def test_should_filter_and_mask_yarn_config_properties(yarn_config: dict) -> None:
    assert filter_properties(yarn_config, lambda prop: bool(re.search(r"(Dataproc|yarn-site)", prop["resource"]))) == [
        {
            "key": "yarn.nodemanager.resource.memory-mb",
            "value": "125872",
            "resource": "Dataproc Cluster Properties",
        },
        {
            "key": "yarn.federation.state-store.sql.password",
            "value": "*****",
            "resource": "yarn-site.xml",
        },
    ]

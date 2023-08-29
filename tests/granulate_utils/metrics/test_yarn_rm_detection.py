import logging
from typing import Any, Dict, List
from unittest.mock import Mock

import pytest

from granulate_utils.metrics.yarn.utils import detect_resource_manager_addresses
from tests.granulate_utils.config_feeder.fixtures.yarn import YarnNodeMock


@pytest.mark.parametrize(
    "properties, expected",
    [
        pytest.param({}, ["0.0.0.0:8088"], id="default"),
        pytest.param(
            {"yarn.resourcemanager.hostname": "172.31.34.239"},
            ["172.31.34.239:8088"],
            id="single-master-hostname",
        ),
        pytest.param(
            {
                "yarn.resourcemanager.hostname": "172.31.34.239",
                "yarn.resourcemanager.webapp.address": "ip-172-31-34-239.ec2.internal:8088",
            },
            ["ip-172-31-34-239.ec2.internal:8088"],
            id="single-master-webapp-address",
        ),
        pytest.param(
            {
                "yarn.resourcemanager.ha.enabled": "true",
                "yarn.resourcemanager.ha.rm-ids": "foo1, foo2",
                "yarn.resourcemanager.hostname.foo2": "172-31-34-239",
                "yarn.resourcemanager.hostname.foo1": "172-31-34-91",
            },
            ["172-31-34-91:8088", "172-31-34-239:8088"],
            id="multiple-masters-hostname",
        ),
        pytest.param(
            {
                "yarn.resourcemanager.ha.enabled": "true",
                "yarn.resourcemanager.ha.rm-ids": "foo1, foo2",
                "yarn.resourcemanager.hostname.foo2": "172-31-34-239",
                "yarn.resourcemanager.hostname.foo1": "172-31-34-91",
                "yarn.resourcemanager.webapp.address.foo2": "ip-172-31-34-239.ec2.internal:8088",
                "yarn.resourcemanager.webapp.address.foo1": "ip-172-31-34-91.ec2.internal:8088",
            },
            ["ip-172-31-34-91.ec2.internal:8088", "ip-172-31-34-239.ec2.internal:8088"],
            id="multiple-masters-webapp-address",
        ),
        pytest.param(
            {
                "yarn.resourcemanager.ha.rm-ids": "foo1, foo2",
                "yarn.resourcemanager.hostname.foo2": "172-31-34-239",
                "yarn.resourcemanager.hostname.foo1": "172-31-34-91",
            },
            ["0.0.0.0:8088"],
            id="default-when-ha-disabled",
        ),
    ],
)
def test_detect_resource_manager_addresses(
    logger: logging.Logger, properties: Dict[str, Any], expected: List[str]
) -> None:
    yarn_site_xml = f"""<?xml version="1.0"?>
    <configuration>
    {"".join(f'<property><name>{key}</name><value>{value}</value></property>' for key, value in properties.items())}
    </configuration>"""

    with YarnNodeMock(
        yarn_site_xml=yarn_site_xml,
    ):
        assert detect_resource_manager_addresses(logger=logger) == expected


def test_should_log_cannot_resolve_variable_error() -> None:
    yarn_site_xml = """<?xml version="1.0"?>
    <configuration>
      <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>${KEY1}:8088</value>
      </property>
      <property>
        <name>KEY1</name>
        <value>${MISSING_KEY}</value>
      </property>
    </configuration>"""

    with YarnNodeMock(
        yarn_site_xml=yarn_site_xml,
    ):
        logger = Mock()
        detect_resource_manager_addresses(logger=logger)
        logger.error.assert_called_with("YARN config error", extra={"error": "could not resolve variable: MISSING_KEY"})

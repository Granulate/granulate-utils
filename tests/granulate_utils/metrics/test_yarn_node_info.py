import logging
from typing import Any, Dict, List, Optional
from unittest.mock import Mock

import pytest

from granulate_utils.metrics.yarn.utils import YarnNodeInfo, get_yarn_node_info
from tests.granulate_utils.config_feeder.fixtures.yarn import YarnNodeMock


@pytest.mark.parametrize(
    "options, expected_index, expected_addresses",
    [
        pytest.param({}, 0, ["0.0.0.0:8088"], id="yarn-defaults"),
        pytest.param(
            {
                "yarn_config": {"yarn.resourcemanager.hostname": "172.31.34.239"},
                "ip": "172.31.34.239",
            },
            0,
            ["172.31.34.239:8088"],
            id="single-rm__config-hostname__on-first-rm",
        ),
        pytest.param(
            {
                "yarn_config": {
                    "yarn.resourcemanager.hostname": "172.31.34.239",
                    "yarn.resourcemanager.webapp.address": "ip-172-31-34-239.ec2.internal:8088",
                },
                "hostname": "ip-172-31-34-239",
            },
            0,
            ["ip-172-31-34-239.ec2.internal:8088"],
            id="single-rm__config-webapp-address__on-first-rm",
        ),
        pytest.param(
            {
                "yarn_config": {
                    "yarn.resourcemanager.ha.enabled": "true",
                    "yarn.resourcemanager.ha.rm-ids": "foo1, foo2",
                    "yarn.resourcemanager.hostname.foo2": "172.31.34.239",
                    "yarn.resourcemanager.hostname.foo1": "172.31.34.91",
                },
                "ip": "172.31.34.91",
            },
            0,
            ["172.31.34.91:8088", "172.31.34.239:8088"],
            id="multiple-rms__config-hostname__on-first-rm",
        ),
        pytest.param(
            {
                "yarn_config": {
                    "yarn.resourcemanager.ha.enabled": "true",
                    "yarn.resourcemanager.ha.rm-ids": "foo1, foo2",
                    "yarn.resourcemanager.hostname.foo2": "172.31.34.239",
                    "yarn.resourcemanager.hostname.foo1": "172.31.34.91",
                    "yarn.resourcemanager.webapp.address.foo2": "ip-172-31-34-239.ec2.internal:8088",
                    "yarn.resourcemanager.webapp.address.foo1": "ip-172-31-34-91.ec2.internal:8088",
                },
                "hostname": "ip-172-31-34-239",
            },
            1,
            ["ip-172-31-34-91.ec2.internal:8088", "ip-172-31-34-239.ec2.internal:8088"],
            id="multiple-rms__config-webapp-address__on-second-rm",
        ),
        pytest.param(
            {
                "yarn_config": {
                    "yarn.resourcemanager.ha.enabled": "true",
                    "yarn.resourcemanager.ha.rm-ids": "foo1,foo2,foo3",
                    "yarn.resourcemanager.webapp.address.foo1": "abcd.internal:8088",
                    "yarn.resourcemanager.webapp.address.foo2": "abc.internal:8088",
                    "yarn.resourcemanager.webapp.address.foo3": "ab.internal:8088",
                },
                "hostname": "ab",
            },
            2,
            ["abcd.internal:8088", "abc.internal:8088", "ab.internal:8088"],
            id="multiple-rms__config-webapp-address__on-third-rm",
        ),
        pytest.param(
            {
                "yarn_config": {
                    "yarn.resourcemanager.ha.rm-ids": "foo1, foo2",
                    "yarn.resourcemanager.hostname.foo2": "172-31-34-239",
                    "yarn.resourcemanager.hostname.foo1": "172-31-34-91",
                }
            },
            0,
            ["0.0.0.0:8088"],
            id="fallback-to-defaults-when-ha-disabled",
        ),
        pytest.param(
            {
                "yarn_config": {"yarn.resourcemanager.hostname": "172.31.34.239"},
                "ip": "172.31.34.34",
            },
            None,
            ["172.31.34.239:8088"],
            id="single-rm__config-hostname__on-worker",
        ),
        pytest.param(
            {
                "yarn_config": {
                    "yarn.resourcemanager.hostname": "172.31.34.239",
                    "yarn.resourcemanager.webapp.address": "ip-172-31-34-239.ec2.internal:8088",
                },
                "hostname": "ip-172-31-34-31",
            },
            None,
            ["ip-172-31-34-239.ec2.internal:8088"],
            id="single-rm__config-web-address__on-worker",
        ),
        pytest.param(
            {
                "yarn_config": {
                    "yarn.resourcemanager.ha.enabled": "true",
                    "yarn.resourcemanager.ha.rm-ids": "foo1, foo2",
                    "yarn.resourcemanager.hostname.foo2": "172.31.34.239",
                    "yarn.resourcemanager.hostname.foo1": "172.31.34.91",
                },
                "ip": "172.31.34.34",
            },
            None,
            ["172.31.34.91:8088", "172.31.34.239:8088"],
            id="multiple-rms__config-hostname__on-worker",
        ),
    ],
)
def test_detect_resource_manager_addresses(
    logger: logging.Logger,
    options: Dict[str, Any],
    expected_index: Optional[int],
    expected_addresses: List[str],
) -> None:
    yarn_config = options.get("yarn_config", {})
    yarn_site_xml = f"""<?xml version="1.0"?>
    <configuration>
    {"".join(f'<property><name>{key}</name><value>{value}</value></property>' for key, value in yarn_config.items())}
    </configuration>"""

    with YarnNodeMock(
        yarn_site_xml=yarn_site_xml,
        hostname=options.get("hostname", "unknown"),
        ip=options.get("ip", "unknown"),
    ):
        yarn_node_info = get_yarn_node_info(logger=logger)
        assert yarn_node_info == YarnNodeInfo(
            resource_manager_index=expected_index,
            resource_manager_webapp_addresses=expected_addresses,
            config=yarn_config,
        )
        assert yarn_node_info.is_resource_manager == (expected_index is not None)
        assert yarn_node_info.is_first_resource_manager == (expected_index == 0)


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
        assert get_yarn_node_info(logger=logger) is None
        logger.error.assert_called_with(
            "YARN config error",
            extra={"error": "could not resolve variable: MISSING_KEY"},
        )

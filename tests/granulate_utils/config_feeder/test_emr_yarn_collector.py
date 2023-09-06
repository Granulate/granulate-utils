import logging
from typing import Any, Dict

import pytest
from requests.exceptions import ConnectionError

from granulate_utils.config_feeder.client.exceptions import MaximumRetriesExceeded
from granulate_utils.config_feeder.client.yarn.collector import YarnConfigCollector
from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from tests.granulate_utils.config_feeder.fixtures.yarn import YarnNodeMock


@pytest.mark.asyncio
async def test_collect_from_master_node(logger: logging.Logger) -> None:
    job_flow_id = "j-1234567890"
    instance_id = "i-06828639fa954e04c"

    yarn_site_xml = """<?xml version="1.0"?>
    <configuration>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>${yarn.resourcemanager.hostname}:8001</value>
    </property>
    </configuration>"""

    with YarnNodeMock(
        provider="aws",
        job_flow_id=job_flow_id,
        instance_id=instance_id,
        is_master=True,
        yarn_site_xml=yarn_site_xml,
        web_address="http://0.0.0.0:8001",
    ) as mock:
        assert await YarnConfigCollector(logger=logger).collect(mock.node_info) == YarnConfig(
            config=mock.expected_config,
        )


@pytest.mark.asyncio
async def test_collect_from_worker_noder(logger: logging.Logger) -> None:
    job_flow_id = "j-1234567890"
    instance_id = "i-0c97511ec7fa849a3"

    with YarnNodeMock(
        provider="aws",
        job_flow_id=job_flow_id,
        instance_id=instance_id,
        is_master=False,
        web_address="http://0.0.0.0:8042",
    ) as mock:
        assert await YarnConfigCollector(logger=logger).collect(mock.node_info) == YarnConfig(
            config=mock.expected_config,
        )


@pytest.mark.parametrize(
    "is_master, web_address",
    [
        pytest.param(True, "http://localhost:8088", id="master"),
        pytest.param(False, "http://0.0.0.0:8042", id="worker"),
    ],
)
@pytest.mark.asyncio
async def test_should_fail_with_max_retries_exception(
    is_master: bool, web_address: str, logger: logging.Logger
) -> None:
    with YarnNodeMock(
        is_master=is_master,
        web_address=web_address,
        home_dir="/home/not-hadoop",
        response={"exc": ConnectionError},
    ) as mock:
        with pytest.raises(MaximumRetriesExceeded, match="maximum number of failed requests reached for /conf"):
            collector = YarnConfigCollector(max_retries=3, logger=logger)
            while True:
                await collector.collect(mock.node_info)


@pytest.mark.asyncio
async def test_should_mask_sensitive_values(logger: logging.Logger) -> None:
    with YarnNodeMock(
        provider="aws",
        job_flow_id="j-1234567890",
        instance_id="i-06828639fa954e04c",
        is_master=True,
        properties=[
            {
                "key": "yarn.federation.state-store.sql.password",
                "value": "password1",
                "isFinal": False,
                "resource": "yarn-site.xml",
            }
        ],
    ) as mock:
        result = await YarnConfigCollector(logger=logger).collect(mock.node_info)

        assert result is not None
        assert result.config == {
            "properties": [
                {
                    "key": "yarn.federation.state-store.sql.password",
                    "value": "*****",
                    "resource": "yarn-site.xml",
                }
            ]
        }


@pytest.mark.parametrize(
    "all_properties, expected_properties",
    [
        pytest.param(True, ["yarn.scheduler.minimum-allocation-mb", "mapreduce.map.memory.mb"], id="all-properties"),
        pytest.param(False, ["yarn.scheduler.minimum-allocation-mb"], id="only-yarn-properties"),
    ],
)
@pytest.mark.asyncio
async def test_collect_all_properties(
    all_properties: bool, expected_properties: Dict[str, Any], logger: logging.Logger
) -> None:
    with YarnNodeMock(
        provider="aws",
        job_flow_id="j-1234567890",
        instance_id="i-06828639fa954e04c",
        is_master=True,
        properties=[
            {
                "key": "yarn.scheduler.minimum-allocation-mb",
                "value": "2048",
                "isFinal": False,
                "resource": "yarn-site.xml",
            },
            {
                "key": "mapreduce.map.memory.mb",
                "value": "4096",
                "isFinal": False,
                "resource": "mapred-default.xml",
            },
        ],
    ) as mock:
        result = await YarnConfigCollector(all_properties=all_properties, logger=logger).collect(mock.node_info)

        assert result is not None
        assert [p["key"] for p in result.config["properties"]] == expected_properties

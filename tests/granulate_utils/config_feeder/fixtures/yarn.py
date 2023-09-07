from __future__ import annotations

from typing import Any, Dict, List, Optional

from requests.exceptions import ConnectionError

from granulate_utils.config_feeder.core.models.node import NodeInfo
from granulate_utils.metrics.yarn.utils import RM_DEFAULT_ADDRESS
from tests.granulate_utils.config_feeder.fixtures.base import NodeMockBase
from tests.granulate_utils.config_feeder.fixtures.dataproc import DataprocNodeMock
from tests.granulate_utils.config_feeder.fixtures.emr import EmrNodeMock


class YarnNodeMock(NodeMockBase):
    def __init__(
        self,
        *,
        provider: str = "aws",
        job_flow_id: str = "",
        cluster_uuid: str = "",
        instance_id: str = "",
        is_master: bool = False,
        web_address: str = RM_DEFAULT_ADDRESS,
        home_dir: str = "/home/hadoop/hadoop",
        yarn_site_xml: str = "",
        hostname: str = "",
        ip: str = "",
        properties: Optional[List[Dict[str, Any]]] = None,
        response: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._node_mock = (
            EmrNodeMock(job_flow_id=job_flow_id, instance_id=instance_id, is_master=is_master)
            if provider == "aws"
            else DataprocNodeMock(cluster_uuid=cluster_uuid, instance_id=instance_id, is_master=is_master)
        )
        self._configure_properties(properties)

        self._node_mock.mock_file("/home/hadoop/hadoop/etc/hadoop/yarn-site.xml", yarn_site_xml)

        self._node_mock.mock_command_stdout(
            "ps -ax",
            f"""12345 ?  Sl  0:04 java
                -Dyarn.home.dir={home_dir}
                -Dyarn.log.file=rm.log
                org.apache.hadoop.yarn.server.resourcemanager.ResourceManager""",
        )

        if web_address != RM_DEFAULT_ADDRESS:
            self._node_mock.mock_http_response("GET", f"{RM_DEFAULT_ADDRESS}/conf", {"exc": ConnectionError})

        response = response or {"json": {"properties": self._properties}}
        self._node_mock.mock_http_response("GET", f"{web_address}/conf", response)

        if hostname:
            self._node_mock.mock_hostname(hostname)

        if ip:
            self._node_mock.mock_ip(ip),

    @property
    def node_info(self) -> NodeInfo:
        return self._node_mock.node_info

    def _configure_properties(self, properties: Optional[List[Dict[str, Any]]]) -> None:
        self._properties = (
            properties
            if properties is not None
            else [
                {
                    "key": "yarn.resourcemanager.resource-tracker.client.thread-count",
                    "value": "64",
                    "isFinal": False,
                    "resource": "yarn-site.xml",
                }
            ]
        )

        self.expected_config = {
            "properties": [{k: v for k, v in prop.items() if k != "isFinal"} for prop in self._properties]
        }

    def __enter__(self) -> YarnNodeMock:
        self._node_mock.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_tb: Any) -> None:
        self._node_mock.__exit__(None, None, None)

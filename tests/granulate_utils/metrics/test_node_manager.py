from unittest.mock import Mock, patch

import pytest

from granulate_utils.metrics.yarn.node_manager import NodeManagerAPI

NM_ADDRESS = "http://localhost:8042"


def test_json_conf_response(yarn_config: dict) -> None:
    resp = Mock()
    resp.headers = {"Content-Type": "application/json"}
    resp.json = lambda: yarn_config
    with patch(
        "granulate_utils.metrics.yarn.yarn_web_service.rest_request",
        return_value=resp,
    ) as mock_rest_request:
        assert NodeManagerAPI(NM_ADDRESS).conf() == yarn_config["properties"]
        mock_rest_request.assert_called_once_with(
            f"{NM_ADDRESS}/conf", requests_kwargs={"headers": {"Accept": "application/json"}}
        )


def test_xml_conf_response() -> None:
    resp = Mock()
    resp.headers = {"Content-Type": "text/xml"}
    resp.text = """<?xml version="1.0"?>
      <configuration>
        <property>
          <name>mapreduce.jobtracker.address</name>
          <value>local</value>
          <source>mapred-default.xml</source>
        </property>
      </configuration>"""
    with patch(
        "granulate_utils.metrics.yarn.yarn_web_service.rest_request",
        return_value=resp,
    ) as mock_rest_request:
        assert NodeManagerAPI(NM_ADDRESS).conf() == [
            {"key": "mapreduce.jobtracker.address", "value": "local", "resource": "mapred-default.xml"}
        ]
        mock_rest_request.assert_called_once_with(
            f"{NM_ADDRESS}/conf", requests_kwargs={"headers": {"Accept": "application/json"}}
        )


def test_should_raise() -> None:
    resp = Mock()
    resp.headers = {"Content-Type": "text/plain"}
    with pytest.raises(Exception, match="unsupported content type: text/plain"):
        with patch("granulate_utils.metrics.yarn.yarn_web_service.rest_request", return_value=resp):
            NodeManagerAPI(NM_ADDRESS).conf()

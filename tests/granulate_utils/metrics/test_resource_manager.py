from unittest.mock import patch

import pytest

from granulate_utils.metrics.yarn.resource_manager import InvalidResourceManagerVersionError, ResourceManagerAPI

RM_ADDRESS = "http://localhost:8008"
PARAMS = {"param": 5}


@pytest.mark.parametrize(
    "response, expected",
    [
        pytest.param({"apps": None}, [], id="null-response"),
        pytest.param({"apps": {}}, [], id="empty-response"),
        pytest.param({"apps": {"app": [{"name": "name1"}]}}, [{"name": "name1"}], id="single-app"),
    ],
)
def test_apps_endpoint(response, expected) -> None:
    with patch(
        "granulate_utils.metrics.yarn.resource_manager.json_request",
        return_value=response,
    ) as mock_json_request:
        assert ResourceManagerAPI(RM_ADDRESS).apps(**PARAMS) == expected
        mock_json_request.assert_called_once_with(f"{RM_ADDRESS}/ws/v1/cluster/apps", {}, **PARAMS)


@pytest.mark.parametrize(
    "response, expected",
    [
        pytest.param({"clusterMetrics": None}, None, id="null-response"),
        pytest.param({"clusterMetrics": {}}, {}, id="empty-response"),
    ],
)
def test_metrics_endpoint(response, expected) -> None:
    with patch(
        "granulate_utils.metrics.yarn.resource_manager.json_request",
        return_value=response,
    ) as mock_json_request:
        assert ResourceManagerAPI(RM_ADDRESS).metrics(**PARAMS) == expected
        mock_json_request.assert_called_once_with(f"{RM_ADDRESS}/ws/v1/cluster/metrics", {}, **PARAMS)


@pytest.mark.parametrize(
    "response, expected",
    [
        pytest.param({"nodes": None}, [], id="null-response"),
        pytest.param({"nodes": {}}, [], id="empty-response"),
        pytest.param(
            {"nodes": {"node": [{"state": "RUNNING"}]}},
            [{"state": "RUNNING"}],
            id="single-node",
        ),
    ],
)
def test_nodes_endpoint(response, expected) -> None:
    with patch(
        "granulate_utils.metrics.yarn.resource_manager.json_request",
        return_value=response,
    ) as mock_json_request:
        assert ResourceManagerAPI(RM_ADDRESS).nodes(**PARAMS) == expected
        mock_json_request.assert_called_once_with(f"{RM_ADDRESS}/ws/v1/cluster/nodes", {}, **PARAMS)


@pytest.mark.parametrize(
    "response, expected",
    [
        pytest.param({"scheduler": None}, None, id="null-response"),
        pytest.param({"scheduler": {}}, None, id="empty-response"),
        pytest.param({"scheduler": {"schedulerInfo": None}}, None, id="null-scheduler-info"),
        pytest.param(
            {"scheduler": {"schedulerInfo": {"queueName": "root"}}},
            {"queueName": "root"},
            id="with-info",
        ),
    ],
)
def test_scheduler_endpoint(response, expected) -> None:
    with patch(
        "granulate_utils.metrics.yarn.resource_manager.json_request",
        return_value=response,
    ) as mock_json_request:
        assert ResourceManagerAPI(RM_ADDRESS).scheduler(**PARAMS) == expected
        mock_json_request.assert_called_once_with(f"{RM_ADDRESS}/ws/v1/cluster/scheduler", {}, **PARAMS)


@pytest.mark.parametrize(
    "response, expected",
    [
        pytest.param({"beans": None}, [], id="null-response"),
        pytest.param({"beans": []}, [], id="empty-response"),
        pytest.param({"beans": [{"AppsCompleted": 5}]}, [{"AppsCompleted": 5}], id="single-bean"),
    ],
)
def test_beans_endpoint(response, expected) -> None:
    with patch(
        "granulate_utils.metrics.yarn.resource_manager.json_request",
        return_value=response,
    ) as mock_json_request:
        assert ResourceManagerAPI(RM_ADDRESS).beans() == expected
        mock_json_request.assert_called_once_with(f"{RM_ADDRESS}/jmx", {})


@pytest.mark.parametrize(
    "rm_version, test_version, expected",
    [
        pytest.param("3.3.4", "3.3.4", True, id="same-version"),
        pytest.param("3.3.4", "3.3.5", False, id="lower-version"),
        pytest.param("3.3.3-amzn-4", "3.3.3", True, id="vendor-suffix"),
        pytest.param("2.7.3.2.6.1.0-129", "2.7.3", True, id="patch-version"),
    ],
)
def test_version_check(rm_version, test_version, expected) -> None:
    with patch(
        "granulate_utils.metrics.yarn.resource_manager.json_request",
        return_value={"clusterInfo": {"resourceManagerVersion": rm_version}},
    ) as mock_json_request:
        rmapi = ResourceManagerAPI(RM_ADDRESS)
        assert rmapi.version == rm_version
        assert rmapi.is_version_at_least(test_version) == expected
        mock_json_request.assert_called_once_with(f"{RM_ADDRESS}/ws/v1/cluster/info", {})


def test_invalid_version() -> None:
    with patch(
        "granulate_utils.metrics.yarn.resource_manager.json_request",
        return_value={"clusterInfo": {"resourceManagerVersion": "my-version"}},
    ):
        with pytest.raises(InvalidResourceManagerVersionError, match="Invalid ResourceManager version: my-version"):
            ResourceManagerAPI(RM_ADDRESS).is_version_at_least("3.3.4")

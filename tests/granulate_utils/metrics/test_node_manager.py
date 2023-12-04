from unittest.mock import patch

from granulate_utils.metrics.yarn.node_manager import NodeManagerAPI

NM_ADDRESS = "http://localhost:8042"


def test_conf_request(yarn_config: dict) -> None:
    with patch(
        "granulate_utils.metrics.yarn.yarn_web_service.json_request",
        return_value=yarn_config,
    ) as mock_json_request:
        assert NodeManagerAPI(NM_ADDRESS).conf() == yarn_config["properties"]
        mock_json_request.assert_called_once_with(f"{NM_ADDRESS}/conf", {})

from unittest.mock import Mock

import pytest

from granulate_utils.config_feeder.client import get_node_info
from tests.granulate_utils.config_feeder.fixtures.dataproc import DataprocNodeMock


@pytest.mark.asyncio
async def test_should_log_missing_metadata_key() -> None:
    logger = Mock()
    with DataprocNodeMock(metadata_response="{}"):
        assert get_node_info(logger) is None
        logger.error.assert_called_with("expected dataproc metadata key was not found", extra={"key": "attributes"})


@pytest.mark.asyncio
async def test_should_log_invalid_metadata_json() -> None:
    logger = Mock()
    with DataprocNodeMock(metadata_response="{"):
        assert get_node_info(logger) is None
        logger.error.assert_called_with("got invalid dataproc metadata JSON")

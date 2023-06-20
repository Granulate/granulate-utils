import pytest

from granulate_utils.config_feeder.client.bigdata import get_node_info


@pytest.fixture(autouse=True)
def clear_cache() -> None:
    get_node_info.cache_clear()

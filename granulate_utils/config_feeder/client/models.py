from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, root_validator

from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from granulate_utils.config_feeder.core.models.node import NodeInfo
from granulate_utils.config_feeder.core.utils import get_config_hash


class CollectionResult(BaseModel):
    node: NodeInfo

    yarn_config: Optional[YarnConfig]
    yarn_config_hash: str = ""

    @root_validator(pre=False)
    def _set_hashes(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if yarn_config := values.get("yarn_config"):
            values["yarn_config_hash"] = get_config_hash(yarn_config.config)
        return values

    @property
    def is_empty(self) -> bool:
        return self.yarn_config is None


class ConfigType(Enum):
    YARN = 0

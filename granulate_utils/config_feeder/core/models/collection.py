import json
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, root_validator

from granulate_utils.config_feeder.core.utils import get_config_hash


class CollectorType(str, Enum):
    UNKNOWN = "unknown"
    SAGENT = "sagent"
    GPROFILER = "gprofiler"


class CollectionResult(BaseModel):
    config: Optional[Dict[str, Any]]
    config_hash: str = ""

    @root_validator(pre=False)
    def _set_hashes(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if config := values.get("config"):
            values["config_hash"] = get_config_hash(config)
        return values

    @property
    def is_empty(self) -> bool:
        return self.config is None

    @property
    def serialized(self) -> str:
        return json.dumps(self.config)

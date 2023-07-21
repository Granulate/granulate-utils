from enum import Enum
from typing import Any, Dict

from pydantic import BaseModel


class AutoScalingMode(str, Enum):
    UNKNOWN = "unknown"
    CUSTOM = "custom"
    MANAGED = "managed"


class AutoScalingConfig(BaseModel):
    mode: AutoScalingMode
    config: Dict[str, Any]

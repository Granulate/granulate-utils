from typing import Any, Dict

from pydantic import BaseModel

from granulate_utils.config_feeder.core.models.autoscaling import AutoScalingMode


class AutoScalingConfig(BaseModel):
    mode: AutoScalingMode
    config: Dict[str, Any]

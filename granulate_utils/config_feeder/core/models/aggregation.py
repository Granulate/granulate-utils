from typing import Any, Dict, Optional

from pydantic import BaseModel, root_validator

from granulate_utils.config_feeder.core.models.autoscaling import (
    ClusterAutoScalingConfig,
    ClusterAutoScalingConfigCreate,
)
from granulate_utils.config_feeder.core.models.yarn import NodeYarnConfig, NodeYarnConfigCreate


class CreateNodeConfigsRequest(BaseModel):
    yarn_config: Optional[NodeYarnConfigCreate] = None
    autoscaling_config: Optional[ClusterAutoScalingConfigCreate] = None

    @root_validator
    def at_least_one_config_is_required(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("yarn_config") is not None:
            return values
        if values.get("autoscaling_config") is not None:
            return values
        raise ValueError("at least one config is required")


class CreateNodeConfigsResponse(BaseModel):
    yarn_config: Optional[NodeYarnConfig]
    autoscaling_config: Optional[ClusterAutoScalingConfig]

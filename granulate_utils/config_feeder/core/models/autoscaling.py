from datetime import datetime
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel

from granulate_utils.config_feeder.core.models.collection import CollectorType


class AutoScalingMode(str, Enum):
    UNKNOWN = "unknown"
    CUSTOM = "custom"
    MANAGED = "managed"


class AutoScalingConfig(BaseModel):
    mode: AutoScalingMode
    config: Dict[str, Any]


class ClusterAutoScalingConfig(BaseModel):
    autoscaling_config_id: str
    cluster_id: str
    mode: AutoScalingMode
    config_hash: str
    config_json: Dict[str, Any]
    ts: datetime


class ClusterAutoScalingConfigCreate(BaseModel):
    collector_type: CollectorType
    mode: AutoScalingMode
    config_json: str


class CreateClusterAutoScalingConfigRequest(BaseModel):
    autoscaling_config: ClusterAutoScalingConfigCreate


class CreateClusterAutoScalingConfigResponse(BaseModel):
    autoscaling_config: ClusterAutoScalingConfig


class GetClusterAutoScalingConfigsResponse(BaseModel):
    autoscaling_configs: List[ClusterAutoScalingConfig]

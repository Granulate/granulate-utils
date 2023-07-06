from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel

from granulate_utils.config_feeder.core.models.collection import CollectorType


class NodeYarnConfigCreate(BaseModel):
    collector_type: CollectorType
    config_json: str


class NodeYarnConfig(BaseModel):
    node_id: str
    yarn_config_id: str
    config_hash: str
    config_json: Dict[str, Any]
    ts: datetime


class CreateNodeYarnConfigRequest(BaseModel):
    yarn_config: NodeYarnConfigCreate


class CreateNodeYarnConfigResponse(BaseModel):
    yarn_config: NodeYarnConfig


class GetNodeYarnConfigsResponse(BaseModel):
    yarn_configs: List[NodeYarnConfig]

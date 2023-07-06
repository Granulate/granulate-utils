from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel

from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.collection import CollectorType


class NodeInfo(BaseModel):
    provider: CloudProvider
    bigdata_platform: BigDataPlatform
    external_cluster_id: str
    external_id: str
    is_master: bool
    properties: Dict[str, Any] = {}


class NodeBase(BaseModel):
    collector_type: CollectorType
    external_id: str
    is_master: bool = False


class NodeCreate(NodeBase):
    pass


class Node(NodeBase):
    id: str
    ts: datetime


class CreateNodeRequest(BaseModel):
    node: NodeCreate
    allow_existing: bool = False


class CreateNodeResponse(BaseModel):
    node: Node


class GetNodesResponse(BaseModel):
    nodes: List[Node]


class GetNodeResponse(BaseModel):
    node: Node

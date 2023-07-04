from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from granulate_utils.config_feeder.core.models.collection import CollectorType


class BigDataPlatform(str, Enum):
    UNKNOWN = "unknown"
    DATAPROC = "dataproc"
    DATABRICKS = "databricks"
    EMR = "emr"


class CloudProvider(str, Enum):
    UNKNOWN = "unknown"
    AWS = "aws"
    GCP = "gcp"


class ClusterBase(BaseModel):
    collector: CollectorType
    provider: CloudProvider
    bigdata_platform: BigDataPlatform
    external_id: str


class ClusterCreate(ClusterBase):
    service: str
    properties: Optional[str] = None


class Cluster(ClusterBase):
    id: str
    properties: Optional[Dict[str, Any]]
    ts: datetime


class CreateClusterRequest(BaseModel):
    cluster: ClusterCreate
    allow_existing: bool = False


class CreateClusterResponse(BaseModel):
    cluster: Cluster


class GetClustersResponse(BaseModel):
    clusters: List[Cluster]


class GetClusterResponse(BaseModel):
    cluster: Cluster

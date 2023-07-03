from datetime import datetime
from enum import Enum
from typing import List

from pydantic import BaseModel

from granulate_utils.config_feeder.core.models.collection import CollectorType


class BigDataPlatform(str, Enum):
    UNKOWN = "unknown"
    EMR = "emr"
    DATAPROC = "dataproc"
    DATABRICKS = "databricks"


class CloudProvider(str, Enum):
    UNKOWN = "unknown"
    AWS = "aws"
    GCP = "gcp"


class ClusterBase(BaseModel):
    collector: CollectorType
    provider: CloudProvider
    external_id: str


class ClusterCreate(ClusterBase):
    service: str


class Cluster(ClusterBase):
    id: str
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

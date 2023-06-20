from datetime import datetime
from typing import List, Literal

from pydantic import BaseModel

BigDataPlatform = Literal["unknown", "emr", "dataproc", "databricks"]
CloudProvider = Literal["unknown", "aws", "gcp"]


class ClusterBase(BaseModel):
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

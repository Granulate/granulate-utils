import datetime

from pydantic import BaseModel


class DatabricksApiKeyBase(BaseModel):
    service_name: int
    api_key: str


class DatabricksApiKey(BaseModel):
    id: int
    client_id: int
    service_id: int
    api_key: str
    ts: datetime.datetime


class CreateApiKeyRequest(BaseModel):
    api_key: DatabricksApiKeyBase


class CreateApiKeyResponse(BaseModel):
    api_key: DatabricksApiKey


class GetApiKeyResponse(BaseModel):
    api_key: DatabricksApiKey

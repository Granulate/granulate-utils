from pydantic import BaseModel

from granulate_utils.config_feeder.core.models.collection import CollectorType


class NodeResourceConfigCreate(BaseModel):
    collector_type: CollectorType
    config_json: str

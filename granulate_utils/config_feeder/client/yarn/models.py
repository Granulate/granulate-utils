from typing import Any, Dict

from pydantic import BaseModel


class YarnConfig(BaseModel):
    config: Dict[str, Any]

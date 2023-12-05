from abc import ABC
from typing import Any, Dict, List, Type, TypeVar

from granulate_utils.metrics import json_request

T = TypeVar("T")


class YarnWebService(ABC):
    def __init__(self, address: str):
        self.address = address
        self._conf_url = f"{address}/conf"

    def conf(self) -> List[Dict[str, Any]]:
        """
        Get running service configuration

        most recent config is returned

        supported version: 2.8.3+
        """
        return json_request(self._conf_url, {}).get("properties") or []

    def request(self, path: str, return_path: str, return_type: Type[T], **kwargs) -> T:
        target_url = f"{self.address}/{path}"
        response = json_request(target_url, {}, **kwargs)
        return self._parse_response(response, return_path.split("."))

    @staticmethod
    def _parse_response(response: Dict[str, Any], nested_attributes: List[str]) -> Any:
        for attribute in nested_attributes:
            response = response.get(attribute) or {}
        return response

import xml.etree.ElementTree as ET
from abc import ABC
from typing import Any, Dict, List, Type, TypeVar

from granulate_utils.metrics import json_request, rest_request

T = TypeVar("T")


class YarnWebService(ABC):
    def __init__(self, address: str):
        self.address = address
        self._conf_url = f"{address}/conf"

    def conf(self) -> List[Dict[str, Any]]:
        """
        Get running service configuration

        most recent config is returned

        supported version: 2.6.5+
        """
        requests_kwargs = {"headers": {"Accept": "application/json"}}
        resp = rest_request(self._conf_url, requests_kwargs=requests_kwargs)
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return resp.json().get("properties") or []
        if "text/xml" in content_type:
            return self._parse_xml_config(resp.text)
        raise ValueError(f"unsupported content type: {content_type}")

    def request(self, path: str, return_path: str, return_type: Type[T], **kwargs) -> T:
        target_url = f"{self.address}/{path}"
        response = json_request(target_url, {}, **kwargs)
        return self._parse_response(response, return_path.split("."))

    @staticmethod
    def _parse_response(response: Dict[str, Any], nested_attributes: List[str]) -> Any:
        for attribute in nested_attributes:
            response = response.get(attribute) or {}
        return response

    @staticmethod
    def _parse_xml_config(xml_config: str) -> List[Dict[str, Any]]:
        root = ET.fromstring(xml_config)
        result = []
        for prop in root.findall("./property"):
            name = prop.find("name")
            value = prop.find("value")
            resource = prop.find("source")
            if name is not None and value is not None and resource is not None:
                result.append({"key": name.text, "value": value.text, "resource": resource.text})
        return result

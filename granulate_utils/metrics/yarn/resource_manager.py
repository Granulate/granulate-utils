#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#
import re
from functools import cached_property
from typing import Any, Dict, List, Optional, Type, TypeVar

from packaging.version import Version

from granulate_utils.metrics import json_request

YARN_RM_CLASSNAME = "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager"
REGEX_SEM_VER = re.compile(r"(\d+\.\d+\.\d+)")


T = TypeVar("T")


class InvalidResourceManagerVersionError(Exception):
    pass


class ResourceManagerAPI:
    def __init__(self, rm_address: str, kerberos_enabled: bool = False) -> None:
        self._rm_address = rm_address
        self._requests_kwargs = {}
        if kerberos_enabled:
            self._requests_kwargs["kerberos_enabled"] = kerberos_enabled
        self._apps_url = f"{rm_address}/ws/v1/cluster/apps"
        self._metrics_url = f"{rm_address}/ws/v1/cluster/metrics"
        self._nodes_url = f"{rm_address}/ws/v1/cluster/nodes"
        self._scheduler_url = f"{rm_address}/ws/v1/cluster/scheduler"
        self._info_url = f"{rm_address}/ws/v1/cluster/info"
        self._jmx_url = f"{rm_address}/jmx"

    def apps(self, **kwargs) -> List[Dict]:
        apps = json_request(self._apps_url, {}, requests_kwargs=self._requests_kwargs, **kwargs).get("apps") or {}
        return apps.get("app", [])

    def metrics(self, **kwargs) -> Optional[Dict]:
        return json_request(self._metrics_url, {}, requests_kwargs=self._requests_kwargs, **kwargs).get(
            "clusterMetrics"
        )

    def nodes(self, **kwargs) -> List[Dict]:
        nodes = json_request(self._nodes_url, {}, requests_kwargs=self._requests_kwargs, **kwargs).get("nodes") or {}
        return nodes.get("node", [])

    def scheduler(self, **kwargs) -> Optional[Dict]:
        scheduler = (
            json_request(self._scheduler_url, {}, requests_kwargs=self._requests_kwargs, **kwargs).get("scheduler")
            or {}
        )
        return scheduler.get("schedulerInfo")

    def beans(self) -> List[Dict]:
        return json_request(self._jmx_url, {}, requests_kwargs=self._requests_kwargs).get("beans") or []

    def request(self, url: str, return_path: str, return_type: Type[T], **kwargs) -> T:
        target_url = f"{self._rm_address}/{url}"
        response = json_request(target_url, {}, requests_kwargs=self._requests_kwargs, **kwargs)
        return self._parse_response(response, return_path.split("."))

    @staticmethod
    def _parse_response(response: Dict[str, Any], nested_attributes: List[str]) -> Any:
        for attribute in nested_attributes:
            response = response.get(attribute) or {}
        return response

    @cached_property
    def version(self) -> str:
        return json_request(self._info_url, {}, requests_kwargs=self._requests_kwargs)["clusterInfo"][
            "resourceManagerVersion"
        ]

    @cached_property
    def sem_version(self) -> Version:
        if sem_version := REGEX_SEM_VER.search(self.version):
            return Version(sem_version.group(1))
        raise InvalidResourceManagerVersionError(f"Invalid ResourceManager version: {self.version}")

    def is_version_at_least(self, version: str) -> bool:
        return self.sem_version >= Version(version)

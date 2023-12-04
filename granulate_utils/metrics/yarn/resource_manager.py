#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#
import re
from functools import cached_property
from typing import Dict, List, Optional

from packaging.version import Version

from granulate_utils.metrics import json_request
from granulate_utils.metrics.yarn.yarn_web_service import YarnWebService

YARN_RM_CLASSNAME = "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager"
REGEX_SEM_VER = re.compile(r"(\d+\.\d+\.\d+)")


class InvalidResourceManagerVersionError(Exception):
    pass


class ResourceManagerAPI(YarnWebService):
    def __init__(self, rm_address: str):
        super().__init__(rm_address)
        self._apps_url = f"{rm_address}/ws/v1/cluster/apps"
        self._metrics_url = f"{rm_address}/ws/v1/cluster/metrics"
        self._nodes_url = f"{rm_address}/ws/v1/cluster/nodes"
        self._scheduler_url = f"{rm_address}/ws/v1/cluster/scheduler"
        self._info_url = f"{rm_address}/ws/v1/cluster/info"
        self._jmx_url = f"{rm_address}/jmx"

    def apps(self, **kwargs) -> List[Dict]:
        apps = json_request(self._apps_url, {}, **kwargs).get("apps") or {}
        return apps.get("app", [])

    def metrics(self, **kwargs) -> Optional[Dict]:
        return json_request(self._metrics_url, {}, **kwargs).get("clusterMetrics")

    def nodes(self, **kwargs) -> List[Dict]:
        nodes = json_request(self._nodes_url, {}, **kwargs).get("nodes") or {}
        return nodes.get("node", [])

    def scheduler(self, **kwargs) -> Optional[Dict]:
        scheduler = json_request(self._scheduler_url, {}, **kwargs).get("scheduler") or {}
        return scheduler.get("schedulerInfo")

    def beans(self) -> List[Dict]:
        return json_request(self._jmx_url, {}).get("beans") or []

    @cached_property
    def version(self) -> str:
        return json_request(self._info_url, {})["clusterInfo"]["resourceManagerVersion"]

    @cached_property
    def sem_version(self) -> Version:
        if sem_version := REGEX_SEM_VER.search(self.version):
            return Version(sem_version.group(1))
        raise InvalidResourceManagerVersionError(f"Invalid ResourceManager version: {self.version}")

    def is_version_at_least(self, version: str) -> bool:
        return self.sem_version >= Version(version)

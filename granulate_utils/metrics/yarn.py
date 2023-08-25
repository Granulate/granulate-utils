#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#
import logging
from functools import cached_property
from typing import Dict, Iterable, List, Optional

from packaging.version import Version

from granulate_utils.metrics import Collector, Sample, json_request, samples_from_json
from granulate_utils.metrics.metrics import YARN_CLUSTER_METRICS, YARN_NODES_METRICS

YARN_RM_CLASSNAME = "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager"


class ResourceManagerAPI:
    def __init__(self, rm_address: str):
        self._apps_url = f"{rm_address}/ws/v1/cluster/apps"
        self._metrics_url = f"{rm_address}/ws/v1/cluster/metrics"
        self._nodes_url = f"{rm_address}/ws/v1/cluster/nodes"
        self._scheduler_url = f"{rm_address}/ws/v1/cluster/scheduler"
        self._info_url = f"{rm_address}/ws/v1/cluster/info"

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

    @cached_property
    def version(self) -> Version:
        return Version(json_request(self._info_url, {})["clusterInfo"]["resourceManagerVersion"])

    def is_version_at_least(self, version: str) -> bool:
        return self.version >= Version(version)


class YarnCollector(Collector):
    name = "yarn"

    def __init__(self, rm_address: str, logger: logging.LoggerAdapter) -> None:
        self.rm_address = rm_address
        self.rm = ResourceManagerAPI(self.rm_address)
        self.logger = logger

    def collect(self) -> Iterable[Sample]:
        try:
            yield from self._cluster_metrics()
            yield from self._nodes_metrics()
        except Exception:
            self.logger.exception("Could not gather yarn metrics")

    def _cluster_metrics(self) -> Iterable[Sample]:
        try:
            if cluster_metrics := self.rm.metrics():
                yield from samples_from_json({}, cluster_metrics, YARN_CLUSTER_METRICS)
        except Exception:
            self.logger.exception("Could not gather yarn cluster metrics")

    def _nodes_metrics(self) -> Iterable[Sample]:
        try:
            for node in self.rm.nodes(states=self._active_node_states):
                for metric, value in node.get("resourceUtilization", {}).items():
                    node[metric] = value  # this will create all relevant metrics under same dictionary

                labels = {"node_hostname": node["nodeHostName"]}
                yield from samples_from_json(labels, node, YARN_NODES_METRICS)
        except Exception:
            self.logger.exception("Could not gather yarn nodes metrics")

    @cached_property
    def _active_node_states(self) -> str:
        """
        Returns all the states that are considered 'active' for a node.

        Taken from isActiveState in:
            https://github.com/apache/hadoop/blob/a91933620d8755e80ad4bdf900b506dd73d26786/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/NodeState.java#L65

        Also, we don't want to collect DECOMMISSIONED because in EMR nodes are
        considered DECOMMISSIONED forever and are never removed from the nodes list
        """

        # DECOMMISSIONING was added in 2.8.0
        if self.rm.is_version_at_least("2.8.0"):
            return "NEW,RUNNING,UNHEALTHY,DECOMMISSIONING"
        else:
            return "NEW,RUNNING,UNHEALTHY"

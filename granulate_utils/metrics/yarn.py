#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#
import logging
from typing import Dict, Iterable, List, Optional

from granulate_utils.metrics import Collector, Sample, json_request, samples_from_json
from granulate_utils.metrics.metrics import YARN_CLUSTER_METRICS, YARN_NODES_METRICS

YARN_RM_CLASSNAME = "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager"


class ResourceManagerAPI:
    def __init__(self, rm_address: str):
        self._apps_url = f"{rm_address}/ws/v1/cluster/apps"
        self._metrics_url = f"{rm_address}/ws/v1/cluster/metrics"
        self._nodes_url = f"{rm_address}/ws/v1/cluster/nodes"

    def apps(self, **kwargs) -> List[Dict]:
        return json_request(self._apps_url, **kwargs).get("apps", {}).get("app", [])

    def metrics(self, **kwargs) -> Optional[Dict]:
        return json_request(self._metrics_url, **kwargs).get("clusterMetrics")

    def nodes(self, **kwargs) -> List[Dict]:
        return json_request(self._nodes_url, **kwargs).get("nodes", {}).get("node", [])


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
            # This are all the statuses that defined as 'active node' in:
            # isActiveState in
            # https://github.com/apache/hadoop/blob/a91933620d8755e80ad4bdf900b506dd73d26786/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/NodeState.java#L65
            # Also, we don't want to collect DECOMMISSIONED because in EMR,
            # nodes are considered DECOMMISSIONED forever and are never removed from the nodes list
            for node in self.rm.nodes(states="NEW,RUNNING,UNHEALTHY,DECOMMISSIONING"):
                for metric, value in node.get("resourceUtilization", {}).items():
                    node[
                        metric
                    ] = value  # this will create all relevant metrics under same dictionary

                labels = {"node_hostname": node["nodeHostName"]}
                yield from samples_from_json(labels, node, YARN_NODES_METRICS)
        except Exception:
            self.logger.exception("Could not gather yarn nodes metrics")

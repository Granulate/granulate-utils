#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
from typing import Any, Dict, Generator

from granulate_utils.metrics import rest_request_to_json, set_metrics_from_json

YARN_CLUSTER_PATH = "ws/v1/cluster/metrics"
YARN_NODES_PATH = "ws/v1/cluster/nodes"

YARN_CLUSTER_METRICS = {
    metric: f"yarn_cluster_{metric}"
    for metric in (
        "appsSubmitted",
        "appsCompleted",
        "appsPending",
        "appsRunning",
        "appsFailed",
        "appsKilled",
        "totalMB",
        "availableMB",
        "allocatedMB",
        "availableVirtualCores",
        "allocatedVirtualCores",
        "totalNodes",
        "activeNodes",
        "lostNodes",
        "decommissioningNodes",
        "decommissionedNodes",
        "rebootedNodes",
        "shutdownNodes",
        "unhealthyNodes",
        "containersAllocated",
        "containersPending",
    )
}
YARN_NODES_METRICS = {
    metric: f"yarn_node_{metric}"
    for metric in (
        "numContainers",
        "usedMemoryMB",
        "availMemoryMB",
        "usedVirtualCores",
        "availableVirtualCores",
        "nodePhysicalMemoryMB",
        "nodeVirtualMemoryMB",
        "nodeCPUUsage",
        "containersCPUUsage",
        "aggregatedContainersPhysicalMemoryMB",
        "aggregatedContainersVirtualMemoryMB",
    )
}


class YarnCollector:
    def __init__(self, master_address: str, logger: Any) -> None:
        self.master_address = master_address
        self.logger = logger

    def collect(self) -> Generator[Dict[str, Any], None, None]:
        collected_metrics: Dict[str, Dict[str, Any]] = {}
        self._cluster_metrics(collected_metrics)
        self._nodes_metrics(collected_metrics)
        yield from collected_metrics.values()

    def _cluster_metrics(self, collected_metrics: Dict[str, Dict[str, Any]]) -> None:
        try:
            metrics_json = rest_request_to_json(self.master_address, YARN_CLUSTER_PATH)
            if metrics_json.get("clusterMetrics") is not None:
                set_metrics_from_json(collected_metrics, {}, metrics_json["clusterMetrics"], YARN_CLUSTER_METRICS)
        except Exception:
            self.logger.exception("Could not gather yarn cluster metrics")

    def _nodes_metrics(self, collected_metrics: Dict[str, Dict[str, Any]]) -> None:
        try:
            metrics_json = rest_request_to_json(self.master_address, YARN_NODES_PATH, states="RUNNING")
            running_nodes = metrics_json.get("nodes", {}).get("node", {})
            for node in running_nodes:
                for metric, value in node.get("resourceUtilization", {}).items():
                    node[metric] = value  # this will create all relevant metrics under same dictionary

                labels = {"node_hostname": node["nodeHostName"]}
                set_metrics_from_json(collected_metrics, labels, node, YARN_NODES_METRICS)
        except Exception:
            self.logger.exception("Could not gather yarn nodes metrics")

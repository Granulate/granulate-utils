#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
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

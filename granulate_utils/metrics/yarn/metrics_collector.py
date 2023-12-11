#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
# (C) Datadog, Inc. 2018-present. All rights reserved.
# Licensed under a 3-clause BSD style license (see LICENSE.bsd3).
#
import logging
from functools import cached_property
from typing import Iterable

from granulate_utils.metrics import Collector, Sample, samples_from_json
from granulate_utils.metrics.metrics import YARN_CLUSTER_METRICS, YARN_NODES_METRICS
from granulate_utils.metrics.yarn.resource_manager import ResourceManagerAPI


class YarnCollector(Collector):
    name = "yarn"

    def __init__(self, rm_address: str, kerberos_enabled: bool, logger: logging.LoggerAdapter) -> None:
        self.rm_address = rm_address
        self.rm = ResourceManagerAPI(self.rm_address)
        self.kerberos_enabled = kerberos_enabled
        self.logger = logger

    def collect(self) -> Iterable[Sample]:
        try:
            yield from self._cluster_metrics()
            yield from self._nodes_metrics()
        except Exception:
            self.logger.exception("Could not gather yarn metrics")

    def _cluster_metrics(self) -> Iterable[Sample]:
        try:
            if cluster_metrics := self.rm.metrics(kerberos_enabled=self.kerberos_enabled):
                yield from samples_from_json({}, cluster_metrics, YARN_CLUSTER_METRICS)
        except Exception:
            self.logger.exception("Could not gather yarn cluster metrics")

    def _nodes_metrics(self) -> Iterable[Sample]:
        try:
            for node in self.rm.nodes(kerberos_enabled=self.kerberos_enabled, states=self._active_node_states):
                for metric, value in node.get("resourceUtilization", {}).items():
                    node[metric] = value  # this will create all relevant metrics under same dictionary

                labels = {"node_hostname": node["nodeHostName"], "node_state": node["state"]}
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

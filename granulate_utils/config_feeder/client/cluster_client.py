import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from granulate_utils.config_feeder.client.http_client import HttpClient
from granulate_utils.config_feeder.core.models.aggregation import NodeResourceConfigCreate
from granulate_utils.config_feeder.core.models.cluster import ClusterCreate, CreateClusterRequest, CreateClusterResponse
from granulate_utils.config_feeder.core.models.collection import CollectionResult, CollectorType
from granulate_utils.config_feeder.core.models.node import CreateNodeRequest, CreateNodeResponse, NodeCreate, NodeInfo


@dataclass
class ClusterClient:
    _logger: Union[logging.Logger, logging.LoggerAdapter]
    _http_client: HttpClient
    _collector_type: CollectorType
    _service: str
    _cluster_id: Optional[str] = field(init=False, default=None)

    def register_cluster_if_needed(self, node_info: NodeInfo, collection_result: Dict[str, CollectionResult]) -> None:
        if self._cluster_id is None and (node_info.is_master or collection_result):
            self._register_cluster(node_info)

    def submit_node_configs(
        self,
        node_info: NodeInfo,
        config_requests: Dict[str, NodeResourceConfigCreate],
    ) -> Dict[str, Any]:
        node_id = self._register_node(node_info)
        self._logger.info(f"sending configs for node {node_info.external_id}")
        request = {}
        for k, v in config_requests.items():
            request[k] = v.dict()
        return self._http_client.request("POST", f"/nodes/{node_id}/configs", request)

    def _register_cluster(self, node_info: NodeInfo) -> None:
        self._logger.debug(f"registering cluster {node_info.external_id}")
        request = CreateClusterRequest(
            cluster=ClusterCreate(
                collector_type=self._collector_type,
                service=self._service,
                provider=node_info.provider,
                bigdata_platform=node_info.bigdata_platform,
                bigdata_platform_version=node_info.bigdata_platform_version,
                hadoop_version=node_info.hadoop_version,
                external_id=node_info.external_cluster_id,
                properties=json.dumps(node_info.properties) if node_info.properties else None,
            ),
            allow_existing=True,
        )
        response = CreateClusterResponse(**self._http_client.request("POST", "/clusters", request))
        self._cluster_id = response.cluster.id

    def _register_node(
        self,
        node: NodeInfo,
    ) -> str:
        assert self._cluster_id is not None
        self._logger.debug(f"registering node {node.external_id}")
        request = CreateNodeRequest(
            node=NodeCreate(
                collector_type=self._collector_type,
                external_id=node.external_id,
                is_master=node.is_master,
            ),
            allow_existing=True,
        )
        response = CreateNodeResponse(
            **self._http_client.request("POST", f"/clusters/{self._cluster_id}/nodes", request)
        )
        return response.node.id

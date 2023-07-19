import asyncio
import json
from collections import defaultdict
from typing import DefaultDict, Dict, Optional

from granulate_utils.config_feeder.client.collector import ConfigFeederCollector, ConfigFeederCollectorParams
from granulate_utils.config_feeder.client.models import CollectionResult, ConfigType
from granulate_utils.config_feeder.client.yarn.collector import YarnConfigCollector
from granulate_utils.config_feeder.client.yarn.models import YarnConfig
from granulate_utils.config_feeder.core.models.aggregation import CreateNodeConfigsRequest, CreateNodeConfigsResponse
from granulate_utils.config_feeder.core.models.cluster import ClusterCreate, CreateClusterRequest, CreateClusterResponse
from granulate_utils.config_feeder.core.models.node import CreateNodeRequest, CreateNodeResponse, NodeCreate, NodeInfo
from granulate_utils.config_feeder.core.models.yarn import NodeYarnConfigCreate


class YarnConfigFeederCollector(ConfigFeederCollector):
    def __init__(
        self,
        params: ConfigFeederCollectorParams,
        *,
        yarn: bool = True,
    ) -> None:
        super().__init__(params)
        self._cluster_id: Optional[str] = None
        self._is_yarn_enabled = yarn
        self._yarn_collector = YarnConfigCollector(logger=self.logger)
        self._last_hash: DefaultDict[ConfigType, Dict[str, str]] = defaultdict(dict)

    async def collect(self, node_info: NodeInfo) -> None:
        collection_result = await self._collect(node_info)

        if self._cluster_id is None and (node_info.is_master or not collection_result.is_empty):
            self._register_cluster(node_info)

        if collection_result.is_empty:
            self.logger.info("no configs to submit")
            return None

        self._submit_node_configs(collection_result)

    async def _collect(self, node_info: NodeInfo) -> CollectionResult:
        results = await asyncio.gather(
            self._collect_yarn_config(node_info),
        )
        return CollectionResult(node=node_info, yarn_config=results[0])

    async def _collect_yarn_config(self, node_info: NodeInfo) -> Optional[YarnConfig]:
        if not self._is_yarn_enabled:
            return None
        self.logger.info("YARN config collection starting")
        yarn_config = await self._yarn_collector.collect(node_info)
        self.logger.info("YARN config collection finished")
        return yarn_config

    def _submit_node_configs(
        self,
        collection_result: CollectionResult,
    ) -> None:
        external_id = collection_result.node.external_id
        request = self._get_configs_request(collection_result)
        if request is None:
            self.logger.info(f"skipping node {external_id}, configs are up to date")
            return None

        node_id = self._register_node(collection_result.node)
        self.logger.info(f"sending configs for node {external_id}")
        response = CreateNodeConfigsResponse(**self._http_client.request("POST", f"/nodes/{node_id}/configs", request))

        if response.yarn_config is not None:
            assert request.yarn_config is not None
            self._last_hash[ConfigType.YARN][external_id] = collection_result.yarn_config_hash

    def _register_node(
        self,
        node: NodeInfo,
    ) -> str:
        assert self._cluster_id is not None
        self.logger.debug(f"registering node {node.external_id}")
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

    def _register_cluster(self, node_info: NodeInfo) -> None:
        self.logger.debug(f"registering cluster {node_info.external_id}")
        request = CreateClusterRequest(
            cluster=ClusterCreate(
                collector_type=self._collector_type,
                service=self._service,
                provider=node_info.provider,
                bigdata_platform=node_info.bigdata_platform,
                external_id=node_info.external_cluster_id,
                properties=json.dumps(node_info.properties) if node_info.properties else None,
            ),
            allow_existing=True,
        )
        response = CreateClusterResponse.parse_obj(self._http_client.request("POST", "/clusters", request))
        self._cluster_id = response.cluster.id

    def _get_configs_request(self, configs: CollectionResult) -> Optional[CreateNodeConfigsRequest]:
        yarn_config = self._get_yarn_config_if_changed(configs)

        if yarn_config is None:
            return None

        return CreateNodeConfigsRequest(
            yarn_config=yarn_config,
        )

    def _get_yarn_config_if_changed(self, configs: CollectionResult) -> Optional[NodeYarnConfigCreate]:
        if configs.yarn_config is None:
            return None
        if self._last_hash[ConfigType.YARN].get(configs.node.external_id) == configs.yarn_config_hash:
            self.logger.debug("YARN config is up to date")
            return None
        return NodeYarnConfigCreate(
            collector_type=self._collector_type, config_json=json.dumps(configs.yarn_config.config)
        )

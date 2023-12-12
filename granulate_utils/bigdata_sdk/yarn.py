import logging
from dataclasses import dataclass
from typing import Dict, Optional, Union

from granulate_utils.metrics.yarn import get_yarn_node_info
from granulate_utils.metrics.yarn.node_manager import NodeManagerAPI
from granulate_utils.metrics.yarn.resource_manager import ResourceManagerAPI
from granulate_utils.metrics.yarn.utils import YarnNodeInfo


class YarnNotFound(Exception):
    pass


@dataclass
class YarnConfig:
    rm_address: Optional[str] = None
    nm_address: Optional[str] = None
    rm: Optional[ResourceManagerAPI] = None
    nm: Optional[NodeManagerAPI] = None
    use_first_rm: bool = True
    use_https: bool = False


class Yarn:
    def __init__(
        self,
        *,
        logger: Union[logging.Logger, logging.LoggerAdapter],
        yarn_config: Optional[YarnConfig] = None,
    ):
        self.logger = logger
        if yarn_config is None:
            yarn_config = YarnConfig()
        self._rm_address = yarn_config.rm_address
        self._nm_address = yarn_config.nm_address
        self._rm = yarn_config.rm
        self._nm = yarn_config.nm
        self._use_first_rm = yarn_config.use_first_rm
        self._use_https = yarn_config.use_https

    @property
    def rm(self) -> ResourceManagerAPI:
        if self._rm is not None:
            return self._rm
        self._rm_address = self._rm_address or self._detect_resource_manager_address()
        if self._rm_address is None:
            raise YarnNotFound("could not resolve ResourceManager address")
        self._rm = ResourceManagerAPI(self._rm_address)
        return self._rm

    @property
    def nm(self) -> NodeManagerAPI:
        if self._nm is not None:
            return self._nm
        self._nm_address = self._nm_address or self._detect_node_manager_address()
        if self._nm_address is None:
            raise YarnNotFound("could not resolve NodeManager address")
        self._nm = NodeManagerAPI(self._nm_address)
        return self._nm

    def get_yarn_node_info(self) -> Optional[YarnNodeInfo]:
        return get_yarn_node_info(logger=self.logger)

    def _detect_resource_manager_address(self) -> Optional[str]:
        if yarn_node_info := get_yarn_node_info(logger=self.logger):
            rm_address = (
                yarn_node_info.first_resource_manager_webapp_address
                if self._use_first_rm
                else yarn_node_info.get_own_resource_manager_webapp_address()
            )
            rm_address = self._add_scheme_if_missing(yarn_node_info.config, rm_address)
            self.logger.debug(f"found ResourceManager address: {rm_address}")
            return rm_address
        else:
            self.logger.error("could not resolve ResourceManager address")
        return None

    def _detect_node_manager_address(self) -> Optional[str]:
        if yarn_node_info := get_yarn_node_info(logger=self.logger):
            nm_address = self._add_scheme_if_missing(yarn_node_info.config, yarn_node_info.node_manager_webapp_address)
            self.logger.debug(f"found NodeManager address: {nm_address}")
            return nm_address
        else:
            self.logger.error("could not resolve NodeManager address")
        return None

    def _add_scheme_if_missing(self, config: Dict[str, str], address: str) -> str:
        if not address.startswith("http"):
            address = (
                f"https://{address}"
                if (self._use_https or config.get("yarn.http.policy", "HTTP_ONLY") == "HTTPS_ONLY")
                else f"http://{address}"
            )
        return address

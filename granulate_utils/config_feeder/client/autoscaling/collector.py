from typing import Optional

from granulate_utils.config_feeder.client.bigdata.databricks import get_databricks_autoscaling_config
from granulate_utils.config_feeder.client.bigdata.dataproc import get_dataproc_autoscaling_config
from granulate_utils.config_feeder.client.bigdata.emr import get_emr_autoscaling_config
from granulate_utils.config_feeder.client.collector import ConfigFeederCollector, ConfigFeederCollectorParams
from granulate_utils.config_feeder.core.models.autoscaling import AutoScalingConfig
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform
from granulate_utils.config_feeder.core.models.collection import CollectionResult
from granulate_utils.config_feeder.core.models.node import NodeInfo


class AutoScalingConfigCollector(ConfigFeederCollector):
    name = "autoscaling_config"

    def __init__(
        self,
        params: ConfigFeederCollectorParams,
    ) -> None:
        super().__init__(params)

    async def collect(self, node_info: NodeInfo) -> CollectionResult:
        if not node_info.is_master:
            self.logger.debug("not a master node, skipping")
            return CollectionResult(config=None)

        config: Optional[AutoScalingConfig] = None

        if node_info.bigdata_platform == BigDataPlatform.EMR:
            config = await get_emr_autoscaling_config(node_info, logger=self.logger)
        elif node_info.bigdata_platform == BigDataPlatform.DATAPROC:
            config = await get_dataproc_autoscaling_config(node_info, logger=self.logger)
        elif node_info.bigdata_platform == BigDataPlatform.DATABRICKS:
            config = await get_databricks_autoscaling_config(node_info)
        else:
            self.logger.debug(
                f"{node_info.bigdata_platform} on {node_info.provider} is not yet supported, skipping"  # noqa: E501
            )

        return CollectionResult(config=config.dict() if config is not None else None)

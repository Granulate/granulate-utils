import logging
from typing import Optional, Union

from granulate_utils.config_feeder.client.bigdata.databricks import get_databricks_autoscaling_config
from granulate_utils.config_feeder.client.bigdata.dataproc import get_dataproc_autoscaling_config
from granulate_utils.config_feeder.client.bigdata.emr import get_emr_autoscaling_config
from granulate_utils.config_feeder.core.models.autoscaling import AutoScalingConfig
from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform
from granulate_utils.config_feeder.core.models.node import NodeInfo


class AutoScalingConfigCollector:
    def __init__(self, *, logger: Union[logging.Logger, logging.LoggerAdapter]) -> None:
        self.logger = logger

    async def collect(self, node_info: NodeInfo) -> Optional[AutoScalingConfig]:
        if not node_info.is_master:
            self.logger.debug("not a master node, skipping")
            return None

        if node_info.bigdata_platform == BigDataPlatform.EMR:
            return await get_emr_autoscaling_config(node_info, logger=self.logger)

        if node_info.bigdata_platform == BigDataPlatform.DATAPROC:
            return await get_dataproc_autoscaling_config(node_info, logger=self.logger)

        if node_info.bigdata_platform == BigDataPlatform.DATABRICKS:
            return await get_databricks_autoscaling_config(node_info)

        self.logger.debug(
            f"{node_info.bigdata_platform} on {node_info.provider} is not yet supported, skipping"  # noqa: E501
        )
        return None

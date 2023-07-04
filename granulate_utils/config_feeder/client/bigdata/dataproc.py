import logging
from typing import Any, Dict, Optional, Union

import requests
from requests.exceptions import ConnectionError, JSONDecodeError

from granulate_utils.config_feeder.core.models.cluster import BigDataPlatform, CloudProvider
from granulate_utils.config_feeder.core.models.node import NodeInfo


def get_dataproc_node_info(logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None) -> Optional[NodeInfo]:
    """
    https://cloud.google.com/compute/docs/metadata/querying-metadata
    https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/metadata

    tested on Dataproc 2.0-debian10
    """
    try:
        metadata = _get_metadata()
        attributes = metadata["attributes"]
        properties = {
            "region": attributes["dataproc-region"],
            "cluster_name": attributes["dataproc-cluster-name"],
        }
        return NodeInfo(
            external_id=str(metadata["id"]),
            external_cluster_id=attributes["dataproc-cluster-uuid"],
            is_master=attributes["dataproc-role"] == "Master",
            provider=CloudProvider.GCP,
            bigdata_platform=BigDataPlatform.DATAPROC,
            properties=properties,
        )
    except JSONDecodeError:
        if logger:
            logger.error("got invalid dataproc metadata JSON")
    except KeyError as e:
        if logger:
            logger.error("expected dataproc metadata key was not found", extra={"key": e.args[0]})
    except ConnectionError:
        pass
    return None


def _get_metadata() -> Dict[str, Any]:
    url = "http://metadata.google.internal/computeMetadata/v1/instance/?recursive=true"  # noqa: E501
    headers = {"Metadata-Flavor": "Google"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

import json
import subprocess
from typing import Any, Dict, Optional

from granulate_utils.config_feeder.core.models.node import NodeInfo


def get_dataproc_node_info() -> Optional[NodeInfo]:
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
            provider="gcp",
            bigdata_platform="dataproc",
            properties=properties,
        )
    except (KeyError, json.JSONDecodeError):
        return None


def _get_metadata() -> Dict[str, Any]:
    text = subprocess.run(
        [
            "curl",
            '"http://metadata.google.internal/computeMetadata/v1/instance/?recursive=true"',  # noqa: E501
            "-H",
            '"Metadata-Flavor: Google"',
        ],
        capture_output=True,
        text=True,
    ).stdout.strip()
    result: Dict[str, Any] = json.loads(text)
    return result

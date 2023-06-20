from __future__ import annotations

import json
from typing import Any, Dict

from tests.granulate_utils.config_feeder.fixtures.base import NodeMockBase


class DataprocNodeMock(NodeMockBase):
    def __init__(
        self,
        *,
        cluster_uuid: str = "",
        cluster_name: str = "",
        instance_id: str = "",
        is_master: bool = False,
        region: str = "us-central1",
        cluster_info: Dict[str, Any] = {},
    ) -> None:
        super().__init__()

        metadata = {
            "id": instance_id,
            "attributes": {
                "dataproc-cluster-uuid": cluster_uuid,
                "dataproc-cluster-name": cluster_name,
                "dataproc-role": "Master" if is_master else "Worker",
                "dataproc-region": region,
            },
        }

        self.mock_command_stdout(
            'curl "http://metadata.google.internal/computeMetadata/v1/instance/?recursive=true" -H "Metadata-Flavor: Google"',  # noqa: E501
            json.dumps(metadata).encode("utf-8"),
        )

        self.mock_command_stdout(
            f"gcloud dataproc clusters describe {cluster_name} --region={region} --format=json",  # noqa: E501
            json.dumps(cluster_info).encode("utf-8"),
        )

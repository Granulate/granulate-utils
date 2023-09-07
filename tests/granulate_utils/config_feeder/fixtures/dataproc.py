from __future__ import annotations

import json
from textwrap import dedent
from typing import Any, Dict, Optional

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
        metadata_response: Optional[str] = None,
        version: str = "2.0",
    ) -> None:
        super().__init__()

        self.metadata = {
            "id": instance_id,
            "attributes": {
                "dataproc-cluster-uuid": cluster_uuid,
                "dataproc-cluster-name": cluster_name,
                "dataproc-role": "Master" if is_master else "Worker",
                "dataproc-region": region,
            },
        }

        url = "http://metadata.google.internal/computeMetadata/v1/instance/?recursive=true"
        self.mock_http_response(
            "GET", url, {"json": self.metadata} if metadata_response is None else {"text": metadata_response}
        )

        self.mock_command_stdout(
            f"gcloud dataproc clusters describe {cluster_name} --region={region} --format=json",  # noqa: E501
            json.dumps(cluster_info).encode("utf-8"),
        )

        self.mock_command_stdout(
            "hadoop version",
            dedent("""Hadoop 3.2.1
            Subversion https://bigdataoss-internal.googlesource.com/third_party/apache/hadoop -r b3e52921b6aaf2c68af220021eab42975114f7cb # noqa: E501
            Compiled by bigtop on 2021-07-12T21:30Z
            Compiled with protoc 2.5.0
            From source with checksum 3ea0fd8f2b9af855fb9f66c2e3130e3
            This command was run using /usr/lib/hadoop/hadoop-common-2.9.2.jar
            """).encode("utf-8"),
        )

        self.mock_file("/etc/environment", f"DATAPROC_IMAGE_VERSION={version}")

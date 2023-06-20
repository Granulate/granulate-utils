from __future__ import annotations

import json
from typing import Any, Dict

from tests.granulate_utils.config_feeder.fixtures.base import NodeMockBase


class EmrNodeMock(NodeMockBase):
    def __init__(
        self,
        *,
        job_flow_id: str = "",
        instance_id: str = "",
        is_master: bool = False,
        cluster_info: Dict[str, Any] = {},
        managed_policy: Dict[str, Any] = {},
    ) -> None:
        super().__init__()
        cluster_info = cluster_info

        self.mock_file("/mnt/var/lib/cloud/data/instance-id", instance_id)
        self.mock_file("/mnt/var/lib/info/instance.json", json.dumps({"isMaster": is_master}))
        self.mock_file("/mnt/var/lib/info/job-flow.json", json.dumps({"jobFlowId": job_flow_id}))

        self.mock_command_stdout(
            f"aws emr describe-cluster --cluster-id {job_flow_id}",
            json.dumps(cluster_info).encode("utf-8"),
        )

        self.mock_command_stdout(
            f"aws emr get-managed-scaling-policy --cluster-id {job_flow_id}",
            json.dumps(managed_policy).encode("utf-8"),
        )

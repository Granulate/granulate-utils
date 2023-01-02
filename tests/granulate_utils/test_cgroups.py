from pathlib import Path
from typing import Callable
from unittest.mock import patch

from granulate_utils.linux.cgroups.cpu_controller import CpuController


@patch("granulate_utils.linux.cgroups.cpu_controller.BaseController._verify_preconditions", return_value=None)
def test_get_cpu_limit_cores(base_group_mock: Callable) -> None:
    cpucgroup = CpuController(Path("test"))
    with patch.object(
        cpucgroup,
        "read_from_control_file",
        {
            CpuController.cfs_period_us: "100",
            CpuController.cfs_quota_us: "50",
        }.__getitem__,
    ):
        assert cpucgroup.get_cpu_limit_cores() == 0.5

    with patch.object(
        cpucgroup,
        "read_from_control_file",
        {
            CpuController.cfs_period_us: "100",
            CpuController.cfs_quota_us: "-1",
        }.__getitem__,
    ):
        assert cpucgroup.get_cpu_limit_cores() == -1.0

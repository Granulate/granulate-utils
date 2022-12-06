from typing import Callable
from unittest.mock import patch

from granulate_utils.linux.cgroups.cpu_cgroup import CpuCgroup


@patch("granulate_utils.linux.cgroups.cpu_cgroup.BaseCgroup._verify_preconditions", return_value=None)
def test_get_cpu_limit_cores(base_group_mock: Callable) -> None:
    cpucgroup = CpuCgroup()
    with patch.object(
        cpucgroup,
        "read_from_control_file",
        {
            CpuCgroup.cfs_period_us: "100",
            CpuCgroup.cfs_quota_us: "50",
        }.__getitem__,
    ):
        assert cpucgroup.get_cpu_limit_cores() == 0.5

    with patch.object(
        cpucgroup,
        "read_from_control_file",
        {
            CpuCgroup.cfs_period_us: "100",
            CpuCgroup.cfs_quota_us: "-1",
        }.__getitem__,
    ):
        assert cpucgroup.get_cpu_limit_cores() == -1.0

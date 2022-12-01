from typing import Callable, Dict
from unittest.mock import patch

from granulate_utils.linux.cgroups.cpu_cgroup import CpuCgroup


def wrap_read_from_control_file(mapping: Dict[str, str]) -> Callable:
    def func(tunable_name: str) -> str:
        return mapping[tunable_name]

    return func


@patch("granulate_utils.linux.cgroups.cpu_cgroup.BaseCgroup.__init__", return_value=None)
def test_get_cpu_limit_cores(base_group_mock: Callable) -> None:
    cpucgroup = CpuCgroup()
    with patch.object(
        cpucgroup,
        "read_from_control_file",
        wrap_read_from_control_file(
            {
                CpuCgroup.cfs_period_us: "100",
                CpuCgroup.cfs_quota_us: "50",
            }
        ),
    ):
        assert cpucgroup.get_cpu_limit_cores() == 0.5

    with patch.object(
        cpucgroup,
        "read_from_control_file",
        wrap_read_from_control_file(
            {
                CpuCgroup.cfs_period_us: "100",
                CpuCgroup.cfs_quota_us: "-1",
            }
        ),
    ):
        assert cpucgroup.get_cpu_limit_cores() == -1.0

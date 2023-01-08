from pathlib import Path
from typing import Callable
from unittest.mock import patch

from pytest import TempPathFactory

from granulate_utils.linux.cgroups.cpu_controller import CpuController
from granulate_utils.linux.cgroups.cpuacct_controller import CpuAcctController
from granulate_utils.linux.cgroups.memory_controller import MemoryController


# BaseController
@patch("granulate_utils.linux.cgroups.cpu_controller.BaseController._verify_preconditions", return_value=None)
def test_base_controller(base_group_mock: Callable, tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("base_controller")
    cpu_controller_dir = Path(tmp_dir)
    cpu_procs = cpu_controller_dir / "cgroup.procs"

    cpu_procs.write_text("1 2 3")

    cpu_controller = CpuController(cpu_controller_dir)
    assert cpu_controller.get_pids_in_cgroup() == set([1, 2, 3])
    cpu_controller.assign_to_cgroup(4)
    assert cpu_controller.get_pids_in_cgroup() == set([4])  # write_text overwrites by default.

    sub_cgroup_dir = cpu_controller_dir / "sub_cgroup"
    assert not sub_cgroup_dir.exists()
    sub_cgroup_controller = CpuController.create_subcgroup("sub_cgroup", cpu_controller.controller_path)
    assert sub_cgroup_dir.exists()
    assert sub_cgroup_controller is not None

    sub_cgroup_procs = sub_cgroup_dir / "cgroup.procs"
    sub_cgroup_controller.assign_to_cgroup(5)
    assert sub_cgroup_controller.get_pids_in_cgroup() == set([5])
    assert sub_cgroup_procs.read_text() == "5"


# CpuController
@patch("granulate_utils.linux.cgroups.cpu_controller.BaseController._verify_preconditions", return_value=None)
def test_cpu_controller(base_group_mock: Callable, tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("cpu_controller")
    cpu_controller_dir = Path(tmp_dir)
    cpu_period = cpu_controller_dir / "cpu.cfs_period_us"
    cpu_quota = cpu_controller_dir / "cpu.cfs_quota_us"
    cpu_stat = cpu_controller_dir / "cpu.stat"

    cpu_period.write_text("100")
    cpu_quota.write_text("50")
    cpu_stat.write_text("stat_value 1")

    cpu_controller = CpuController(cpu_controller_dir)
    assert cpu_controller.get_cpu_limit_cores() == 0.5
    stat_data = cpu_controller.get_stat()
    assert len(stat_data) == 1
    assert stat_data["stat_value"] == 1

    cpu_controller.set_cpu_limit_cores(3.5)
    assert cpu_controller.get_cpu_limit_cores() == 3.5
    assert int(cpu_quota.read_text()) == 350

    cpu_controller.reset_cpu_limit()
    assert cpu_controller.get_cpu_limit_cores() == -1
    assert int(cpu_quota.read_text()) == -1


@patch("granulate_utils.linux.cgroups.cpu_controller.BaseController._verify_preconditions", return_value=None)
def test_get_cpu_limit_cores(base_group_mock: Callable) -> None:
    cpu_controller = CpuController(Path("test"))
    with patch.object(
        cpu_controller,
        "read_from_control_file",
        {
            CpuController.cfs_period_us: "100",
            CpuController.cfs_quota_us: "50",
        }.__getitem__,
    ):
        assert cpu_controller.get_cpu_limit_cores() == 0.5

    with patch.object(
        cpu_controller,
        "read_from_control_file",
        {
            CpuController.cfs_period_us: "100",
            CpuController.cfs_quota_us: "-1",
        }.__getitem__,
    ):
        assert cpu_controller.get_cpu_limit_cores() == -1.0


# MemoryController
@patch("granulate_utils.linux.cgroups.memory_controller.BaseController._verify_preconditions", return_value=None)
def test_memory_controller(base_group_mock: Callable, tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("memory_controller")
    memory_controller_dir = Path(tmp_dir)

    bytes_limit = memory_controller_dir / "memory.limit_in_bytes"
    swap_bytes_limit = memory_controller_dir / "memory.memsw.limit_in_bytes"
    max_bytes_usage = memory_controller_dir / "memory.max_usage_in_bytes"
    usage_in_bytes = memory_controller_dir / "memory.usage_in_bytes"

    bytes_limit.write_text("128")
    swap_bytes_limit.write_text("100")
    max_bytes_usage.write_text("400")
    usage_in_bytes.write_text("500")

    memory_controller = MemoryController(memory_controller_dir)
    assert memory_controller.get_memory_limit() == 128
    assert memory_controller.get_max_usage_in_bytes() == 400
    assert memory_controller.get_usage_in_bytes() == 500

    memory_controller.set_limit_in_bytes(50)
    assert memory_controller.get_memory_limit() == 50
    assert int(bytes_limit.read_text()) == 50
    assert int(swap_bytes_limit.read_text()) == 50

    memory_controller.reset_memory_limit()
    assert memory_controller.get_memory_limit() == -1
    assert int(bytes_limit.read_text()) == -1
    assert int(swap_bytes_limit.read_text()) == -1


# CpuAcctController
@patch("granulate_utils.linux.cgroups.cpuacct_controller.BaseController._verify_preconditions", return_value=None)
def test_cpuacct_controller(base_group_mock: Callable, tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("cpuacct_controller")
    cpuacct_controller_dir = Path(tmp_dir)

    cpuacct_usage = cpuacct_controller_dir / "cpuacct.usage"
    cpuacct_usage.write_text("128")
    cpuacct_controller = CpuAcctController(cpuacct_controller_dir)
    assert cpuacct_controller.get_cpu_time_ns() == 128

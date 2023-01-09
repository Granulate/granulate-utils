#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from pathlib import Path
from unittest.mock import patch

import pytest
from pytest import TempPathFactory

from granulate_utils.linux.cgroups.cgroup import CgroupCoreV1, CgroupCoreV2, get_current_process_cgroup
from granulate_utils.linux.cgroups.cpu_controller import CpuController
from granulate_utils.linux.cgroups.cpuacct_controller import CpuAcctController
from granulate_utils.linux.cgroups.memory_controller import MemoryController


# Cgroup
def test_cgroup_sanity(tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("base_controller")
    cgroup_dir = Path(tmp_dir)
    cpu_procs = cgroup_dir / "cgroup.procs"

    cpu_procs.write_text("1 2 3")

    cgroup_v1 = CgroupCoreV1(cgroup_dir)

    assert cgroup_v1.get_pids_in_cgroup() == set([1, 2, 3])
    cgroup_v1.assign_process_to_cgroup(4)
    assert cgroup_v1.get_pids_in_cgroup() == set([4])  # write_text overwrites by default.

    sub_cgroup_dir = cgroup_dir / "sub_cgroup"
    assert not sub_cgroup_dir.exists()
    sub_cgroup = cgroup_v1.create_subcgroup("dummy", "sub_cgroup")
    assert sub_cgroup_dir.exists()
    assert sub_cgroup is not None
    assert sub_cgroup.has_parent_cgroup(cgroup_v1.path.name)

    sub_cgroup_procs = sub_cgroup_dir / "cgroup.procs"
    sub_cgroup.assign_process_to_cgroup(5)
    assert sub_cgroup.get_pids_in_cgroup() == set([5])
    assert sub_cgroup_procs.read_text() == "5"

    same_cgroup = sub_cgroup.create_subcgroup("dummy", sub_cgroup.path.name)
    assert same_cgroup.path == sub_cgroup.path

    parent_cgroup = sub_cgroup.create_subcgroup("dummy", cgroup_v1.path.name)
    assert parent_cgroup.path == cgroup_v1.path


def test_cgroup_v2(tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("base_controller")
    cgroup_dir = Path(tmp_dir)
    SUB_CGROUP_NAME = "sub_cgroup"
    sub_cgroup_dir = cgroup_dir / SUB_CGROUP_NAME
    sub_cgroup_dir.mkdir()
    supported_controllers = sub_cgroup_dir / "cgroup.controllers"
    enabled_controllers = sub_cgroup_dir / "cgroup.subtree_control"

    CONTROLLER_TYPE = "dummy"
    supported_controllers.write_text(CONTROLLER_TYPE)
    cgroup = CgroupCoreV2(cgroup_dir)
    sub_cgroup = cgroup.create_subcgroup(CONTROLLER_TYPE, SUB_CGROUP_NAME)
    assert sub_cgroup is not None
    assert enabled_controllers.read_text().strip() == f"+{CONTROLLER_TYPE}"

    with pytest.raises(AssertionError) as exception:
        sub_cgroup = cgroup.create_subcgroup("dummy2", SUB_CGROUP_NAME)
    assert exception.value.args[0] == "Controller not supported"


def test_get_cgroup_current_process():
    root_path = Path("root_path/")
    relative_path = Path("/dummy")
    CONTROLLER_TYPE = "dummy"

    with patch("granulate_utils.linux.cgroups.cgroup.get_cgroup_mount", return_value=CgroupCoreV1(root_path)):
        with patch("granulate_utils.linux.cgroups.cgroup.read_proc_file", return_value=b"1:dummy:/dummy\n"):
            cgroup = get_current_process_cgroup(CONTROLLER_TYPE)
            assert cgroup.path == (root_path / relative_path)

    with patch("granulate_utils.linux.cgroups.cgroup.get_cgroup_mount", return_value=CgroupCoreV2(root_path)):
        with patch("granulate_utils.linux.cgroups.cgroup.read_proc_file", return_value=b"1:dummy:/fail\n0::/dummy\n"):
            cgroup = get_current_process_cgroup(CONTROLLER_TYPE)
            assert cgroup.path == (root_path / relative_path)

    with pytest.raises(Exception) as exception:
        with patch("granulate_utils.linux.cgroups.cgroup.get_cgroup_mount", return_value=CgroupCoreV2(root_path)):
            with patch("granulate_utils.linux.cgroups.cgroup.read_proc_file", return_value=b"1:dummy:/fail\n"):
                cgroup = get_current_process_cgroup(CONTROLLER_TYPE)
    assert exception.value.args[0] == f"'{CONTROLLER_TYPE}' not found"


# CpuController
def test_cpu_controller_v1(tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("cpu_controller")
    cpu_controller_dir = Path(tmp_dir)
    cpu_period = cpu_controller_dir / "cpu.cfs_period_us"
    cpu_quota = cpu_controller_dir / "cpu.cfs_quota_us"
    cpu_stat = cpu_controller_dir / "cpu.stat"

    cpu_period.write_text("100")
    cpu_quota.write_text("50")
    cpu_stat.write_text("stat_value 1")

    cgroup_v1 = CgroupCoreV1(cpu_controller_dir)
    cpu_controller = CpuController(cgroup_v1)
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


def test_cpu_controller_v2(tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("cpu_controller")
    cpu_controller_dir = Path(tmp_dir)
    cpu_max = cpu_controller_dir / "cpu.max"
    cpu_stat = cpu_controller_dir / "cpu.stat"

    cpu_max.write_text("50 100")
    cpu_stat.write_text("stat_value 1")

    cgroup_v2 = CgroupCoreV2(cpu_controller_dir)
    cpu_controller = CpuController(cgroup_v2)
    assert cpu_controller.get_cpu_limit_cores() == 0.5
    stat_data = cpu_controller.get_stat()
    assert len(stat_data) == 1
    assert stat_data["stat_value"] == 1

    cpu_controller.set_cpu_limit_cores(3.5)
    assert cpu_controller.get_cpu_limit_cores() == 3.5
    assert cpu_max.read_text() == "350 100"

    cpu_controller.reset_cpu_limit()
    assert cpu_controller.get_cpu_limit_cores() == -1
    assert cpu_max.read_text() == "max 100"


# MemoryController
def test_memory_controller_v1(tmp_path_factory: TempPathFactory):
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

    cgroup_v1 = CgroupCoreV1(memory_controller_dir)
    memory_controller = MemoryController(cgroup_v1)
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


def test_memory_controller_v2(tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("memory_controller")
    memory_controller_dir = Path(tmp_dir)

    bytes_limit = memory_controller_dir / "memory.max"
    swap_bytes_limit = memory_controller_dir / "memory.swap.max"
    usage_in_bytes = memory_controller_dir / "memory.current"

    bytes_limit.write_text("128")
    swap_bytes_limit.write_text("100")
    usage_in_bytes.write_text("500")

    cgroup_v2 = CgroupCoreV2(memory_controller_dir)
    memory_controller = MemoryController(cgroup_v2)
    assert memory_controller.get_memory_limit() == 128
    assert memory_controller.get_usage_in_bytes() == 500

    memory_controller.set_limit_in_bytes(50)
    assert memory_controller.get_memory_limit() == 50
    assert int(bytes_limit.read_text()) == 50
    assert int(swap_bytes_limit.read_text()) == 0

    memory_controller.reset_memory_limit()
    assert memory_controller.get_memory_limit() == -1
    assert bytes_limit.read_text() == "max"
    assert swap_bytes_limit.read_text() == "max"

    with pytest.raises(Exception) as exception:
        memory_controller.get_max_usage_in_bytes()
    assert exception.value.args[0] == "Not implemented"


# CpuAcctController
def test_cpuacct_controller(tmp_path_factory: TempPathFactory):
    tmp_dir = tmp_path_factory.mktemp("cpuacct_controller")
    cpuacct_controller_dir = Path(tmp_dir)

    cpuacct_usage = cpuacct_controller_dir / "cpuacct.usage"
    cpuacct_usage.write_text("128")

    cgroup_v1 = CgroupCoreV1(cpuacct_controller_dir)
    cpuacct_controller = CpuAcctController(cgroup_v1)
    assert cpuacct_controller.get_cpu_time_ns() == 128

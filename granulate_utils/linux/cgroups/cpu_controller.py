#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Union

from granulate_utils.linux.cgroups.base_controller import BaseController
from granulate_utils.linux.cgroups.cgroup import (
    CGROUP_V2_UNBOUNDED_VALUE,
    CgroupCore,
    CgroupCoreV1,
    CgroupCoreV2,
    ControllerType,
)


@dataclass
class CpuLimitParams:
    period: int
    quota: int


class CpuControllerInterface:
    CPU_STAT_FILE: str

    def __init__(self, cgroup: CgroupCore):
        self.cgroup = cgroup

    @abstractmethod
    def get_cpu_limit_period(self) -> int:
        pass

    @abstractmethod
    def get_cpu_limit_quota(self) -> int:
        pass

    @abstractmethod
    def get_cpu_limit_params(self) -> CpuLimitParams:
        pass

    @abstractmethod
    def set_cpu_limit_quota(self, quota: int) -> None:
        pass


class CpuControllerV1(CpuControllerInterface):
    CPU_STAT_FILE = "cpu.stat"
    CPU_PERIOD_FILE = "cpu.cfs_period_us"
    CPU_QUOTA_FILE = "cpu.cfs_quota_us"

    def get_cpu_limit_period(self) -> int:
        return int(self.cgroup.read_from_interface_file(self.CPU_PERIOD_FILE))

    def get_cpu_limit_quota(self) -> int:
        return int(self.cgroup.read_from_interface_file(self.CPU_QUOTA_FILE))

    def get_cpu_limit_params(self) -> CpuLimitParams:
        return CpuLimitParams(self.get_cpu_limit_period(), self.get_cpu_limit_quota())

    def set_cpu_limit_quota(self, quota: int) -> None:
        self.cgroup.write_to_interface_file(self.CPU_QUOTA_FILE, str(quota))


class CpuControllerV2(CpuControllerInterface):
    CPU_STAT_FILE = "cpu.stat"
    CPU_LIMIT_FILE = "cpu.max"

    def get_cpu_limit_period(self) -> int:
        return self.get_cpu_limit_params().period

    def get_cpu_limit_quota(self) -> int:
        return self.get_cpu_limit_params().quota

    def get_cpu_limit_params(self) -> CpuLimitParams:
        cpu_limit = self.cgroup.read_from_interface_file(self.CPU_LIMIT_FILE).split()
        # If quota is unbounded ('max') we return -1 to be API consistent with CgroupV1
        return CpuLimitParams(
            period=int(cpu_limit[1]), quota=int(cpu_limit[0]) if cpu_limit[0] != CGROUP_V2_UNBOUNDED_VALUE else -1
        )

    def set_cpu_limit_quota(self, quota: int) -> None:
        period = self.get_cpu_limit_period()
        quota_str = str(quota)
        if quota_str == "-1":
            quota_str = CGROUP_V2_UNBOUNDED_VALUE
        self.cgroup.write_to_interface_file(self.CPU_LIMIT_FILE, f"{quota_str} {period}")


class CpuController(BaseController):
    CONTROLLER: ControllerType = "cpu"

    def __init__(self, cgroup: Optional[Union[Path, CgroupCore]] = None):
        super().__init__(cgroup)
        if isinstance(self.cgroup, CgroupCoreV1):
            self.controller_interface: CpuControllerInterface = CpuControllerV1(self.cgroup)
        elif isinstance(self.cgroup, CgroupCoreV2):
            self.controller_interface = CpuControllerV2(self.cgroup)

    def set_cpu_limit_cores(self, cores: float) -> None:
        period = self.controller_interface.get_cpu_limit_period()
        self.controller_interface.set_cpu_limit_quota(int(period * cores))

    def get_cpu_limit_cores(self) -> float:
        """
        Returns the cores limit: quota / period. If quota is unbounded (-1) will return -1
        """
        cpu_limit_params = self.controller_interface.get_cpu_limit_params()
        # if quota is set to -1 it means this cgroup is unbounded
        return cpu_limit_params.quota / cpu_limit_params.period if cpu_limit_params.quota != -1 else -1.0

    def reset_cpu_limit(self) -> None:
        self.controller_interface.set_cpu_limit_quota(-1)

    def get_stat(self) -> Dict[str, int]:
        stat_text = self.read_from_interface_file(self.controller_interface.CPU_STAT_FILE)
        return {line.split()[0]: int(line.split()[1]) for line in stat_text.splitlines()}

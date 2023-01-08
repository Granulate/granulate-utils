#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Union

from granulate_utils.linux.cgroups.base_controller import BaseController
from granulate_utils.linux.cgroups.cgroup import CgroupCore, CgroupCoreV1, CgroupCoreV2


@dataclass
class CpuLimitParams:
    period: int
    quota: int


class CpuControllerInterface:
    cpu_stat: str

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
    def set_cpu_limit_quota(self, quota: str) -> None:
        pass


class CpuControllerV1(CpuControllerInterface):
    cpu_stat = "cpu.stat"
    period = "cpu.cfs_period_us"
    quota = "cpu.cfs_quota_us"

    def get_cpu_limit_period(self) -> int:
        return int(self.cgroup.read_from_interface_file(self.period))

    def get_cpu_limit_quota(self) -> int:
        return int(self.cgroup.read_from_interface_file(self.quota))

    def get_cpu_limit_params(self) -> CpuLimitParams:
        return CpuLimitParams(self.get_cpu_limit_period(), self.get_cpu_limit_quota())

    def set_cpu_limit_quota(self, quota: str) -> None:
        self.cgroup.write_to_interface_file(self.quota, quota)


class CpuControllerV2(CpuControllerInterface):
    UNBOUNDED_QUOTA_VALUE = "max"
    cpu_stat = "cpu.stat"
    cpu_limit_file = "cpu.max"

    def get_cpu_limit_period(self) -> int:
        return self.get_cpu_limit_params().period

    def get_cpu_limit_quota(self) -> int:
        return self.get_cpu_limit_params().quota

    def get_cpu_limit_params(self) -> CpuLimitParams:
        cpu_limit = self.cgroup.read_from_interface_file(self.cpu_limit_file).split()
        return CpuLimitParams(
            period=int(cpu_limit[1]), quota=int(cpu_limit[0]) if cpu_limit[0] != self.UNBOUNDED_QUOTA_VALUE else -1
        )

    def set_cpu_limit_quota(self, quota: str) -> None:
        period = self.get_cpu_limit_period()
        if quota == "-1":
            quota = self.UNBOUNDED_QUOTA_VALUE
        self.cgroup.write_to_interface_file(self.cpu_limit_file, f"{quota} {period}")


class CpuController(BaseController):
    controller = "cpu"

    def __init__(self, cgroup: Optional[Union[Path, CgroupCore]] = None):
        super().__init__(cgroup)
        if isinstance(self.cgroup, CgroupCoreV1):
            self.controller_interface: CpuControllerInterface = CpuControllerV1(self.cgroup)
        elif isinstance(self.cgroup, CgroupCoreV2):
            self.controller_interface = CpuControllerV2(self.cgroup)

    def set_cpu_limit_cores(self, cores: float) -> None:
        period = self.controller_interface.get_cpu_limit_period()
        self.controller_interface.set_cpu_limit_quota(str(int(period * cores)))

    def get_cpu_limit_cores(self) -> float:
        cpu_limit_params = self.controller_interface.get_cpu_limit_params()
        # if quota is set to -1 it means this cgroup is unlimited
        return cpu_limit_params.quota / cpu_limit_params.period if cpu_limit_params.quota != -1 else -1.0

    def reset_cpu_limit(self) -> None:
        self.controller_interface.set_cpu_limit_quota("-1")

    def get_stat(self) -> Dict[str, int]:
        stat_text = self.read_from_interface_file(self.controller_interface.cpu_stat)
        return {line.split()[0]: int(line.split()[1]) for line in stat_text.splitlines()}

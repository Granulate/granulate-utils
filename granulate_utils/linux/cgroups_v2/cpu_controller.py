#
# Copyright (C) 2023 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Union

from psutil import Process

from granulate_utils.linux.cgroups_v2.base_controller import BaseController
from granulate_utils.linux.cgroups_v2.cgroup import CgroupCore, ControllerType


@dataclass
class CpuLimitParams:
    period: int
    quota: int


class CpuController(BaseController):
    CONTROLLER: ControllerType = "cpu"
    CPU_STAT_FILE: str

    def __init__(self, cgroup: Optional[Union[Path, CgroupCore]] = None):
        super().__init__(cgroup)

    def set_cpu_limit_cores(self, cores: float) -> None:
        period = self.get_cpu_limit_period()
        self.set_cpu_limit(quota=int(period * cores))

    def get_cpu_limit_cores(self) -> float:
        """
        Returns the cores limit: quota / period. If quota is unbounded (-1) will return -1
        """
        cpu_limit_params = self.get_cpu_limit_params()
        # if quota is set to -1 it means this cgroup is unbounded
        return cpu_limit_params.quota / cpu_limit_params.period if cpu_limit_params.quota != -1 else -1.0

    def reset_cpu_limit(self) -> None:
        self.set_cpu_limit(quota=-1)

    def get_stat(self) -> Dict[str, int]:
        stat_text = self.read_from_interface_file(self.CPU_STAT_FILE)
        return {line.split()[0]: int(line.split()[1]) for line in stat_text.splitlines()}

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
    def set_cpu_limit(self, quota: Optional[int] = None, period: Optional[int] = None) -> None:
        pass


class CpuControllerV1(CpuController):
    CPU_STAT_FILE = "cpu.stat"
    CPU_PERIOD_FILE = "cpu.cfs_period_us"
    CPU_QUOTA_FILE = "cpu.cfs_quota_us"

    def get_cpu_limit_period(self) -> int:
        return int(self.cgroup.read_from_interface_file(self.CPU_PERIOD_FILE))

    def get_cpu_limit_quota(self) -> int:
        return int(self.cgroup.read_from_interface_file(self.CPU_QUOTA_FILE))

    def get_cpu_limit_params(self) -> CpuLimitParams:
        return CpuLimitParams(self.get_cpu_limit_period(), self.get_cpu_limit_quota())

    def set_cpu_limit(self, quota: Optional[int] = None, period: Optional[int] = None) -> None:
        if quota is not None:
            self.cgroup.write_to_interface_file(self.CPU_QUOTA_FILE, str(quota))
        if period is not None:
            self.cgroup.write_to_interface_file(self.CPU_PERIOD_FILE, str(period))


class CpuControllerV2(CpuController):
    CPU_STAT_FILE = "cpu.stat"
    CPU_LIMIT_FILE = "cpu.max"

    def get_cpu_limit_period(self) -> int:
        return self.get_cpu_limit_params().period

    def get_cpu_limit_quota(self) -> int:
        return self.get_cpu_limit_params().quota

    def get_cpu_limit_params(self) -> CpuLimitParams:
        cpu_limit = self.cgroup.read_from_interface_file(self.CPU_LIMIT_FILE).split()
        # If quota is unbounded ('max') we return -1 to be API consistent with CgroupV1
        return CpuLimitParams(period=int(cpu_limit[1]), quota=self.cgroup.convert_inner_value_to_outer(cpu_limit[0]))

    def set_cpu_limit(self, quota: Optional[int] = None, period: Optional[int] = None) -> None:
        cpu_limit_params = self.get_cpu_limit_params()
        if quota is None:
            quota = cpu_limit_params.quota
        if period is None:
            period = cpu_limit_params.period

        quota_str = self.cgroup.convert_outer_value_to_inner(quota)
        self.cgroup.write_to_interface_file(self.CPU_LIMIT_FILE, f"{quota_str} {period}")


class CpuControllerFactory:
    @staticmethod
    def get_cpu_controller(cgroup: Optional[Union[CgroupCore, Path, Process]] = None) -> CpuController:
        cgroup_core = CpuController.get_cgroup_core(cgroup)
        if cgroup_core.is_v1:
            return CpuControllerV1(cgroup_core)
        return CpuControllerV2(cgroup_core)

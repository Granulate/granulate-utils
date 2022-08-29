#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from granulate_utils.linux.cgroups.base_cgroup import BaseCgroup


class CpuCgroup(BaseCgroup):
    controller = "cpu"

    def set_cpu_limit_cores(self, cores: float) -> None:
        period = int(self.read_from_controller("cpu.cfs_period_us"))
        self.write_to_controller("cpu.cfs_quota_us", str(period * cores))

    def get_cpu_limit_cores(self) -> float:
        period = int(self.read_from_controller("cpu.cfs_period_us"))
        return int(self.read_from_controller("cpu.cfs_quota_us")) / period

    def reset_cpu_limit(self) -> None:
        self.write_to_controller("cpu.cfs_quota_us", "-1")

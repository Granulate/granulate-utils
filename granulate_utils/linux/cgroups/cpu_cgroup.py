#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from granulate_utils.linux.cgroups.base_cgroup import BaseCgroup


class CpuCgroup(BaseCgroup):
    HIERARCHY = 'cpu'

    def set_cpu_limit_cores(self, cores: int) -> None:
        period = int(self.read_from_controller('cpu.cfs_period_us').split('\n')[0])
        self.write_to_controller('cpu.cfs_quota_us', str(period*cores))

    def get_cpu_limit_cores(self) -> float:
        period = float(self.read_from_controller('cpu.cfs_period_us').split('\n')[0])
        return float(self.read_from_controller('cpu.cfs_quota_us').split('\n')[0]) / period

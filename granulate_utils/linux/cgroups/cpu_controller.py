#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
from typing import Dict

from granulate_utils.linux.cgroups.base_controller import BaseController


class CpuController(BaseController):
    subsystem = "cpu"
    cfs_period_us = "cpu.cfs_period_us"
    cfs_quota_us = "cpu.cfs_quota_us"
    cpu_stat = "cpu.stat"

    def set_cpu_limit_cores(self, cores: float) -> None:
        period = int(self.read_from_control_file(self.cfs_period_us))
        self.write_to_control_file(self.cfs_quota_us, str(int(period * cores)))

    def get_cpu_limit_cores(self) -> float:
        period = int(self.read_from_control_file(self.cfs_period_us))
        quota = int(self.read_from_control_file(self.cfs_quota_us))
        # if quota is set to -1 it means this cgroup is unlimited
        return quota / period if quota != -1 else -1.0

    def reset_cpu_limit(self) -> None:
        self.write_to_control_file(self.cfs_quota_us, "-1")

    def get_stat(self) -> Dict[str, int]:
        stat_text = self.read_from_control_file(self.cpu_stat)
        return {line.split()[0]: int(line.split()[1]) for line in stat_text.splitlines()}

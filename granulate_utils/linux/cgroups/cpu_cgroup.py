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
from typing import Dict

from granulate_utils.linux.cgroups.base_cgroup import BaseCgroup


class CpuCgroup(BaseCgroup):
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

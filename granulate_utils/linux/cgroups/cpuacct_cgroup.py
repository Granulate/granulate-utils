#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from granulate_utils.linux.cgroups.base_cgroup import BaseCgroup


class CpuAcctCgroup(BaseCgroup):
    subsystem = "cpuacct"
    cpuacct_usage = "cpuacct.usage"

    def get_cpu_time_ns(self) -> int:
        return int(self.read_from_control_file(self.cpuacct_usage))

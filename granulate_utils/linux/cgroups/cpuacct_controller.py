#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from granulate_utils.linux.cgroups.base_controller import BaseController


class CpuAcctController(BaseController):
    controller = "cpuacct"
    cpuacct_usage = "cpuacct.usage"

    def get_cpu_time_ns(self) -> int:
        return int(self.read_from_interface_file(self.cpuacct_usage))

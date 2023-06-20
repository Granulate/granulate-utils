#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from granulate_utils.linux.cgroups_v2.base_controller import BaseController
from granulate_utils.linux.cgroups_v2.cgroup import ControllerType


class CpuAcctController(BaseController):
    CONTROLLER: ControllerType = "cpuacct"
    CPUACCT_USAGE_FILE = "cpuacct.usage"

    def get_cpu_time_ns(self) -> int:
        return int(self.read_from_interface_file(self.CPUACCT_USAGE_FILE))

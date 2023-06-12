#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from pathlib import Path
from typing import Optional, Union

from granulate_utils.linux.cgroups_v2.base_controller import BaseController
from granulate_utils.linux.cgroups_v2.cgroup import CgroupCore, ControllerType


class CpuAcctController(BaseController):
    CONTROLLER: ControllerType = "cpuacct"
    CPUACCT_USAGE_FILE = "cpuacct.usage"

    def get_cpu_time_ns(self) -> int:
        return int(self.read_from_interface_file(self.CPUACCT_USAGE_FILE))


class CpuAcctControllerFactory:
    @staticmethod
    def create_sub_cpu_controller(
        new_cgroup_name: str, parent_cgroup: Optional[Union[Path, CgroupCore]] = None
    ) -> CpuAcctController:
        current_cgroup = CpuAcctController.get_cgroup_core(parent_cgroup)
        subcgroup_core = current_cgroup.get_subcgroup(CpuAcctController.CONTROLLER, new_cgroup_name)
        return CpuAcctController(subcgroup_core)

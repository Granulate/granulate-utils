#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from pathlib import Path
from typing import Optional, Set

from granulate_utils.exceptions import UnsupportedCGroupV2
from granulate_utils.linux.cgroups.cgroup import CgroupUtils, get_cgroups, is_known_controller


class BaseController:
    cgroup_procs = "cgroup.procs"

    def __init__(self, controller_path: Optional[Path] = None) -> None:
        self._verify_preconditions()
        if controller_path is None:
            self.controller_path = CgroupUtils.get_current_cgroup_path(self.subsystem)
        else:
            self.controller_path = controller_path

    @property
    def subsystem(self) -> str:
        raise NotImplementedError

    def _verify_preconditions(self) -> None:
        assert is_known_controller(self.subsystem), f"{self.subsystem!r} is not supported"

        # "/proc/$PID/cgroup" lists a process's cgroup membership.  If legacy
        # cgroup is in use in the system, this file may contain multiple lines, one for each hierarchy.
        # The entry for cgroup v2 is always in the format "0::$PATH"::
        if len(get_cgroups()) == 1:
            raise UnsupportedCGroupV2()

    def assign_to_cgroup(self, pid: int = 0) -> None:
        """
        Assign process to this Cgroup
        :param pid: pid of the process to assign (0 is current)
        """
        if not self.controller_path.exists():
            raise FileNotFoundError("Cgroup doesn't exist")

        self.write_to_control_file(self.cgroup_procs, str(pid))

    def get_pids_in_subsystem(self) -> Set[int]:
        return {int(proc) for proc in self.read_from_control_file(self.cgroup_procs).split()}

    def read_from_control_file(self, file_name: str) -> str:
        controller_path = self.controller_path / file_name
        return controller_path.read_text()

    def write_to_control_file(self, file_name: str, data: str) -> None:
        controller_path = self.controller_path / file_name
        controller_path.write_text(data)

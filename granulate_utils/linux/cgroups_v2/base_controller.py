#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from __future__ import annotations

from pathlib import Path
from typing import Optional, Set, Union

from psutil import Process

from granulate_utils.linux.cgroups_v2.cgroup import CgroupCore, ControllerType, get_cgroup_core


class BaseController:
    CONTROLLER: ControllerType  # class attribute (should be initialized in inheriting classes)

    def __init__(self, cgroup: Optional[Union[Path, CgroupCore, Process]] = None) -> None:
        self.cgroup = self.get_cgroup_core(cgroup)
        assert self.cgroup is not None

    @classmethod
    def get_cgroup_core(cls, cgroup: Optional[Union[Path, CgroupCore, Process]] = None) -> CgroupCore:
        return get_cgroup_core(cls.CONTROLLER, cgroup)

    def assign_process_to_cgroup(self, pid: int = 0) -> None:
        """
        Assign process to this Cgroup
        :param pid: pid of the process to assign (0 is current)
        """
        self.cgroup.assign_process_to_cgroup(pid)

    def get_pids_in_cgroup(self) -> Set[int]:
        return self.cgroup.get_pids_in_cgroup()

    def read_from_interface_file(self, interface_name: str) -> str:
        return self.cgroup.read_from_interface_file(interface_name)

    def write_to_interface_file(self, interface_name: str, data: str) -> None:
        self.cgroup.write_to_interface_file(interface_name, data)

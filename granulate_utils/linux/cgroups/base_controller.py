#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from __future__ import annotations

from pathlib import Path
from typing import Optional, Set, Union

from granulate_utils.linux.cgroups.cgroup import (
    CgroupCore,
    ControllerType,
    create_cgroup_from_path,
    get_cgroup_for_process,
)


class BaseController:
    CONTROLLER: ControllerType  # class attribute (should be initialized in inheriting classes)

    def __init__(self, cgroup: Optional[Union[Path, CgroupCore]] = None) -> None:
        self.cgroup = self.get_cgroup_core(cgroup)
        assert self.cgroup is not None

    @classmethod
    def get_cgroup_core(cls, cgroup: Optional[Union[Path, CgroupCore]] = None) -> CgroupCore:
        if cgroup is None:
            # return current CgroupCore
            return get_cgroup_for_process(cls.CONTROLLER)
        elif isinstance(cgroup, Path):
            # create CgroupCore from path
            cgroup = create_cgroup_from_path(cls.CONTROLLER, cgroup)

        assert isinstance(cgroup, CgroupCore)
        return cgroup

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

    @classmethod
    def get_subcgroup(cls, cgroup_name: str, parent_cgroup: Optional[Union[Path, CgroupCore]] = None):
        current_cgroup = cls.get_cgroup_core(parent_cgroup)
        subcgroup_core = current_cgroup.get_subcgroup(cls.CONTROLLER, cgroup_name)
        return cls(subcgroup_core)

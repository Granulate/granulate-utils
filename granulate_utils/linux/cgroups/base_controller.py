#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from __future__ import annotations

from pathlib import Path
from typing import Optional, Set, Union

from granulate_utils.linux.cgroups.cgroup import (
    CgroupCore,
    create_cgroup_from_path,
    get_cgroup_for_process,
    is_known_controller,
)


class BaseController:
    controller: str  # class attribute to determine the controller type (should be initialized in inheriting classes)

    def __init__(self, cgroup: Optional[Union[Path, CgroupCore]] = None) -> None:
        assert is_known_controller(self.controller), f"{self.controller!r} is not supported"
        if cgroup is None:
            self.cgroup = get_cgroup_for_process(self.controller)
        else:
            if isinstance(cgroup, Path):
                self.cgroup = create_cgroup_from_path(self.controller, cgroup)
            else:
                self.cgroup = cgroup

        assert self.cgroup is not None

    def assign_process_to_cgroup(self, pid: int = 0) -> None:
        """
        Assign process to this Cgroup
        :param pid: pid of the process to assign (0 is current)
        """
        self.cgroup.assign_process_to_cgroup(pid)

    def get_pids_in_cgroup(self) -> Set[int]:
        return self.cgroup.get_pids_in_cgroup()

    def read_from_interface_file(self, file_name: str) -> str:
        return self.cgroup.read_from_interface_file(file_name)

    def write_to_interface_file(self, file_name: str, data: str) -> None:
        self.cgroup.write_to_interface_file(file_name, data)

    @classmethod
    def get_cgroup_in_hierarchy(cls, cgroup_name: str, parent_cgroup: Optional[Union[Path, CgroupCore]] = None):
        parent_controller = cls(parent_cgroup)
        new_cgroup = parent_controller.cgroup.get_cgroup_in_hierarchy(cls.controller, cgroup_name)
        return cls(new_cgroup)

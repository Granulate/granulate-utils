#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from pathlib import Path
from typing import Mapping, Optional, Set

from granulate_utils.exceptions import UnsupportedCGroupV2
from granulate_utils.linux.cgroups.cgroup import SUBSYSTEMS, find_v1_hierarchies, get_cgroups


class BaseCgroup:
    predefined_cgroups = ["kubepods", "docker", "ecs"]
    cgroup_procs = "cgroup.procs"
    _v1_hierarchies: Optional[Mapping[str, str]] = None

    def __init__(self) -> None:
        self._verify_preconditions()

    @staticmethod
    def get_cgroup_hierarchies() -> Mapping[str, str]:
        if BaseCgroup._v1_hierarchies is None:
            BaseCgroup._v1_hierarchies = find_v1_hierarchies()
        return BaseCgroup._v1_hierarchies

    def _verify_preconditions(self) -> None:
        assert self.subsystem in SUBSYSTEMS, f"{self.subsystem!r} is not supported"

        # "/proc/$PID/cgroup" lists a process's cgroup membership.  If legacy
        # cgroup is in use in the system, this file may contain multiple lines, one for each hierarchy.
        # The entry for cgroup v2 is always in the format "0::$PATH"::
        if len(get_cgroups()) == 1:
            raise UnsupportedCGroupV2()

    @property
    def subsystem(self) -> str:
        raise NotImplementedError

    def _get_cgroup(self) -> str:
        hierarchy_details = get_cgroups()
        for line in hierarchy_details:
            if self.subsystem in line[1]:
                return line[2]
        raise Exception(f"{self.subsystem!r} not found")

    @property
    def cgroup(self) -> str:
        return self._get_cgroup()

    @property
    def cgroup_mount_path(self) -> Path:
        return Path(self.cgroup_path / self.cgroup[1:])

    @property
    def cgroup_path(self) -> Path:
        return Path(self.get_cgroup_hierarchies()[self.subsystem])

    def get_pids_in_cgroup(self) -> Set[int]:
        return {int(proc) for proc in self.read_from_control_file(self.cgroup_procs).split()}

    def move_to_cgroup(self, custom_cgroup: str, pid: int = 0) -> Path:
        # move to a new cgroup inside the current cgroup
        # by setting pid=0 we move current process to the custom cgroup
        new_cgroup_path = Path(self.cgroup_mount_path / custom_cgroup)
        self.move_to_cgroup_abs_path(new_cgroup_path, pid)
        return new_cgroup_path

    @classmethod
    def move_to_cgroup_abs_path(cls, new_cgroup_path: Path, pid: int = 0) -> None:
        new_cgroup_path.mkdir(exist_ok=True)
        cls.write_to_control_file(new_cgroup_path, cls.cgroup_procs, str(pid))

    def read_from_control_file(self, file_name: str, subsystem_path: Optional[Path] = None) -> str:
        if subsystem_path is None:
            subsystem_path = self.cgroup_mount_path
        controller_path = subsystem_path / file_name
        return controller_path.read_text()

    @staticmethod
    def write_to_control_file(subsystem_path: Path, file_name: str, data: str) -> None:
        controller_path = subsystem_path / file_name
        controller_path.write_text(data)

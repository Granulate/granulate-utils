#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import os
from pathlib import Path
from typing import List, Tuple

from psutil import NoSuchProcess
from granulate_utils.exceptions import AlreadyInCgroup

CGROUPFS = Path("/sys/fs/cgroup")
SUBSYSTEMS = {"memory", "cpu"}




def get_cgroups(pid: int) -> List[Tuple[str, List[str], str]]:
    """
    Get the cgroups of a process in [(hier id., controllers, path)] parsed form.
    """

    def parse_line(line: str) -> Tuple[str, List[str], str]:
        hier_id, controller_list, cgroup_path = line.split(":", maxsplit=2)
        return hier_id, controller_list.split(","), cgroup_path

    try:
        text = Path(f"/proc/{pid}/cgroup").read_text()
    except FileNotFoundError:
        raise NoSuchProcess(pid)
    else:
        return [parse_line(line) for line in text.splitlines()]


class BaseCgroup:
    predefined_cgroups = ["kubepods", "docker", "ecs"]

    def __init__(self) -> None:
        self._verify_preconditions()

    def _verify_preconditions(self) -> None:
        assert self.subsystem in SUBSYSTEMS, f"{self.subsystem!r} is not supported"

        # "/proc/$PID/cgroup" lists a process's cgroup membership.  If legacy
        # cgroup is in use in the system, this file may contain multiple lines, one for each hierarchy.
        # The entry for cgroup v2 is always in the format "0::$PATH"::
        if len(get_cgroups(os.getpid())) == 1:
            raise Exception("cgroup V2 is unsupported")

    @property
    def subsystem(self) -> str:
        raise NotImplementedError

    def _get_cgroup(self) -> str:
        hierarchy_details = get_cgroups(os.getpid())
        for line in hierarchy_details:
            if self.subsystem in line[1]:
                return line[2]
        raise Exception(f"{self.subsystem!r} not found")

    @property
    def cgroup(self) -> str:
        return self._get_cgroup()

    @property
    def cgroup_mount_path(self) -> Path:
        return Path(CGROUPFS / self.subsystem / self.cgroup[1:])

    def move_to_cgroup(self, custom_cgroup: str, tid: int = 0) -> None:
        # move to a new cgroup inside the current cgroup
        # by setting tid=0 we move current tid to the custom cgroup
        if any(x in self.predefined_cgroups for x in self.cgroup.split("/")):
            raise AlreadyInCgroup(self.subsystem, self.cgroup)
        Path(self.cgroup_mount_path / custom_cgroup).mkdir(exist_ok=True)
        Path(self.cgroup_mount_path / custom_cgroup / "tasks").write_text(str(tid))

    def read_from_control_file(self, file_name: str) -> str:
        controller_path = Path(self.cgroup_mount_path / file_name)
        return controller_path.read_text()

    def write_to_control_file(self, file_name: str, data: str) -> None:
        controller_path = Path(self.cgroup_mount_path / file_name)
        controller_path.write_text(data)

    def get_cgroup_pids(self) -> List[int]:
        content = Path(self.cgroup_mount_path / "tasks").read_text()
        return list(map(int, filter(None, content.split("\n"))))

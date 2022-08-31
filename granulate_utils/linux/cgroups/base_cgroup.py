#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from enum import Enum
from pathlib import Path
from typing import List

PID_CGROUPS = Path("/proc/self/cgroup")
CGROUPFS = Path("/sys/fs/cgroup")


class SUBSYSTEMS(Enum):
    memory = "memory"
    cpu = "cpu,cpuacct"


class BaseCgroup:
    predefined_cgroups = ["kubepods", "docker", "ecs"]

    def __init__(self) -> None:
        self._verify_preconditions()

    def _verify_preconditions(self) -> None:
        assert self.subsystem in SUBSYSTEMS.__members__, f"{self.subsystem!r} is not supported"

        # "/proc/$PID/cgroup" lists a process's cgroup membership.  If legacy
        # cgroup is in use in the system, this file may contain multiple lines, one for each hierarchy.
        # The entry for cgroup v2 is always in the format "0::$PATH"::
        if len(PID_CGROUPS.read_text().split("\n")) == 2:
            raise Exception("cgroup V2 is unsupported")

    @property
    def subsystem(self) -> str:
        raise NotImplementedError()

    def _get_cgroup(self) -> str:
        for line in PID_CGROUPS.read_text().split("\n"):
            hierarchy = line.strip().split(":")
            if SUBSYSTEMS[self.subsystem].value == hierarchy[1]:
                return hierarchy[2]
        raise Exception(f"{self.subsystem!r} is not in found")

    def move_to_cgroup(self, custom_cgroup: str, pid: int = 0) -> None:
        if any(x in self.predefined_cgroups for x in self.cgroup.split("/")):
            raise Exception(f"{self.subsystem!r} subsytem is already in a predefined cgroup: {self.cgroup!r}")
        Path(self.cgroup_mount_path / custom_cgroup).mkdir(exist_ok=True)
        Path(self.cgroup_mount_path / custom_cgroup / "tasks").write_text(str(pid))

    @property
    def cgroup(self) -> str:
        return self._get_cgroup()

    @property
    def cgroup_mount_path(self) -> Path:
        return Path(CGROUPFS / self.subsystem / self.cgroup[1:])

    def read_from_control_file(self, file_name: str) -> str:
        controller_path = Path(self.cgroup_mount_path / file_name)
        return controller_path.read_text()

    def write_to_control_file(self, file_name: str, data: str) -> None:
        controller_path = Path(self.cgroup_mount_path / file_name)
        controller_path.write_text(data)

    def get_cgroup_pids(self) -> List[int]:
        content = Path(self.cgroup_mount_path / "tasks").read_text()
        return list(map(int, filter(None, content.split("\n"))))

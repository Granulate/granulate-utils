#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from enum import Enum
from pathlib import Path
from typing import List

from granulate_utils.linux.cgroups.exceptions import MissingCgroup, MissingController, SkippedCgroup, UnsupportedCgroup

PID_CGROUPS = Path("/proc/self/cgroup")
CGROUP_PARENT_PATH = Path("/sys/fs/cgroup")


class CGROUPS(Enum):
    memory = "memory"
    cpu = "cpu,cpuacct"


class CgroupVerifications:
    @staticmethod
    def is_cgroup_v1() -> None:
        # "/proc/$PID/cgroup" lists a process's cgroup membership.  If legacy
        # cgroup is in use in the system, this file may contain multiple lines,
        # one for each hierarchy.
        # The entry for cgroup v2 is always in the format "0::$PATH"::
        if len(PID_CGROUPS.read_text().split("\n")) == 2:
            raise UnsupportedCgroup("version 2")

    @staticmethod
    def is_supported_cgroup(cgroup_type: str) -> None:
        if cgroup_type not in CGROUPS.__members__:
            raise UnsupportedCgroup(cgroup_type)

    @staticmethod
    def is_ignored_cgroup(cgroup: str, custom_cgroup: str) -> None:
        ignore_list = ["kubepods", "docker", "ecs"]
        if any(x in ignore_list for x in cgroup.split("/")):
            raise SkippedCgroup(cgroup, custom_cgroup)

    @staticmethod
    def is_controller_exists(controller_path: Path) -> None:
        if not controller_path.is_file():
            raise MissingController(controller_path)


class BaseCgroup:
    def __init__(self) -> None:
        CgroupVerifications.is_cgroup_v1()
        CgroupVerifications.is_supported_cgroup(self.controller)

    @property
    def controller(self) -> str:
        raise NotImplementedError()

    def _get_cgroup(self) -> str:
        cgroups = PID_CGROUPS.read_text()
        for line in cgroups.split("\n"):
            parsed_cgroup = line.strip().split(":")
            if CGROUPS[self.controller].value == parsed_cgroup[1]:
                return parsed_cgroup[2]

        raise MissingCgroup(self.controller, PID_CGROUPS)

    def move_to_cgroup(self, custom_cgroup: str, pid: int = 0) -> None:
        CgroupVerifications.is_ignored_cgroup(self.cgroup, custom_cgroup)
        Path.mkdir(Path(self.cgroup_path / custom_cgroup), exist_ok=True)
        Path(self.cgroup_path / custom_cgroup / "tasks").write_text(str(pid))

    @property
    def cgroup(self) -> str:
        return self._get_cgroup()

    @property
    def cgroup_path(self) -> Path:
        return Path(CGROUP_PARENT_PATH / self.controller / self.cgroup[1:])

    def read_from_controller(self, file_name: str) -> str:
        controller_path = Path(self.cgroup_path / file_name)
        try:
            return controller_path.read_text()
        except FileNotFoundError:
            raise MissingController(controller_path)

    def write_to_controller(self, file_name: str, data: str) -> None:
        controller_path = Path(self.cgroup_path / file_name)
        CgroupVerifications.is_controller_exists(controller_path)
        try:
            controller_path.write_text(data)
        except FileNotFoundError:
            raise MissingController(controller_path)

    def get_cgroup_pids(self) -> List[str]:
        content = Path(self.cgroup_path / "tasks").read_text()
        return list(filter(None, content.split("\n")))

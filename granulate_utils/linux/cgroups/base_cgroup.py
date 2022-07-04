#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import os
from enum import Enum
from pathlib import Path
from typing import List

from granulate_utils.linux.cgroups.common import split_and_filter
from granulate_utils.linux.cgroups.exceptions import MissingCgroup, SkippedCgroup, UnsupportedCgroup

PID_CGROUPS = Path("/proc/self/cgroup")
CGROUP_PARENT_PATH = Path("/sys/fs/cgroup")

IGNORE_LIST = ["kubepods", "docker"]


class HIERARCHIES(Enum):
    memory = "memory"
    cpu = "cpu,cpuacct"


class CgroupVerifications:
    @staticmethod
    def verify_cgroup_v1() -> None:
        if len(PID_CGROUPS.read_text().split("\n")) == 2:
            raise UnsupportedCgroup("version 2")

    @staticmethod
    def verify_supported_cgroup(cgroup_type: str) -> None:
        assert cgroup_type, "must provide cgroup type"
        if cgroup_type not in HIERARCHIES.__members__:
            raise UnsupportedCgroup(cgroup_type)

    @staticmethod
    def verify_ignored_cgroup(cgroup: str, cgroup_name: str) -> None:
        if any(x in IGNORE_LIST for x in cgroup.split("/")):
            raise SkippedCgroup(cgroup, cgroup_name)


class BaseCgroup:
    HIERARCHY = ""

    def __init__(self) -> None:
        CgroupVerifications.verify_cgroup_v1()
        CgroupVerifications.verify_supported_cgroup(self.HIERARCHY)

    def _get_cgroup(self) -> str:
        cgroups = PID_CGROUPS.read_text()
        for line in cgroups.split("\n"):
            parsed_cgroup = line.strip().split(":")
            if HIERARCHIES[self.HIERARCHY].value == parsed_cgroup[1]:
                return parsed_cgroup[2]

        raise MissingCgroup(self.HIERARCHY, PID_CGROUPS.as_posix())

    def move_to_cgroup(self, cgroup_name: str, pid: int = 0) -> None:
        CgroupVerifications.verify_ignored_cgroup(self.cgroup, cgroup_name)
        os.makedirs(Path(self.cgroup_path / cgroup_name), exist_ok=True)
        Path(self.cgroup_path / cgroup_name / "tasks").write_text("%s\n" % pid)

    @property
    def cgroup(self) -> str:
        return self._get_cgroup()

    @property
    def cgroup_path(self) -> Path:
        return Path(CGROUP_PARENT_PATH / self.HIERARCHY / self.cgroup[1:])

    def read_from_controller(self, file_name: str) -> str:
        return Path(self.cgroup_path / file_name).read_text()

    def write_to_controller(self, file_name: str, data: str) -> None:
        Path(self.cgroup_path / file_name).write_text(data)

    def get_cgroup_pids(self) -> List[str]:
        return split_and_filter(Path(self.cgroup_path / "tasks").read_text())

    def print_cgroups(self):
        print(PID_CGROUPS.read_text())

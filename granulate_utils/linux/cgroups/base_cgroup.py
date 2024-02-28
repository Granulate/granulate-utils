#
# Copyright (C) 2023 Intel Corporation
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pathlib import Path
from typing import List, Mapping, Optional, Set

from granulate_utils.exceptions import AlreadyInCgroup, UnsupportedCGroupV2
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

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

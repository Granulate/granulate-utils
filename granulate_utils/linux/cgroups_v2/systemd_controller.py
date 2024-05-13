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
from typing import Optional, Union

from psutil import Process

from granulate_utils.linux.cgroups_v2.base_controller import BaseController
from granulate_utils.linux.cgroups_v2.cgroup import CgroupCore, ControllerType


class SystemdLegacyController(BaseController):
    CONTROLLER: ControllerType = "name=systemd"

    @classmethod
    def get_cgroup_core(cls, cgroup: Optional[Union[Path, CgroupCore, Process]] = None) -> CgroupCore:
        core = super().get_cgroup_core(cgroup)
        assert core.is_v1, "Systemd controller should only be available in cgroups v1"
        return core


class SystemdUnifiedController(BaseController):
    """Systemd controller for cgroups v2, which just represents the v2 hierarchy without any particular controller"""

    CONTROLLER: ControllerType = ""

    @classmethod
    def get_cgroup_core(cls, cgroup: Optional[Union[Path, CgroupCore, Process]] = None) -> CgroupCore:
        core = super().get_cgroup_core(cgroup)
        assert core.is_v2, "Empty controller should only be available in cgroups v2"
        return core

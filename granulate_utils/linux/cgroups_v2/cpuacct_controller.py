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

from granulate_utils.linux.cgroups_v2.base_controller import BaseController
from granulate_utils.linux.cgroups_v2.cgroup import ControllerType


class CpuAcctController(BaseController):
    CONTROLLER: ControllerType = "cpuacct"
    CPUACCT_USAGE_FILE = "cpuacct.usage"

    def get_cpu_time_ns(self) -> int:
        return int(self.read_from_interface_file(self.CPUACCT_USAGE_FILE))

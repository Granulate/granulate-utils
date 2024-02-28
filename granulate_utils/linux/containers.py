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

import re
from typing import Optional

from psutil import Process

from granulate_utils.linux import cgroups

# ECS uses /ecs/uuid/container-id
# standard Docker uses /docker/container-id
# k8s uses /kubepods/{burstable,besteffort}/uuid/container-id
# there are some variations to the above formats, but generally, the container
# ID is always 64-hex digits.
CONTAINER_ID_PATTERN = re.compile(r"[a-f0-9]{64}")


def get_process_container_id(process: Process) -> Optional[str]:
    """
    Gets the container ID of a running process, or None if not in a container.
    :raises NoSuchProcess: If the process doesn't or no longer exists
    """
    for _, _, cgpath in cgroups.get_cgroups(process):
        found = CONTAINER_ID_PATTERN.findall(cgpath)
        if found:
            return found[-1]

    return None

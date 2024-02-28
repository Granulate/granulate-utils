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

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import psutil


@dataclass
class TimeInfo:
    create_time: datetime  # Creation time of the container (UTC)
    start_time: Optional[datetime]  # Start time of the container (UTC) - None=not started


@dataclass
class Container:
    """
    Shared "Container" descriptor class, used for Docker containers & CRI containers.
    """

    runtime: str  # docker / containerd / crio
    # container name for Docker
    # reconstructed container name (as if it were Docker) for CRI
    name: str
    id: str
    labels: Dict[str, str]
    running: bool
    # None if not requested / container is dead
    process: Optional[psutil.Process]
    # None if not requested, make sure to pass all_info=True
    time_info: Optional[TimeInfo]


class ContainersClientInterface:
    def list_containers(self, all_info: bool) -> List[Container]:
        raise NotImplementedError

    def get_container(self, container_id: str, all_info: bool) -> Container:
        raise NotImplementedError

    def get_runtimes(self) -> List[str]:
        raise NotImplementedError

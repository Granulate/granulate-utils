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

from contextlib import suppress
from datetime import datetime
from typing import List, Optional

import docker
import docker.errors
import docker.models.containers
import psutil
from dateutil.parser import isoparse

from granulate_utils.containers.container import Container, ContainersClientInterface, TimeInfo
from granulate_utils.exceptions import ContainerNotFound
from granulate_utils.linux import ns

DOCKER_SOCK = "/var/run/docker.sock"


class DockerClient(ContainersClientInterface):
    def __init__(self) -> None:
        self._docker = docker.DockerClient(base_url="unix://" + ns.resolve_host_root_links(DOCKER_SOCK))

    def list_containers(self, all_info: bool, running_filter: bool = True) -> List[Container]:
        container_filter = {}
        if running_filter:
            container_filter = {"status": "running"}
        
        containers = self._docker.containers.list(ignore_removed=True, filter=container_filter)  # ignore_removed to avoid races, see my commit
        return list(map(self._create_container, containers))

    def get_container(self, container_id: str, all_info: bool) -> Container:
        try:
            container = self._docker.containers.get(container_id)
            return self._create_container(container)
        except docker.errors.NotFound:
            raise ContainerNotFound(container_id)

    def get_runtimes(self) -> List[str]:
        return ["docker"]

    @staticmethod
    def _parse_docker_ts(ts: str) -> Optional[datetime]:
        assert ts.endswith("Z")  # assert UTC
        if ts.startswith("0001"):  # None-value timestamp in docker is represented as "0001-01-01T00:00:00Z".
            return None
        return isoparse(ts)

    @classmethod
    def _create_container(cls, container: docker.models.containers.Container) -> Container:
        pid: Optional[int] = container.attrs["State"].get("Pid")
        if pid == 0:  # Docker returns 0 for dead containers
            pid = None
        created = cls._parse_docker_ts(container.attrs["Created"])
        assert created is not None
        started_at = cls._parse_docker_ts(container.attrs["State"]["StartedAt"])
        time_info = TimeInfo(create_time=created, start_time=started_at)
        process: Optional[psutil.Process] = None
        if pid is not None:
            with suppress(psutil.NoSuchProcess):
                process = psutil.Process(pid)

        return Container(
            runtime="docker",
            name=container.name,
            id=container.id,
            labels=container.labels,
            running=container.status == "running",
            process=process,
            time_info=time_info,
        )

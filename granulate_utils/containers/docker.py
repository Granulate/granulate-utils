#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from datetime import datetime
from typing import List, Optional

import docker
import docker.errors
import docker.models.containers
from dateutil.parser import isoparse

from granulate_utils.containers.container import Container, ContainersClientInterface, TimeInfo
from granulate_utils.exceptions import ContainerNotFound
from granulate_utils.linux import ns

DOCKER_SOCK = "/var/run/docker.sock"


class DockerClient(ContainersClientInterface):
    def __init__(self) -> None:
        self._docker = docker.DockerClient(base_url="unix://" + ns.resolve_host_root_links(DOCKER_SOCK))

    def list_containers(self, all_info: bool) -> List[Container]:
        containers = self._docker.containers.list(ignore_removed=True)  # ignore_removed to avoid races, see my commit
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
        created = cls._parse_docker_ts(container.attrs["Created"])
        assert created is not None
        started_at = cls._parse_docker_ts(container.attrs["State"]["StartedAt"])
        if pid == 0:  # Docker returns 0 for dead containers
            pid = None
        time_info = TimeInfo(create_time=created, start_time=started_at)
        return Container(
            runtime="docker",
            name=container.name,
            id=container.id,
            labels=container.labels,
            running=container.status == "running",
            pid=pid,
            time_info=time_info,
        )

#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from datetime import datetime
from typing import List, Optional

import docker
import docker.errors
import docker.models.containers

from granulate_utils.containers.container import Container, ContainersClientInterface
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
    def _parse_docker_timestamp(time_str: str) -> Optional[datetime]:
        """
        Parses timestamps provided by docker API to datetime.
        DockerAPI provides iso datetimes (in UTC) with fractional milliseconds that python standard library doesn't parse, and
        also ends with "Z" timezone indicator for UTC.
        """
        assert time_str.endswith('Z')  # assert UTC
        if time_str.startswith('0001'):
            return None
        return datetime.fromisoformat(time_str.split('.')[0])

    @classmethod
    def _create_container(cls, container: docker.models.containers.Container) -> Container:
        pid: Optional[int] = container.attrs["State"].get("Pid")
        created = cls._parse_docker_timestamp(container.attrs['Created'])
        assert created is not None
        started_at = cls._parse_docker_timestamp(container.attrs['State']['StartedAt'])
        if pid == 0:  # Docker returns 0 for dead containers
            pid = None
        return Container(
            runtime="docker",
            name=container.name,
            id=container.id,
            labels=container.labels,
            running=container.status == "running",
            pid=pid,
            create_time=created,
            start_time=started_at
        )

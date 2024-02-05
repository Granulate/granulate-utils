#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from contextlib import suppress
from datetime import datetime
import json
from typing import Any, List, Optional

import docker
import docker.errors
import docker.models.containers
import psutil
from dateutil.parser import isoparse

from granulate_utils.containers.container import Container, ContainersClientInterface, TimeInfo, Network
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
    
    def get_networks(self, container_id: str) -> Network:
        container = self._docker.containers.get(container_id)
        networks_dict = json.loads(next(container.stats()))['networks']
        networks_filtered_dict = {k: v for k, v in networks_dict.items() if k.startswith("eth")}

        return [
            Network(
                name=k,
                rx_bytes=v["rx_bytes"],
                rx_errors=v["rx_errors"],
                tx_bytes=v["tx_bytes"],
                tx_errors=v["tx_errors"],
            )
            for k, v in networks_filtered_dict.items()
        ]
    
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
            networks=json.loads(next(container.stats()))['networks'],
        )

#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from typing import List, Optional

import docker

from granulate_utils.containers.container import Container
from granulate_utils.linux.ns import resolve_host_root_links

DOCKER_SOCK = "/var/run/docker.sock"


class DockerClient:
    def __init__(self):
        self._docker = docker.DockerClient(base_url="unix://" + resolve_host_root_links(DOCKER_SOCK))

    def list_containers(self, all_info: bool) -> List[Container]:
        containers: List[Container] = []

        for container in self._docker.containers.list():
            pid: Optional[int] = container.attrs["State"].get("Pid")
            if pid == 0:  # Docker returns 0 for dead containers
                pid = None
            containers.append(
                Container(
                    runtime="docker",
                    name=container.name,
                    id=container.id,
                    labels=container.labels,
                    running=container.status == "running",
                    pid=pid,
                )
            )

        return containers

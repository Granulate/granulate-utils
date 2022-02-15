#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import logging
from typing import List

from granulate_utils.containers.container import Container
from granulate_utils.containers.cri.client import CRIClient
from granulate_utils.containers.docker import DockerClient


class ContainersClient:
    """
    Wraps DockerClient and CRIClient to provide a unified view of all containers
    running on a system.
    """

    def __init__(self):
        try:
            self._docker_client = DockerClient()
        except Exception:
            logging.warning("Failed to initialize DockerClient, skipping...", exc_info=True)
            self._docker_client = None
        try:
            self._cri_client = CRIClient()
        except Exception:
            logging.warning("Failed to initialize CRIClient, skipping...", exc_info=True)
            self._cri_client = None

    def list_containers(self, all_info: bool = False) -> List[Container]:
        docker_containers = self._docker_client.list_containers(all_info) if self._docker_client is not None else []
        cri_containers = self._cri_client.list_containers(all_info) if self._cri_client is not None else []

        # start with all Docker containers
        containers = docker_containers.copy()
        # then add CRI containers that are not already listed
        # TODO explain why both, ...
        for cri_container in cri_containers:
            matching_docker = filter(lambda c: c.id == cri_container.id, containers)
            try:
                docker_container = next(matching_docker)
                assert (
                    docker_container.name == cri_container.name
                ), f"Non matching names: {cri_container} {docker_container}"
            except StopIteration:
                containers.append(cri_container)

        return containers

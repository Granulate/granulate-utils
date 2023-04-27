#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import multiprocessing
from typing import Any, List

from granulate_utils.containers.container import Container, ContainersClientInterface
from granulate_utils.exceptions import ContainerNotFound, CriNotAvailableError


class CriClientSubprocess(ContainersClientInterface):
    def __init__(self) -> None:
        self._runtimes = self.call_with_args(self._remote_get_runtimes)

        if not self._runtimes:
            raise CriNotAvailableError("CRI is not available")

    @staticmethod
    def _remote_get_runtimes(channel) -> None:
        from granulate_utils.containers.cri import CriClient

        channel.send(CriClient().get_runtimes())

    @staticmethod
    def _remote_list_containers(channel, all_info: bool) -> None:
        from granulate_utils.containers.cri import CriClient
        ctrs = CriClient().list_containers(all_info=all_info)
        print(ctrs)
        channel.send(ctrs)

    @staticmethod
    def _remote_get_container(channel, container_id: str, all_info: bool) -> None:
        from granulate_utils.containers.cri import CriClient

        try:
            c = CriClient().get_container(container_id=container_id, all_info=all_info)
        except ContainerNotFound:
            channel.send(None)
        else:
            channel.send(c)

    @staticmethod
    def call_with_args(callable, *args) -> Any:
        ch1, ch2 = multiprocessing.Pipe()
        p = multiprocessing.Process(target=callable, args=(ch2, *args))
        p.start()
        p.join()
        return ch1.recv()

    def list_containers(self, all_info: bool) -> List[Container]:
        return self.call_with_args(self._remote_list_containers, all_info)

    def get_container(self, container_id: str, all_info: bool) -> Container:
        r = self.call_with_args(self._remote_list_containers, container_id, all_info)
        if r is None:
            raise ContainerNotFound(container_id)
        return r

    def get_runtimes(self) -> List[str]:
        return self._runtimes

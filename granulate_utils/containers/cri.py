#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import json
from datetime import datetime
from typing import List, Optional, Union, cast

import grpc  # type: ignore # no types-grpc sadly

from granulate_utils.containers.container import Container, ContainersClientInterface
from granulate_utils.exceptions import ContainerNotFound, CriNotAvailableError
from granulate_utils.generated.containers.cri import api_pb2 as api_pb2  # type: ignore
from granulate_utils.generated.containers.cri.api_pb2_grpc import RuntimeServiceStub  # type: ignore
from granulate_utils.linux import ns

RUNTIMES = (
    ("containerd", "/run/containerd/containerd.sock"),
    ("crio", "/var/run/crio/crio.sock"),
)

# see https://github.com/kubernetes/cri-api/blob/v0.24.0-alpha.2/pkg/apis/runtime/v1alpha2/api.proto#L1013
CONTAINER_RUNNING = 1


class RuntimeServiceWrapper(RuntimeServiceStub):
    def __init__(self, path: str):
        self._channel = grpc.insecure_channel(path)
        super().__init__(self._channel)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._channel.close()


class CriClient(ContainersClientInterface):
    def __init__(self) -> None:
        self._runtimes = {}
        for rt, path in RUNTIMES:
            path = "unix://" + ns.resolve_host_root_links(path)
            if self._is_cri_available(path):
                self._runtimes[rt] = path

        if not self._runtimes:
            raise CriNotAvailableError(f"CRI is not available at any of {RUNTIMES}")

    @staticmethod
    def _is_cri_available(path: str) -> bool:
        with RuntimeServiceWrapper(path) as stub:
            try:
                stub.Version(api_pb2.VersionRequest())
                return True
            except grpc._channel._InactiveRpcError:
                return False

    @staticmethod
    def _reconstruct_name(container: Union[api_pb2.Container, api_pb2.ContainerStatus]) -> str:
        """
        Reconstruct the name that dockershim would have used, for compatibility with DockerClient.
        See makeContainerName in kubernetes/pkg/kubelet/dockershim/naming.go
        """
        # I know that those labels exist because CRI lists only k8s containers.
        container_name = container.labels["io.kubernetes.container.name"]
        sandbox_name = container.labels["io.kubernetes.pod.name"]
        namespace = container.labels["io.kubernetes.pod.namespace"]
        sandbox_uid = container.labels["io.kubernetes.pod.uid"]
        restart_count = container.annotations["io.kubernetes.container.restartCount"]
        return "_".join(["k8s", container_name, sandbox_name, namespace, sandbox_uid, restart_count])

    def list_containers(self, all_info: bool) -> List[Container]:
        containers: List[Container] = []

        for rt, path in self._runtimes.items():
            with RuntimeServiceWrapper(path) as stub:
                for c in stub.ListContainers(api_pb2.ListContainersRequest()).containers:
                    container = self._get_container_from_runtime(c.id, rt, stub, all_info)
                    if container is not None:
                        containers.append(container)

        return containers

    def _get_container_from_runtime(self, container_id: str, runtime_name: str, stub: RuntimeServiceWrapper, all_info: bool) -> Optional[Container]:
        try:
            status_response = stub.ContainerStatus(
                api_pb2.ContainerStatusRequest(container_id=container_id, verbose=all_info)
            )
        except grpc._channel._InactiveRpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise

        pid: Optional[int] = json.loads(status_response.info.get("info", "{}")).get("pid")
        return self._create_container_from_status(status_response.status, pid, runtime_name)

    def get_container(self, container_id: str, all_info: bool) -> Container:
        for rt, path in self._runtimes.items():
            with RuntimeServiceWrapper(path) as stub:
                maybe_container = self._get_container_from_runtime(container_id, rt, stub, all_info)
                if maybe_container is not None:
                    return maybe_container

        raise ContainerNotFound(container_id)

    def get_runtimes(self) -> List[str]:
        return list(self._runtimes.keys())

    @classmethod
    def _create_container_from_status(
        cls, status: api_pb2.ContainerStatus, pid: Optional[int], runtime: str
    ) -> Container:
        created_at_ns = cast(int, status.created_at)
        started_at_ns = cast(int, status.started_at)
        create_time = datetime.utcfromtimestamp(created_at_ns / 1e9)
        start_time = None
        # from ContainerStatus message docs, 0 == not started
        if started_at_ns != 0:
            start_time = datetime.utcfromtimestamp(started_at_ns / 1e9)
        return Container(
            runtime=runtime,
            name=cls._reconstruct_name(status),
            id=status.id,
            labels=status.labels,
            running=status.state == CONTAINER_RUNNING,
            pid=pid,
            create_time=create_time,
            start_time=start_time
        )

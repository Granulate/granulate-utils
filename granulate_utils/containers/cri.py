#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import json
from datetime import datetime, timezone
from typing import List, Optional, Union

import grpc  # type: ignore # no types-grpc sadly

from granulate_utils.containers.container import Container, ContainersClientInterface, TimeInfo
from granulate_utils.exceptions import ContainerNotFound, CriNotAvailableError
from granulate_utils.generated.containers.cri import api_pb2 as api_pb2  # type: ignore
from granulate_utils.generated.containers.cri.api_pb2_grpc import RuntimeServiceStub  # type: ignore
from granulate_utils.linux import ns
from granulate_utils.type_utils import assert_cast

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
                for container in stub.ListContainers(api_pb2.ListContainersRequest()).containers:
                    if all_info:
                        # need verbose=True to get the info which contains the PID
                        status_response = self._container_status_request(stub, container.id, verbose=True)
                        if not status_response:
                            # container probably went down
                            continue
                        pid: Optional[int] = json.loads(status_response.info.get("info", "{}")).get("pid")
                        containers.append(self._create_container(status_response.status, rt, pid))
                    else:
                        containers.append(self._create_container(container, rt, None))

        return containers

    def _container_status_request(
        self, stub: RuntimeServiceStub, container_id: str, *, verbose: bool
    ) -> Optional[api_pb2.ContainerStatusResponse]:
        try:
            return stub.ContainerStatus(api_pb2.ContainerStatusRequest(container_id=container_id, verbose=verbose))
        except grpc._channel._InactiveRpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise

    def get_container(self, container_id: str, all_info: bool) -> Container:
        for rt, path in self._runtimes.items():
            with RuntimeServiceWrapper(path) as stub:
                status_response = self._container_status_request(stub, container_id, verbose=all_info)
                if status_response is None:
                    continue
                pid: Optional[int] = json.loads(status_response.info.get("info", "{}")).get("pid")
                return self._create_container(status_response.status, rt, pid)

        raise ContainerNotFound(container_id)

    def get_runtimes(self) -> List[str]:
        return list(self._runtimes.keys())

    @classmethod
    def _create_container(
        cls,
        container: Union[api_pb2.Container, api_pb2.ContainerStatus],
        runtime: str,
        pid: Optional[int] = None,
    ) -> Container:
        time_info: Optional[TimeInfo] = None
        if isinstance(container, api_pb2.ContainerStatus):
            created_at_ns = assert_cast(int, container.created_at)
            started_at_ns = assert_cast(int, container.started_at)
            create_time = datetime.fromtimestamp(created_at_ns / 1e9, tz=timezone.utc)
            start_time = None
            # from ContainerStatus message docs, 0 == not started
            if started_at_ns != 0:
                start_time = datetime.fromtimestamp(started_at_ns / 1e9, tz=timezone.utc)
            time_info = TimeInfo(create_time=create_time, start_time=start_time)
        return Container(
            runtime=runtime,
            name=cls._reconstruct_name(container),
            id=container.id,
            labels=container.labels,
            running=container.state == CONTAINER_RUNNING,
            pid=pid,
            time_info=time_info,
        )

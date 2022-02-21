#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import json
from typing import List, Optional

import grpc  # type: ignore # no types-grpc sadly

import granulate_utils.generated.containers.cri.api_pb2 as api_pb2  # type: ignore
from granulate_utils.containers.container import Container, ContainersClientInterface
from granulate_utils.exceptions import CriNotAvailableError
from granulate_utils.generated.containers.cri.api_pb2_grpc import RuntimeServiceStub  # type: ignore
from granulate_utils.linux.ns import resolve_host_root_links

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
    def __init__(self):
        for rt, path in RUNTIMES:
            path = "unix://" + resolve_host_root_links(path)
            if self._is_cri_available(path):
                self._path = path
                self._runtime_name = rt
                break
        else:
            raise CriNotAvailableError(f"CRI is not available at any of {RUNTIMES}")

    def _is_cri_available(self, path: str) -> bool:
        with RuntimeServiceWrapper(path) as stub:
            try:
                stub.Version(api_pb2.VersionRequest())
                return True
            except grpc._channel._InactiveRpcError:
                return False

    def _reconstruct_name(self, container: api_pb2.Container) -> str:
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

        with RuntimeServiceWrapper(self._path) as stub:
            for container in stub.ListContainers(api_pb2.ListContainersRequest()).containers:
                if all_info:
                    # need verbose=True to get the info which contains the PID
                    status = stub.ContainerStatus(
                        api_pb2.ContainerStatusRequest(container_id=container.id, verbose=True)
                    )
                    pid: Optional[int] = json.loads(status.info.get("info", "{}")).get("pid")
                else:
                    pid = None

                containers.append(
                    Container(
                        runtime=self._runtime_name,
                        name=self._reconstruct_name(container),
                        id=container.id,
                        labels=container.labels,
                        running=container.state == CONTAINER_RUNNING,
                        pid=pid,
                    )
                )

        return containers

    def get_runtimes(self) -> List[str]:
        return [self._runtime_name]

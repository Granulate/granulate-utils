#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

# no types-grpc sadly
import grpc  # type: ignore

from granulate_utils.containers.cri.generated.api_pb2 import ListContainersRequest  # type: ignore
from granulate_utils.containers.cri.generated.api_pb2_grpc import RuntimeServiceStub  # type: ignore


def list_containers():
    channel = grpc.insecure_channel("unix:///run/containerd/containerd.sock")
    runtime_stub = RuntimeServiceStub(channel)
    return runtime_stub.ListContainers(ListContainersRequest())

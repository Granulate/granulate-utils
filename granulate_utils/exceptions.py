#
# Copyright (C) 2023 Intel Corporation
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from psutil import Process


class UnsupportedNamespaceError(Exception):
    def __init__(self, nstype: str):
        super().__init__(f"Namespace {nstype!r} is not supported by this kernel")
        self.nstype = nstype


class UnsupportedCGroupV2(Exception):
    def __init__(self):
        super().__init__("cgroup v2 is not supported by granulate-utils")


class CouldNotAcquireMutex(Exception):
    def __init__(self, name) -> None:
        super().__init__(f"Could not acquire mutex {name!r}. Another process might be holding it.")


class CriNotAvailableError(Exception):
    pass


class NoContainerRuntimesError(Exception):
    pass


class ContainerNotFound(Exception):
    def __init__(self, container_id: str) -> None:
        super().__init__(f"Could not find container with id {container_id!r}")


class BadResponseCode(Exception):
    def __init__(self, response_code: int):
        super().__init__(f"Got a bad HTTP response code {response_code}")


class MissingExePath(Exception):
    def __init__(self, process: Process):
        self.process = process
        super(MissingExePath, self).__init__(f"No exe path was found for {process}, threads: {process.threads()}")


class AlreadyInCgroup(Exception):
    def __init__(self, subsystem: str, cgroup: str) -> None:
        super().__init__(f"{subsystem!r} subsystem is already in a predefined cgroup: {cgroup!r}")


class DatabricksMetadataFetchException(Exception):
    pass


class SparkAPIException(DatabricksMetadataFetchException):
    pass


class SparkNotReadyException(SparkAPIException):
    pass


class SparkAppsURLDiscoveryException(DatabricksMetadataFetchException):
    pass


class DatabricksTagsExtractionException(DatabricksMetadataFetchException):
    pass


class DatabricksJobNameDiscoverException(DatabricksMetadataFetchException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class CgroupInterfaceNotSupported(Exception):
    def __init__(self, interface_name: str, cgroup_version: str):
        super(CgroupInterfaceNotSupported, self).__init__(
            f"Interface file {interface_name} is not supported in cGroup {cgroup_version}"
        )


class CgroupControllerNotMounted(Exception):
    def __init__(self, controller_name: str):
        super(CgroupControllerNotMounted, self).__init__(f"Controller {controller_name} is not mounted on the system")

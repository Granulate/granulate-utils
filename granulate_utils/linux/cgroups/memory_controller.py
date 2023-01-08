#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from abc import abstractmethod
from pathlib import Path
from typing import Optional, Union

from granulate_utils.linux.cgroups.base_controller import BaseController
from granulate_utils.linux.cgroups.cgroup import CgroupCore, CgroupCoreV1, CgroupCoreV2


class MemoryControllerInterface:
    memory_limit: str
    usage: str
    max_usage_in_bytes: str = ""
    swap_limit: str = ""

    def __init__(self, cgroup: CgroupCore):
        self.cgroup = cgroup

    @abstractmethod
    def set_limit_in_bytes(self, limit: int) -> None:
        pass

    def get_memory_limit(self) -> int:
        return int(self.cgroup.read_from_interface_file(self.memory_limit))

    def _set_swap_limit(self, limit: str) -> None:
        try:
            self.cgroup.write_to_interface_file(self.swap_limit, limit)
        except PermissionError:
            # if swap extension is not enabled (CONFIG_MEMCG_SWAP) this file doesn't exist
            # and PermissionError is thrown (since it can't be created)
            pass


class MemoryControllerV1(MemoryControllerInterface):
    memory_limit = "memory.limit_in_bytes"
    swap_limit = "memory.memsw.limit_in_bytes"
    max_usage_in_bytes = "memory.max_usage_in_bytes"
    usage = "memory.usage_in_bytes"

    def set_limit_in_bytes(self, limit: int) -> None:
        # in case memsw.limit_in_bytes file exists we need to reset it in order to
        # change limit_in_bytes in case it's smaller than memsw.limit_in_bytes
        super()._set_swap_limit("-1")
        self.cgroup.write_to_interface_file(self.memory_limit, str(limit))

        # memsw.limit_in_bytes is already set to -1
        if limit != -1:
            self._set_swap_limit(str(limit))


class MemoryControllerV2(MemoryControllerInterface):
    UNBOUNDED_QUOTA_VALUE = "max"
    memory_limit = "memory.max"
    swap_limit = "memory.swap.max"
    usage = "memory.current"

    def set_limit_in_bytes(self, limit: int) -> None:
        # In CgroupV2 swap and memory are 2 different attributes that can be tuned, as opposed to Cgroup V1,
        # in which we can just set a limit on both swap and memory together.
        # So in Cgroup V2 we set swap limit to be 0.
        swap_limit = "0"
        memory_limit = str(limit)
        if limit == -1:
            memory_limit = swap_limit = "max"
        super()._set_swap_limit(swap_limit)
        self.cgroup.write_to_interface_file(self.memory_limit, str(memory_limit))

    def get_memory_limit(self) -> int:
        memory_limit = self.cgroup.read_from_interface_file(self.memory_limit)
        if memory_limit == self.UNBOUNDED_QUOTA_VALUE:
            return -1
        return int(memory_limit)


class MemoryController(BaseController):
    controller = "memory"

    def __init__(self, cgroup: Optional[Union[Path, CgroupCore]] = None):
        super().__init__(cgroup)
        if isinstance(self.cgroup, CgroupCoreV1):
            self.controller_interface: MemoryControllerInterface = MemoryControllerV1(self.cgroup)
        elif isinstance(self.cgroup, CgroupCoreV2):
            self.controller_interface = MemoryControllerV2(self.cgroup)

    def get_memory_limit(self) -> int:
        return self.controller_interface.get_memory_limit()

    def get_usage_in_bytes(self) -> int:
        return int(self.read_from_interface_file(self.controller_interface.usage))

    def get_max_usage_in_bytes(self) -> int:
        if self.controller_interface.max_usage_in_bytes == "":
            raise Exception("Not implemented")
        return int(self.read_from_interface_file(self.controller_interface.max_usage_in_bytes))

    def reset_memory_limit(self) -> None:
        self.set_limit_in_bytes(-1)

    def set_limit_in_bytes(self, limit: int) -> None:
        self.controller_interface.set_limit_in_bytes(limit)

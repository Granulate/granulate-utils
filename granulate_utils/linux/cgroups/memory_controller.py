#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from abc import abstractmethod
from pathlib import Path
from typing import Optional, Union

from granulate_utils.exceptions import CgroupInterfaceNotSupported
from granulate_utils.linux.cgroups.base_controller import BaseController
from granulate_utils.linux.cgroups.cgroup import (
    CGROUP_V2_UNBOUNDED_VALUE,
    CgroupCore,
    CgroupCoreV1,
    CgroupCoreV2,
    ControllerType,
)


class MemoryControllerInterface:
    MEMORY_LIMIT_FILE: str
    MEMORY_USAGE_FILE: str
    MEMORY_MAX_USAGE_IN_BYTES_FILE: str
    MEMORY_SWAP_LIMIT_FILE: str

    def __init__(self, cgroup: CgroupCore):
        self.cgroup = cgroup

    @abstractmethod
    def set_limit_in_bytes(self, limit: int) -> None:
        pass

    def get_memory_limit(self) -> int:
        return self.convert_inner_value_to_outer(self.cgroup.read_from_interface_file(self.MEMORY_LIMIT_FILE))

    @classmethod
    def convert_outer_value_to_inner(cls, val: int) -> str:
        return str(val)

    @classmethod
    def convert_inner_value_to_outer(cls, val: str) -> int:
        return int(val)

    def _set_swap_limit(self, limit: int) -> None:
        try:
            self.cgroup.write_to_interface_file(self.MEMORY_SWAP_LIMIT_FILE, self.convert_outer_value_to_inner(limit))
        except PermissionError:
            # if swap extension is not enabled (CONFIG_MEMCG_SWAP), this file doesn't exist
            # and PermissionError is thrown (since it can't be created)
            pass


class MemoryControllerV1(MemoryControllerInterface):
    MEMORY_LIMIT_FILE = "memory.limit_in_bytes"
    MEMORY_SWAP_LIMIT_FILE = "memory.memsw.limit_in_bytes"
    MEMORY_MAX_USAGE_IN_BYTES_FILE = "memory.max_usage_in_bytes"
    MEMORY_USAGE_FILE = "memory.usage_in_bytes"

    def set_limit_in_bytes(self, limit: int) -> None:
        # if we try to set memsw.limit_in_bytes so that memsw.limit_in_bytes < memory.limit_in_bytes,
        # it will result in an error.
        # So we reset memsw.limit_in_bytes before setting memory.limit_in_bytes and then set it.
        super()._set_swap_limit(-1)
        self.cgroup.write_to_interface_file(self.MEMORY_LIMIT_FILE, str(limit))

        # memsw.limit_in_bytes is already set to -1
        if limit != -1:
            self._set_swap_limit(limit)


class MemoryControllerV2(MemoryControllerInterface):
    MEMORY_LIMIT_FILE = "memory.max"
    MEMORY_SWAP_LIMIT_FILE = "memory.swap.max"
    MEMORY_MAX_USAGE_IN_BYTES_FILE = ""
    MEMORY_USAGE_FILE = "memory.current"

    def set_limit_in_bytes(self, limit: int) -> None:
        # In CgroupV2 swap and memory are 2 different attributes that can be tuned, as opposed to Cgroup V1,
        # in which we can just set a limit on both swap and memory together.
        # So in Cgroup V2 we set swap limit to be 0.
        if limit == -1:
            swap_limit = -1
            memory_limit = CGROUP_V2_UNBOUNDED_VALUE
        else:
            swap_limit = 0
            memory_limit = str(limit)
        super()._set_swap_limit(swap_limit)
        self.cgroup.write_to_interface_file(self.MEMORY_LIMIT_FILE, memory_limit)

    @classmethod
    def convert_outer_value_to_inner(cls, val: int) -> str:
        if val == -1:
            return CGROUP_V2_UNBOUNDED_VALUE
        return super().convert_outer_value_to_inner(val)

    @classmethod
    def convert_inner_value_to_outer(cls, val: str) -> int:
        if val == CGROUP_V2_UNBOUNDED_VALUE:
            return -1
        return super().convert_inner_value_to_outer(val)


class MemoryController(BaseController):
    CONTROLLER: ControllerType = "memory"

    def __init__(self, cgroup: Optional[Union[Path, CgroupCore]] = None):
        super().__init__(cgroup)
        if isinstance(self.cgroup, CgroupCoreV1):
            self.controller_interface: MemoryControllerInterface = MemoryControllerV1(self.cgroup)
        elif isinstance(self.cgroup, CgroupCoreV2):
            self.controller_interface = MemoryControllerV2(self.cgroup)

    def get_memory_limit(self) -> int:
        """
        Returns the memory limit. If memory is unbounded, return -1
        """
        return self.controller_interface.get_memory_limit()

    def get_usage_in_bytes(self) -> int:
        return int(self.read_from_interface_file(self.controller_interface.MEMORY_USAGE_FILE))

    def get_max_usage_in_bytes(self) -> int:
        if self.controller_interface.MEMORY_MAX_USAGE_IN_BYTES_FILE == "":
            # Cgroup V2 doesn't implement max_usage
            raise CgroupInterfaceNotSupported("max_usage_in_bytes", "v2")
        return int(self.read_from_interface_file(self.controller_interface.MEMORY_MAX_USAGE_IN_BYTES_FILE))

    def reset_memory_limit(self) -> None:
        self.set_limit_in_bytes(-1)

    def set_limit_in_bytes(self, limit: int) -> None:
        """
        Set the memory limit in bytes.
        :param limit: memory limit in bytes. -1 is for unbounded.
        """
        self.controller_interface.set_limit_in_bytes(limit)

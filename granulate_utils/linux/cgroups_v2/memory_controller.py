#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from abc import abstractmethod
from pathlib import Path
from typing import Optional, Union

from psutil import Process

from granulate_utils.exceptions import CgroupInterfaceNotSupported
from granulate_utils.linux.cgroups_v2.base_controller import BaseController
from granulate_utils.linux.cgroups_v2.cgroup import CgroupCore, ControllerType


class MemoryController(BaseController):
    CONTROLLER: ControllerType = "memory"
    MEMORY_LIMIT_FILE: str = ""
    MEMORY_USAGE_FILE: str = ""
    MEMORY_MAX_USAGE_IN_BYTES_FILE: str = ""
    MEMORY_SWAP_LIMIT_FILE: str = ""

    def __init__(self, cgroup: Optional[Union[Path, CgroupCore]] = None):
        super().__init__(cgroup)

    def get_usage_in_bytes(self) -> int:
        return int(self.read_from_interface_file(self.MEMORY_USAGE_FILE))

    def get_max_usage_in_bytes(self) -> int:
        if self.MEMORY_MAX_USAGE_IN_BYTES_FILE == "":
            # Cgroup V2 doesn't implement max_usage
            raise CgroupInterfaceNotSupported("max_usage_in_bytes", "v2")
        return int(self.read_from_interface_file(self.MEMORY_MAX_USAGE_IN_BYTES_FILE))

    def reset_memory_limit(self) -> None:
        self.set_limit_in_bytes(-1)

    @abstractmethod
    def set_limit_in_bytes(self, limit: int) -> None:
        """
        Set the memory limit in bytes.
        :param limit: memory limit in bytes. -1 is for unbounded.
        """
        pass

    def get_memory_limit(self) -> int:
        """
        Returns the memory limit. If memory is unbounded, return -1
        """
        return self.cgroup.convert_inner_value_to_outer(self.cgroup.read_from_interface_file(self.MEMORY_LIMIT_FILE))

    def _set_swap_limit(self, limit: int) -> None:
        try:
            self.cgroup.write_to_interface_file(
                self.MEMORY_SWAP_LIMIT_FILE, self.cgroup.convert_outer_value_to_inner(limit)
            )
        except PermissionError:
            # if swap extension is not enabled (CONFIG_MEMCG_SWAP), this file doesn't exist
            # and PermissionError is thrown (since it can't be created)
            pass


class MemoryControllerV1(MemoryController):
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


class MemoryControllerV2(MemoryController):
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
        else:
            swap_limit = 0

        super()._set_swap_limit(swap_limit)
        self.cgroup.write_to_interface_file(self.MEMORY_LIMIT_FILE, self.cgroup.convert_outer_value_to_inner(limit))


class MemoryControllerFactory:
    @staticmethod
    def get_memory_controller(cgroup: Optional[Union[CgroupCore, Path, Process]] = None) -> MemoryController:
        cgroup_core = MemoryController.get_cgroup_core(cgroup)
        if cgroup_core.is_v1:
            return MemoryControllerV1(cgroup_core)
        return MemoryControllerV2(cgroup_core)

    @classmethod
    def create_sub_memory_controller(
        cls, new_cgroup_name: str, parent_cgroup: Optional[Union[Path, CgroupCore]] = None
    ) -> MemoryController:
        current_cgroup = MemoryController.get_cgroup_core(parent_cgroup)
        subcgroup_core = current_cgroup.get_subcgroup(MemoryController.CONTROLLER, new_cgroup_name)
        return cls.get_memory_controller(subcgroup_core)

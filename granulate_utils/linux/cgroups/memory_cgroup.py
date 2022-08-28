#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from base_cgroup import BaseCgroup
from exceptions import MissingController


class MemoryCgroup(BaseCgroup):
    HIERARCHY = "memory"

    def get_memory_limit(self) -> int:
        return int(self.read_from_controller("memory.limit_in_bytes"))

    def get_max_memory(self) -> int:
        return int(self.read_from_controller("memory.max_usage_in_bytes"))

    def set_memory_limit(self, limit: int) -> None:
        self.write_to_controller("memory.limit_in_bytes", f"{limit}")
        try:
            self.write_to_controller("memory.memsw.limit_in_bytes", f"{limit}")
        except MissingController:
            # if swap extension is not enabled (CONFIG_MEMCG_SWAP) this file doesn't exist
            pass

    def reset_memory_limit(self) -> None:
        self.write_to_controller("memory.limit_in_bytes", "-1")

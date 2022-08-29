#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from granulate_utils.linux.cgroups.base_cgroup import BaseCgroup
from granulate_utils.linux.cgroups.exceptions import MissingController


class MemoryCgroup(BaseCgroup):
    controller = "memory"

    def get_memory_limit(self) -> int:
        return int(self.read_from_controller("memory.limit_in_bytes"))

    def get_max_usage_in_bytes(self) -> int:
        return int(self.read_from_controller("memory.max_usage_in_bytes"))

    def set_limit_in_bytes(self, limit: int) -> None:
        self.write_to_controller("memory.limit_in_bytes", str(limit))
        try:
            self.write_to_controller("memory.memsw.limit_in_bytes", str(limit))
        except MissingController:
            # if swap extension is not enabled (CONFIG_MEMCG_SWAP) this file doesn't exist
            pass

    def reset_memory_limit(self) -> None:
        self.write_to_controller("memory.limit_in_bytes", "-1")

#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from granulate_utils.linux.cgroups.base_cgroup import BaseCgroup


class MemoryCgroup(BaseCgroup):
    subsystem = "memory"
    limit_in_bytes = "memory.limit_in_bytes"
    memsw_limit_in_bytes = "memory.memsw.limit_in_bytes"
    max_usage_in_bytes = "memory.max_usage_in_bytes"

    def get_memory_limit(self) -> int:
        return int(self.read_from_control_file(self.limit_in_bytes))

    def get_max_usage_in_bytes(self) -> int:
        return int(self.read_from_control_file(self.max_usage_in_bytes))

    def set_limit_in_bytes(self, limit: int) -> None:
        self.write_to_control_file(self.limit_in_bytes, str(limit))
        try:
            self.write_to_control_file(self.memsw_limit_in_bytes, str(limit))
        except PermissionError:
            # if swap extension is not enabled (CONFIG_MEMCG_SWAP) this file doesn't exist
            pass

    def reset_memory_limit(self) -> None:
        self.write_to_control_file(self.limit_in_bytes, "-1")

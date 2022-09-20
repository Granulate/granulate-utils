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

    def _set_memsw_limit_in_bytes(self, limit: int) -> None:
        try:
            self.write_to_control_file(self.memsw_limit_in_bytes, str(limit))
        except PermissionError:
            # if swap extension is not enabled (CONFIG_MEMCG_SWAP) this file doesn't exist
            # and PermissionError is thrown (since it can't be created)
            pass

    def set_limit_in_bytes(self, limit: int) -> None:
        # in case memsw_limit_in_bytes file exists we need to reset it in order to
        # change limit_in_bytes in case it's smaller than memsw_limit_in_bytes
        self._set_memsw_limit_in_bytes(-1)
        self.write_to_control_file(self.limit_in_bytes, str(limit))
        if limit != -1:
            # memsw_limit_in_bytes already happend for -1
            self._set_memsw_limit_in_bytes(limit)

    def reset_memory_limit(self) -> None:
        self.set_limit_in_bytes(-1)

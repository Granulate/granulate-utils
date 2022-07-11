#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from granulate_utils.linux.cgroups_client.base_cgroup import BaseCgroup
from granulate_utils.linux.cgroups_client.common import bytes_to_gigabytes
from granulate_utils.linux.cgroups_client.exceptions import MissingController


class MemoryCgroup(BaseCgroup):
    HIERARCHY = "memory"

    def get_memory_limit(self) -> float:
        return bytes_to_gigabytes(float(self.read_from_controller("memory.limit_in_bytes")))

    def get_max_memory(self) -> float:
        return bytes_to_gigabytes(float(self.read_from_controller("memory.max_usage_in_bytes")))

    def set_memory_limit(self, limit_in_gb: float) -> None:
        self.write_to_controller("memory.limit_in_bytes", f"{limit_in_gb}G")
        try:
            self.write_to_controller("memory.memsw.limit_in_bytes", f"{limit_in_gb}G")
        except MissingController:
            # if swap extension is not enabled (CONFIG_MEMCG_SWAP) this file doesn't exist
            pass

    def reset_memory_limit(self) -> None:
        self.write_to_controller("memory.limit_in_bytes", "-1")

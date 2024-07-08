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

from granulate_utils.linux.cgroups.base_cgroup import BaseCgroup


class MemoryCgroup(BaseCgroup):
    subsystem = "memory"
    limit_in_bytes = "memory.limit_in_bytes"
    memsw_limit_in_bytes = "memory.memsw.limit_in_bytes"
    max_usage_in_bytes = "memory.max_usage_in_bytes"
    usage_in_bytes = "memory.usage_in_bytes"

    def get_memory_limit(self) -> int:
        return int(self.read_from_control_file(self.limit_in_bytes))

    def get_max_usage_in_bytes(self) -> int:
        return int(self.read_from_control_file(self.max_usage_in_bytes))

    def get_usage_in_bytes(self) -> int:
        return int(self.read_from_control_file(self.usage_in_bytes))

    def _set_memsw_limit_in_bytes(self, limit: int) -> None:
        try:
            self.write_to_control_file(self.memsw_limit_in_bytes, str(limit))
        except PermissionError:
            # if swap extension is not enabled (CONFIG_MEMCG_SWAP) this file doesn't exist
            # and PermissionError is thrown (since it can't be created)
            pass

    def set_limit_in_bytes(self, limit: int) -> None:
        # in case memsw.limit_in_bytes file exists we need to reset it in order to
        # change limit_in_bytes in case it's smaller than memsw.limit_in_bytes
        self._set_memsw_limit_in_bytes(-1)
        self.write_to_control_file(self.limit_in_bytes, str(limit))

        # memsw.limit_in_bytes is already set to -1
        if limit != -1:
            self._set_memsw_limit_in_bytes(limit)

    def reset_memory_limit(self) -> None:
        self.set_limit_in_bytes(-1)

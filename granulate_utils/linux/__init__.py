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
import os
from typing import Tuple


def get_kernel_release() -> Tuple[int, int]:
    """Return Linux kernel version as (major, minor) tuple."""
    major_str, minor_str = os.uname().release.split(".", maxsplit=2)[:2]
    return int(major_str), int(minor_str)


# TASK_COMM_LEN is 16, and it is always null-terminated, so 15.
COMM_PATTERN = r".{0,15}"

import warnings

warnings.warn(
    "granulate_utils.linux.cgroups is deprecated, use granulate_utils.linux.cgroups_v2 instead",
    DeprecationWarning,
    stacklevel=2,
)

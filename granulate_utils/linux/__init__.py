#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import os
from typing import Tuple


def get_kernel_release() -> Tuple[int, int]:
    """Return Linux kernel version as (major, minor) tuple."""
    major_str, minor_str = os.uname().release.split(".", maxsplit=2)[:2]
    return int(major_str), int(minor_str)

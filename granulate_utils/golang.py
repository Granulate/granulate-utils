#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import functools
import struct
from typing import Optional

from psutil import NoSuchProcess, Process

from granulate_utils.linux.elf import read_elf_symbol, read_elf_va


def is_golang_process(process: Process) -> bool:
    return get_process_golang_version(process) is not None


@functools.lru_cache(maxsize=4096)
def get_process_golang_version(process: Process) -> Optional[str]:
    elf_path = f"/proc/{process.pid}/exe"
    try:
        symbol_data = read_elf_symbol(elf_path, "runtime.buildVersion", 16)
    except FileNotFoundError:
        raise NoSuchProcess(process.pid)
    if symbol_data is None:
        return None

    # Declaration of go string type:
    # type stringStruct struct {
    # 	str unsafe.Pointer
    # 	len int
    # }
    addr, length = struct.unpack("QQ", symbol_data)
    try:
        golang_version_bytes = read_elf_va(elf_path, addr, length)
    except FileNotFoundError:
        raise NoSuchProcess(process.pid)
    if golang_version_bytes is None:
        return None

    return golang_version_bytes.decode()

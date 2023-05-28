#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import functools
import struct
from typing import Optional

from psutil import NoSuchProcess, Process, pids

from granulate_utils.linux.elf import read_elf_symbol, read_elf_va
from granulate_utils.linux.ns import get_mnt_ns_ancestor
from granulate_utils.linux.process import is_kernel_thread, process_exe


def is_golang_process(process: Process) -> bool:
    return not is_kernel_thread(process) and get_process_golang_version(process.create_time()) is not None


@functools.lru_cache(maxsize=4096)
def get_process_golang_version(process_start_time: float) -> Optional[str]:
    process = None
    for pid in pids():
        if Process(pid).create_time() == process_start_time:
            process = Process(pid)

    if process is None:
        raise NoSuchProcess(process_start_time)
    try:
        exe = process_exe(process)
    except:
        return None
    elf_path = f"/proc/{get_mnt_ns_ancestor(process).pid}/root{exe}"
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

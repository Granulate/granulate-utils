#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import os
import re
import struct
from functools import lru_cache
from typing import Optional

import psutil

from granulate_utils.exceptions import MissingExePath
from granulate_utils.linux.elf import get_elf_id

_AUXV_ENTRY = struct.Struct("LL")

AT_EXECFN = 31
PATH_MAX = 4096


def process_exe(process: psutil.Process) -> str:
    """
    psutil.Process(pid).exe() returns "" for zombie processes, incorrectly. It should raise ZombieProcess, and return ""
    only for kernel threads.

    See https://github.com/giampaolo/psutil/pull/2062
    """
    exe = process.exe()
    if exe == "":
        if is_process_zombie(process):
            raise psutil.ZombieProcess(process.pid)
        raise MissingExePath(process)
    return exe


def is_process_running(process: psutil.Process, allow_zombie: bool = False) -> bool:
    """
    psutil.Process(pid).is_running() considers zombie processes as running. This utility can be used to check if a
    process is actually running and not in a zombie state
    """
    return process.is_running() and (allow_zombie or not is_process_zombie(process))


def is_process_zombie(process: psutil.Process) -> bool:
    return process.status() == "zombie"


def is_musl(process: psutil.Process) -> bool:
    # TODO: make sure no glibc libc.so file exists (i.e, return True if musl, False if glibc, and raise
    # if not conclusive)
    return any("ld-musl" in m.path for m in process.memory_maps())


def get_mapped_dso_elf_id(process: psutil.Process, dso_part: str) -> Optional[str]:
    """
    Searches for a DSO path containing "dso_part" and gets its elfid.
    Returns None if not found.
    """
    for m in process.memory_maps():
        if dso_part in m.path:
            # don't need resolve_proc_root_links here - paths in /proc/pid/maps are normalized.
            return get_elf_id(f"/proc/{process.pid}/root/{m.path}")
    else:
        return None


def _read_process_auxv(process: psutil.Process, auxv_id: int) -> int:
    try:
        with open(f"/proc/{process.pid}/auxv", "rb") as f:
            auxv = f.read()
    except FileNotFoundError:
        raise psutil.NoSuchProcess(process.pid)

    for i in range(0, len(auxv), _AUXV_ENTRY.size):
        entry = auxv[i : i + _AUXV_ENTRY.size]
        id_, val = _AUXV_ENTRY.unpack(entry)

        if id_ == auxv_id:
            assert isinstance(val, int)  # mypy fails to understand
            return val
    else:
        raise ValueError(f"auxv id {auxv_id} was not found!")


def _read_process_memory(process: psutil.Process, addr: int, size: int) -> bytes:
    try:
        with open(f"/proc/{process.pid}/mem", "rb", buffering=0) as mem:
            mem.seek(addr)
            return mem.read(size)
    except FileNotFoundError:
        raise psutil.NoSuchProcess(process.pid)


def read_process_execfn(process: psutil.Process) -> str:
    # reads process AT_EXECFN
    addr = _read_process_auxv(process, AT_EXECFN)
    fn = _read_process_memory(process, addr, PATH_MAX)
    return fn[: fn.index(b"\0")].decode()


@lru_cache(maxsize=512)
def is_process_basename_matching(process: psutil.Process, basename_pattern: str) -> bool:
    if re.match(basename_pattern, os.path.basename(process_exe(process))):
        return True

    # process was executed AS basename (but has different exe name)
    cmd = process.cmdline()
    if len(cmd) > 0 and re.match(basename_pattern, cmd[0]):
        return True

    return False

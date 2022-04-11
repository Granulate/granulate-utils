#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import ctypes
import os
from typing import Any, Optional

import psutil


def process_exe(process: psutil.Process) -> str:
    """
    psutil.Process(pid).exe() returns "" for zombie processes, incorrectly. It should raise ZombieProcess, and return ""
    only for kernel threads.

    See https://github.com/giampaolo/psutil/pull/2062
    """
    exe = process.exe()
    if exe == "" and is_process_zombie(process):
        raise psutil.ZombieProcess(process.pid)
    return exe


def is_process_running(process: psutil.Process, allow_zombie: bool = False) -> bool:
    """
    psutil.Process(pid).is_running() considers zombie processes as running. This utility can be used to check if a
    process is actually running and not in a zombie state
    """
    return process.is_running() and (allow_zombie or not is_process_zombie(process))


def is_process_zombie(process: psutil.Process) -> bool:
    return process.status() == "zombie"


libc: Optional[ctypes.CDLL] = None


def prctl(*argv: Any) -> None:
    global libc
    if libc is None:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
    ret = libc.prctl(*argv)
    if ret != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))

#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import psutil


class MissingExePath(Exception):
    def __init__(self, process):
        super(MissingExePath, self).__init__(f"No exe path was found for pid {process.pid}")


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

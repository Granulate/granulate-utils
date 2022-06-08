#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import psutil
import os


class DeletedExeException(psutil.Error):
    def __init__(self, pid):
        super(DeletedExeException, self).__init__(f"exe deleted for pid: {pid}")


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
        # following to https://man7.org/linux/man-pages/man5/proc.5.html,
        # an empty exe can be returned in case of a broken link
        if is_exe_deleted(process):
            raise DeletedExeException(process.pid)
    return exe


def is_process_running(process: psutil.Process, allow_zombie: bool = False) -> bool:
    """
    psutil.Process(pid).is_running() considers zombie processes as running. This utility can be used to check if a
    process is actually running and not in a zombie state
    """
    return process.is_running() and (allow_zombie or not is_process_zombie(process))


def is_process_zombie(process: psutil.Process) -> bool:
    return process.status() == "zombie"


def is_exe_deleted(process: psutil.Process) -> bool:
    return os.path.exists(f"/proc/{process.pid}/exe")

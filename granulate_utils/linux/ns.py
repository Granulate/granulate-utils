#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import ctypes
import enum
import os
from pathlib import Path
from threading import Thread
from typing import Callable, List, Optional, TypeVar, Union

from psutil import NoSuchProcess, Process

T = TypeVar("T")


class _Sentinel:
    pass


_SENTINEL = _Sentinel()


class NsType(enum.IntFlag):
    mnt = 0x00020000  # CLONE_NEWNS
    net = 0x40000000  # CLONE_NEWNET
    pid = 0x20000000  # CLONE_NEWPID
    uts = 0x04000000  # CLONE_NEWUTS


libc: Optional[ctypes.CDLL] = None


def resolve_proc_root_links(proc_root: str, ns_path: str) -> str:
    """
    Resolves "ns_path" which (possibly) resides in another mount namespace.

    If ns_path contains absolute symlinks, it can't be accessed merely by /proc/pid/root/ns_path,
    because the resolved absolute symlinks will "escape" the /proc/pid/root base.

    To work around that, we resolve the path component by component; if any component "escapes", we
    add the /proc/pid/root prefix once again.
    """
    assert ns_path[0] == "/", f"expected {ns_path!r} to be absolute"
    parts = Path(ns_path).parts

    path = proc_root
    for part in parts[1:]:  # skip the / (or multiple /// as .parts gives them)
        next_path = os.path.join(path, part)
        if os.path.islink(next_path):
            link = os.readlink(next_path)
            if os.path.isabs(link):
                # absolute - prefix with proc_root
                next_path = proc_root + link
            else:
                # relative: just join
                next_path = os.path.join(path, link)
        path = next_path

    return path


def is_same_ns(pid: int, nstype: str, pid2: int = None) -> bool:
    return (
        os.stat(f"/proc/{pid2 if pid2 is not None else 'self'}/ns/{nstype}").st_ino
        == os.stat(f"/proc/{pid}/ns/{nstype}").st_ino
    )


def run_in_ns(nstypes: List[str], callback: Callable[[], T], target_pid: int = 1) -> T:
    """
    Runs a callback in a new thread, switching to a set of the namespaces of a target process before
    doing so.

    Needed initially for switching mount namespaces, because we can't setns(CLONE_NEWNS) in a multithreaded
    program (unless we unshare(CLONE_NEWNS) before). so, we start a new thread, unshare() & setns() it,
    run our callback and then stop the thread (so we don't keep unshared threads running around).
    For other namespace types, we use this function to execute callbacks without changing the namespaces
    for the core threads.

    By default, run stuff in init NS. You can pass 'target_pid' to run in the namespace of that process.
    """

    # make sure "mnt" is last, once we change it our /proc is gone
    nstypes = sorted(nstypes, key=lambda ns: 1 if ns == "mnt" else 0)

    ret: Union[T, _Sentinel] = _SENTINEL
    exc: Optional[BaseException] = None

    def _switch_and_run():
        try:
            global libc
            if libc is None:
                libc = ctypes.CDLL("libc.so.6")

            for nstype in nstypes:
                if not is_same_ns(target_pid, nstype):
                    flag = NsType[nstype].value
                    if libc.unshare(flag) != 0:
                        raise ValueError(f"Failed to unshare({nstype})")

                    with open(f"/proc/{target_pid}/ns/{nstype}", "r") as nsf:
                        if libc.setns(nsf.fileno(), flag) != 0:
                            raise ValueError(f"Failed to setns({nstype}) (to pid {target_pid})")

            nonlocal ret
            ret = callback()
        except BaseException as e:
            # save the exception so we can re-raise it in the calling thread
            nonlocal exc
            exc = e

    t = Thread(target=_switch_and_run)
    t.start()
    t.join()

    if isinstance(ret, _Sentinel):
        assert exc is not None
        raise Exception("run_in_ns execution failed") from exc
    else:
        assert exc is None
        return ret


def get_mnt_ns_ancestor(process: Process) -> int:
    """
    Gets the topmost ancestor of "process" that runs in the same mount namespace of "process".
    """
    while True:
        parent = process.parent()
        if parent is None:  # topmost ancestor?
            return process.pid

        if not is_same_ns(process.pid, "mnt", parent.pid):
            return process.pid

        process = parent


def is_running_in_init_pid() -> bool:
    """
    Check if we're running in the init PID namespace.

    This check is implemented by checking if PID 2 is running, and if it's named "kthreadd"
    which is the kernel thread from which kernel threads are forked. It's always PID 2 and
    we should always see it in the init NS. If we don't have a PID 2 running, or if it's not named
    kthreadd, then we're not in the init PID NS.
    """
    try:
        p = Process(2)
    except NoSuchProcess:
        return False
    else:
        # technically, funny processes can name themselves "kthreadd", causing this check to pass in a non-init NS.
        # but we don't need to handle such extreme cases, I think.
        return p.name() == "kthreadd"


def get_proc_root_path(process: Process) -> str:
    """
    Gets /proc/<pid>/root of a given process, then a file can be read from the host mnt ns.
    """
    return f"/proc/{get_mnt_ns_ancestor(process)}/root"


def get_resolved_proc_root_path(process: Process, ns_path: str) -> str:
    """
    Gets /proc/<pid>/root of a given process, just like `get_proc_root_path` except that here we also resolve all the
    filesystem links.
    """
    return resolve_proc_root_links(get_proc_root_path(process), ns_path)

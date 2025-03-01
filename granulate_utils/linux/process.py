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
import contextlib
import os
import re
import struct
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Callable, Generator, Iterator, List, Optional

import psutil
from psutil import AccessDenied, NoSuchProcess

from granulate_utils.exceptions import MissingExePath
from granulate_utils.linux.elf import get_elf_id

_AUXV_ENTRY = struct.Struct("LL")

AT_EXECFN = 31
PATH_MAX = 4096


def process_exe(process: psutil.Process) -> str:
    """
    psutil.Process(pid).exe() caches the result. This function returns the up-to-date exe in case the process exec-ed.
    """
    # Clear the "exe" cache on the process object
    process._exe = None  # type: ignore
    exe = process.exe()
    if exe == "":
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


def is_musl(process: psutil.Process, maps: Optional[List[Any]] = None) -> bool:  # no proper type for maps :/
    """
    Returns True if the maps of the process contain a mapping of ld-musl, which we use as an identifier of
    musl-based processes.
    Note that this doesn't check for existence of glibc-compat (https://github.com/sgerrand/alpine-pkg-glibc). Processes
    might have ld-musl, but if they use glibc-compat we might want to consider them glibc based. This decision is left
    for the caller.
    """
    # TODO: make sure no glibc libc.so file exists (i.e, return True if musl, False if glibc, and raise
    # if not conclusive). if glibc-compat is in use, we will have glibc related maps...
    if maps is None:
        maps = process.memory_maps()
    return any("ld-musl" in m.path for m in maps)


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


def read_proc_file(process: psutil.Process, name: str) -> bytes:
    with translate_proc_errors(process):
        with open(f"/proc/{process.pid}/{name}", "rb") as f:
            return f.read()


def read_process_execfn(process: psutil.Process) -> str:
    # reads process AT_EXECFN
    addr = _read_process_auxv(process, AT_EXECFN)
    fn = _read_process_memory(process, addr, PATH_MAX)
    return fn[: fn.index(b"\0")].decode()


def _read_process_auxv(process: psutil.Process, auxv_id: int) -> int:
    auxv = read_proc_file(process, "auxv")
    if not auxv:
        # Kernel threads and exit()-ed processes don't have auxv.
        # We don't expect to be called on kernel threads
        assert not is_kernel_thread(process), "attempted reading auxv of kthread!"
        # The process status might still be alive until kernel updates it.
        # That's ok, it will become zombie/dead very soon.
        raise psutil.ZombieProcess(process.pid)

    for i in range(0, len(auxv), _AUXV_ENTRY.size):
        entry = auxv[i : i + _AUXV_ENTRY.size]
        id_, val = _AUXV_ENTRY.unpack(entry)

        if id_ == auxv_id:
            assert isinstance(val, int)  # mypy fails to understand
            return val
    else:
        raise ValueError(f"auxv id {auxv_id} was not found!")


def _read_process_memory(process: psutil.Process, addr: int, size: int) -> bytes:
    with translate_proc_errors(process):
        with open(f"/proc/{process.pid}/mem", "rb", buffering=0) as mem:
            mem.seek(addr)
            return mem.read(size)


@contextmanager
def translate_proc_errors(process: psutil.Process) -> Generator[None, None, None]:
    try:
        yield
        # Don't use the result if PID has been reused
        if not process.is_running():
            raise psutil.NoSuchProcess(process.pid)
    except PermissionError:
        raise psutil.AccessDenied(process.pid)
    except ProcessLookupError:
        raise psutil.NoSuchProcess(process.pid)
    except FileNotFoundError:
        if not os.path.exists(f"/proc/{process.pid}"):
            raise psutil.NoSuchProcess(process.pid)
        raise


@lru_cache(maxsize=512)
def is_process_basename_matching(process: psutil.Process, basename_pattern: str) -> bool:
    if re.match(basename_pattern, os.path.basename(process_exe(process))):
        return True

    # process was executed AS basename (but has different exe name)
    cmd = process.cmdline()
    if len(cmd) > 0 and re.match(basename_pattern, os.path.basename(cmd[0])):
        return True

    return False


def is_kernel_thread(process: psutil.Process) -> bool:
    # Kernel threads should be child of process with pid 2, or with pid 2.
    return process.pid == 2 or process.ppid() == 2


def search_for_process(filter: Callable[[psutil.Process], bool]) -> Iterator[psutil.Process]:
    for proc in psutil.process_iter():
        with contextlib.suppress(NoSuchProcess, AccessDenied):
            if is_process_running(proc) and filter(proc):
                yield proc


class ProcCgroupLine:
    """
    The format of the line:  hierarchy-ID:controller-list:relative-path
    Example line: 1:cpu:/custom_cgroup

    relative-path - the path of the cgroup the process belongs to, relative to the hierarchy mount point
    e.g. /sys/fs/cgroup/memory on v1 or just the cgroups v2 mount on v2 e.g /sys/fs/cgroup.
    """

    hier_id: str
    controllers: List[str]
    relative_path: str

    def __init__(self, procfs_line: str):
        hier_id, controller_list, relative_path = procfs_line.split(":", maxsplit=2)
        self.hier_id = hier_id
        self.controllers = controller_list.split(",")
        self.relative_path = relative_path


def get_process_cgroups(process: Optional[psutil.Process] = None) -> List[ProcCgroupLine]:
    """
    Get the cgroups of a process in [(hier id., controllers, path)] parsed form.
    If process is None, gets the cgroups of the current process.
    """
    process = process or psutil.Process()
    text = read_proc_file(process, "cgroup").decode()
    return [ProcCgroupLine(line) for line in text.splitlines()]

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

from __future__ import annotations

from pathlib import Path
from typing import List, Literal, Mapping, Optional, Union

import psutil
from psutil import Process
from typing_extensions import Self

from granulate_utils.exceptions import CgroupControllerNotMounted
from granulate_utils.linux import ns
from granulate_utils.linux.mountinfo import iter_mountinfo
from granulate_utils.linux.process import read_proc_file

ControllerType = Literal["memory", "cpu", "cpuacct"]
CONTROLLERS = {
    "blkio",
    "cpu",
    "cpuacct",
    "cpuset",
    "devices",
    "freezer",
    "hugetlb",
    "memory",
    "net_cls",
    "net_prio",
    "perf_event",
    "pids",
    "rdma",
}

CGROUP_V2_UNBOUNDED_VALUE = "max"


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


def _find_v1_hierarchies() -> Mapping[str, tuple[str, str]]:
    """
    Finds all the mounted hierarchies for all currently enabled cgroup v1 controllers.
    :return: A mapping from cgroup controller names to their respective hierarchies.
    """
    hierarchies = {}
    for mount in iter_mountinfo(1):
        if mount.filesystem_type != "cgroup":
            continue
        controllers = set(mount.super_options) & CONTROLLERS
        if controllers:
            hierarchy = mount.mount_point
            hierarchy = ns.resolve_host_root_links(hierarchy)
            mount_root = mount.root
            for controller in controllers:
                hierarchies[controller] = (hierarchy, mount_root)
    return hierarchies


def _find_v2_hierarchy() -> Optional[tuple[str, str]]:
    """
    Finds the mounted unified hierarchy for cgroup v2 controllers.
    """
    cgroup2_mounts = [
        mount for mount in iter_mountinfo(1) if mount.filesystem_type == "cgroup2" and mount.mount_source == "cgroup2"
    ]
    if not cgroup2_mounts:
        return None
    if len(cgroup2_mounts) > 1:
        raise Exception("More than one cgroup2 mount found!")
    path = cgroup2_mounts[0].mount_point
    path = ns.resolve_host_root_links(path)
    mount_root = cgroup2_mounts[0].root
    return path, mount_root


# Cgroup v1 and v2 handle the cgroup assigned processes using cgroup.procs
CGROUP_PROCS_FILE = "cgroup.procs"


class CgroupCore:
    """
    This object abstracts the general operations for CGroups for both v1 and v2:
    - Getting processes that belong to the cgroup or assigning processes to the cgroup.
    - Reading from or writing to interface files.
    - Creating CGroups under the hierarchy.
    cgroup_abs_path: The absolute path for this CGroupCore object
    """

    cgroup_abs_path: Path
    cgroup_mount_path: Path
    cgroup_mount_root: Path

    def __init__(self, cgroup_abs_path: Path, cgroup_mount_path: Path, cgroup_mount_root: Path):
        self.cgroup_abs_path = cgroup_abs_path
        self.cgroup_mount_path = cgroup_mount_path
        self.cgroup_mount_root = cgroup_mount_root

    def get_pids_in_cgroup(self) -> set[int]:
        return {int(proc) for proc in self.read_from_interface_file(CGROUP_PROCS_FILE).split()}

    def assign_process_to_cgroup(self, pid: int = 0) -> None:
        """
        :param pid: the pid, if 0 - current process
        """
        # This way is consistent between cGroups v1 and v2
        # The tasks file in cGroups v1 is used for moving specific threads instead of processes
        self.write_to_interface_file(CGROUP_PROCS_FILE, str(pid))

    def read_from_interface_file(self, interface_name: str) -> str:
        interface_path = self.cgroup_abs_path / interface_name
        return interface_path.read_text().strip()

    def write_to_interface_file(self, interface_name: str, data: str) -> None:
        interface_path = self.cgroup_abs_path / interface_name
        interface_path.write_text(data)

    @property
    def filesystem_type(self) -> str:
        return ""

    @property
    def is_v1(self) -> bool:
        return self.filesystem_type == "cgroup"

    @property
    def is_v2(self) -> bool:
        return self.filesystem_type == "cgroup2"

    @classmethod
    def convert_outer_value_to_inner(cls, val: int) -> str:
        return str(val)

    @classmethod
    def convert_inner_value_to_outer(cls, val: str) -> int:
        return int(val)

    @classmethod
    def build_object(cls, cgroup_abs_path: Path, mount_point: Path, cgroup_source_path: Path):
        return cls(cgroup_abs_path, mount_point, cgroup_source_path)

    def with_new_path(self, new_path: Path | str) -> Self:
        """Create a new CgroupCore with a new cgroup_abs_path under the same mount point
        Handles three cases:
        1. Relative path: concatenates to the current cgroup_abs_path
        2. Absolute path relative to the mount point (/sys/fs/cgroup/...): use it
        3. Weird path that comes from /proc/pid/cgroup that starts with a / but is actually
           relative to the mount ROOT, which is / in most cases but /docker/guid in some containers.
           In these cases, we strip the mount ROOT and treat as a relative path under the mount path"""

        new_path = Path(new_path)
        if not new_path.is_absolute():
            normalized_path = self.cgroup_abs_path / new_path
        else:
            # Real absolute paths have to be under self.cgroup_mount_path.
            try:
                new_path.relative_to(self.cgroup_mount_path)  # no exception -> real absolute path
                normalized_path = new_path
            except ValueError:
                # Some paths resemble absolute paths, but they are not:
                # /proc/pid/cgroup yields something the resembles an absolute path, but in reality it's relative
                # to the mount path (/sys/fs/cgroup/...) in the HOST ns.
                # We first strip the cgroup_mount_root ('/' if not in a container, '/docker/<guid>' if docker on V1)
                # to create a relative path, then add it to the mount path
                try:
                    normalized_path = self.cgroup_mount_path / new_path.relative_to(self.cgroup_mount_root)
                except ValueError:
                    raise ValueError(
                        f"new path {new_path} is not relative to cgroup mount root "
                        f"{self.cgroup_mount_root} or cgroup abs path {self.cgroup_abs_path}"
                    )

        return type(self)(normalized_path, self.cgroup_mount_path, self.cgroup_mount_root)


class CgroupCoreV1(CgroupCore):
    @property
    def filesystem_type(self) -> str:
        return "cgroup"


CGROUP_V2_SUPPORTED_CONTROLLERS = "cgroup.controllers"
CGROUP_V2_DELEGATED_CONTROLLERS = "cgroup.subtree_control"


class CgroupCoreV2(CgroupCore):
    def is_controller_supported(self, controller: ControllerType):
        return controller in self.read_from_interface_file(CGROUP_V2_SUPPORTED_CONTROLLERS).split()

    def is_controller_delegated(self, controller: ControllerType):
        return controller in self.read_from_interface_file(CGROUP_V2_DELEGATED_CONTROLLERS).split()

    @classmethod
    def convert_outer_value_to_inner(cls, val: int) -> str:
        if val == -1:
            return CGROUP_V2_UNBOUNDED_VALUE
        return super().convert_outer_value_to_inner(val)

    @classmethod
    def convert_inner_value_to_outer(cls, val: str) -> int:
        if val == CGROUP_V2_UNBOUNDED_VALUE:
            return -1
        return super().convert_inner_value_to_outer(val)

    @property
    def filesystem_type(self) -> str:
        return "cgroup2"


def _get_cgroup_mount(controller: ControllerType) -> Optional[CgroupCore]:
    """
    Returns a CgroupCore object for requested controller, or None if no such controller mount was found.
    If no v1 mount was found for the requested controller and there is a v2 mount (either unified or hybrid),
    a CgroupCoreV2 is returned (as all controllers share the same hierarchy in v2)
    """
    v1_paths = _find_v1_hierarchies()
    v2_path = _find_v2_hierarchy()
    if controller in v1_paths:
        # Either v1 or hybrid with the requested controller bound to a v1 hierarchy
        mount_point, mount_root = v1_paths[controller]
        return CgroupCoreV1(Path(mount_point), Path(mount_point), Path(mount_root))
    if v2_path:
        mount_path, mount_root = v2_path
        # v2 (unified) - check given controller is supported in cgroup v2
        cgroup_v2 = CgroupCoreV2(Path(mount_path), Path(mount_path), Path(mount_root))
        if cgroup_v2.is_controller_supported(controller):
            return cgroup_v2
    # No cgroup mount of the requested controller
    return None


def _get_cgroup_mount_checked(controller: ControllerType) -> CgroupCore:
    cgroup_mount = _get_cgroup_mount(controller)
    if cgroup_mount is None:
        raise CgroupControllerNotMounted(controller_name=controller)
    return cgroup_mount


def _get_cgroup_from_path(controller: ControllerType, cgroup_path_or_full_path: Path) -> CgroupCore:
    cgroup_mount = _get_cgroup_mount_checked(controller)
    return cgroup_mount.with_new_path(cgroup_path_or_full_path)


def _get_controller_relative_path(
    controller: ControllerType, process: Optional[psutil.Process] = None
) -> Optional[str]:
    for process_cgroup in get_process_cgroups(process):
        if controller in process_cgroup.controllers:
            return process_cgroup.relative_path

    return None


def _get_unified_controller_relative_path(process: Optional[psutil.Process] = None) -> Optional[str]:
    for process_cgroup in get_process_cgroups(process):
        if process_cgroup.hier_id == "0":
            return process_cgroup.relative_path

    return None


def _get_cgroup_for_process(controller: ControllerType, process: Optional[psutil.Process] = None) -> CgroupCore:
    """
    Get a CgrouopCore object for a given process. If process is None return for current process.
    """
    cgroup_mount = _get_cgroup_mount_checked(controller)

    if cgroup_mount.is_v1:
        controller_cgroup_relative_path = _get_controller_relative_path(controller, process)
        if controller_cgroup_relative_path is not None:
            return cgroup_mount.with_new_path(controller_cgroup_relative_path)
    elif cgroup_mount.is_v2:
        unified_cgroup_relative_path = _get_unified_controller_relative_path(process)
        if unified_cgroup_relative_path is not None:
            return cgroup_mount.with_new_path(unified_cgroup_relative_path)

    raise Exception(f"{controller!r} not found")


def get_cgroup_core(
    controller: ControllerType, cgroup: Optional[Union[Path, CgroupCore, Process]] = None
) -> CgroupCore:
    if cgroup is None or isinstance(cgroup, Process):
        # return current CgroupCore
        return _get_cgroup_for_process(controller, cgroup)
    elif isinstance(cgroup, Path):
        # create CgroupCore from path
        cgroup = _get_cgroup_from_path(controller, cgroup)

    assert isinstance(cgroup, CgroupCore), f"Unable to get CgroupCore from given input type {type(cgroup)}"
    return cgroup

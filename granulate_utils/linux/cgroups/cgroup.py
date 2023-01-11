#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from __future__ import annotations

import os
from abc import abstractmethod
from pathlib import Path
from typing import List, Mapping, Optional

import psutil

from granulate_utils.linux import ns
from granulate_utils.linux.mountinfo import iter_mountinfo
from granulate_utils.linux.process import read_proc_file

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


class ProcCgroupLine:
    """
    The format of the line:  hierarchy-ID:controller-list:cgroup-path
    Example line: 1:cpu:/custom_cgroup

    cgroup-path - the cgroup the process belongs to, relative to the hierarchy mount point e.g. /sys/fs/cgroup
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


def is_known_controller(controller: str) -> bool:
    return controller in CONTROLLERS


def find_v1_hierarchies(resolve_host_root_links: bool = True) -> Mapping[str, str]:
    """
    Finds all the mounted hierarchies for all currently enabled cgroup v1 controllers.
    :return: A mapping from cgroup controller names to their respective hierarchies.
    """
    hierarchies = {}
    for mount in iter_mountinfo(1):
        # The mount source is always "cgroup" by convention, we don't really have to check it.
        if mount.mount_source != "cgroup" or mount.filesystem_type != "cgroup":
            continue
        controllers = set(mount.super_options) & CONTROLLERS
        if controllers:
            hierarchy = mount.mount_point
            if resolve_host_root_links:
                hierarchy = ns.resolve_host_root_links(hierarchy)
            for controller in controllers:
                hierarchies[controller] = hierarchy
    return hierarchies


def find_v2_hierarchy(resolve_host_root_links: bool = True) -> Optional[str]:
    """
    Finds the mounted unified hierarchy for cgroup v2 controllers.
    """
    cgroup2_mounts = [mount for mount in iter_mountinfo(1) if mount.filesystem_type == "cgroup2"]
    if not cgroup2_mounts:
        return None
    if len(cgroup2_mounts) > 1:
        raise Exception("More than one cgroup2 mount found!")
    path = cgroup2_mounts[0].mount_point
    if resolve_host_root_links:
        path = ns.resolve_host_root_links(path)
    return path


CGROUP_PROCS_FILE = "cgroup.procs"


class CgroupCore:
    path: Path

    def __init__(self, path: Path):
        self.path = path

    def _get_parent_cgroup(self, cgroup_name: str) -> Optional[Path]:
        if cgroup_name in self.path.parts:
            return Path(self.path.as_posix().split(cgroup_name)[0]) / cgroup_name
        return None

    def _get_cgroup_in_hierarchy(self, cgroup_name: str) -> Optional[Path]:
        if self.has_parent_cgroup(cgroup_name):
            new_cgroup_path = self._get_parent_cgroup(cgroup_name)
        else:
            new_cgroup_path = self.path / cgroup_name
            new_cgroup_path.mkdir(exist_ok=True)

        return new_cgroup_path

    def has_parent_cgroup(self, cgroup_name: str) -> bool:
        """
        Check if cgroup has the given cgroup_name in its hierarchy
        :param cgroup_name: the cgroup name
        :return: bool indicating if cgroup is in hierarchy
        """
        return self._get_parent_cgroup(cgroup_name) is not None

    def get_pids_in_cgroup(self) -> set[int]:
        return {int(proc) for proc in self.read_from_interface_file(CGROUP_PROCS_FILE).split()}

    def assign_process_to_cgroup(self, pid: int = 0) -> None:
        self.write_to_interface_file(CGROUP_PROCS_FILE, str(pid))

    def read_from_interface_file(self, interface_name: str) -> str:
        interface_path = self.path / interface_name
        return interface_path.read_text()

    def write_to_interface_file(self, interface_name: str, data: str) -> None:
        interface_path = self.path / interface_name
        interface_path.write_text(data)

    @abstractmethod
    def get_cgroup_in_hierarchy(self, controller: str, cgroup_name: str) -> CgroupCore:
        """
        Return a CGroup in the hierarchy which has the given name.
        If one doesn't exist higher up in the hierarchy, create a new one lower in the hierarchy.
        """
        pass


class CgroupCoreV1(CgroupCore):
    def get_cgroup_in_hierarchy(self, controller: str, cgroup_name: str) -> CgroupCore:
        subcgroup_path = self._get_cgroup_in_hierarchy(cgroup_name)
        assert subcgroup_path is not None
        return CgroupCoreV1(subcgroup_path)


CGROUP_V2_SUPPORTED_CONTROLLERS = "cgroup.controllers"
CGROUP_V2_ENABLED_CONTROLLERS = "cgroup.subtree_control"


class CgroupCoreV2(CgroupCore):
    def is_controller_supported(self, controller: str):
        return controller in self.read_from_interface_file(CGROUP_V2_SUPPORTED_CONTROLLERS).split()

    def enable_controller(self, controller: str):
        assert self.is_controller_supported(controller), f"Controller not supported {controller!r}"
        enabled_controllers_file = self.path / CGROUP_V2_ENABLED_CONTROLLERS
        enabled_controllers_file.write_text(f"+{controller}{os.linesep}")

    def disable_controller(self, controller: str):
        enabled_controllers_file = self.path / CGROUP_V2_ENABLED_CONTROLLERS
        enabled_controllers_file.write_text(f"-{controller}{os.linesep}")

    def get_cgroup_in_hierarchy(self, controller: str, cgroup_name: str) -> CgroupCore:
        subcgroup_path = self._get_cgroup_in_hierarchy(cgroup_name)
        assert subcgroup_path is not None

        new_cgroup = CgroupCoreV2(subcgroup_path)
        new_cgroup.enable_controller(controller)
        return new_cgroup


def get_cgroup_mount(controller: str, resolve_host_root_links: bool = True) -> Optional[CgroupCore]:
    """
    Returns the folder that the requested controller is mounted to, or None if no such controller mount was found
    If no v1 mount was found for the requested controller and there is a v2 mount (either unified or hybrid),
    the v2 mountpoint is returned (as all controllers share the same hierarchy in v2)
    """
    v1_paths = find_v1_hierarchies(resolve_host_root_links)
    v2_path = find_v2_hierarchy(resolve_host_root_links)
    if v1_paths and controller in v1_paths:
        # Either v1 or hybrid with the requested controller bound to a v1 hierarchy
        return CgroupCoreV1(Path(v1_paths[controller]))
    if v2_path:
        # v2 (unified) - check given controller is supported in cgroup v2
        cgroup_v2 = CgroupCoreV2(Path(v2_path))
        if cgroup_v2.is_controller_supported(controller):
            return cgroup_v2
    # No cgroup mount of the requested controller
    return None


def get_cgroup_mount_checked(controller: str, resolve_host_root_links: bool = True) -> CgroupCore:
    cgroup_mount = get_cgroup_mount(controller)
    assert cgroup_mount is not None, f"Could not find cgroup mount point for controller {controller!r}"
    return cgroup_mount


def create_cgroup_from_path(controller: str, cgroup_path: Path) -> CgroupCore:
    cgroup_mount = get_cgroup_mount_checked(controller)

    try:
        cgroup_path.relative_to(cgroup_mount.path)
    except ValueError:
        cgroup_path = cgroup_mount.path / cgroup_path

    if isinstance(cgroup_mount, CgroupCoreV1):
        return CgroupCoreV1(cgroup_path)
    else:
        return CgroupCoreV2(cgroup_path)


def get_current_process_cgroup(controller: str) -> CgroupCore:
    cgroup_mount = get_cgroup_mount_checked(controller)
    is_v1 = isinstance(cgroup_mount, CgroupCoreV1)

    for process_cgroup in get_process_cgroups():
        if is_v1 and controller in process_cgroup.controllers:
            return CgroupCoreV1(cgroup_mount.path / process_cgroup.relative_path[1:])
        elif not is_v1 and process_cgroup.hier_id == "0":
            return CgroupCoreV2(cgroup_mount.path / process_cgroup.relative_path[1:])
    raise Exception(f"{controller!r} not found")

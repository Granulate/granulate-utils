#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from __future__ import annotations

import os
from abc import abstractmethod
from dataclasses import dataclass
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


@dataclass
class ProcCgroup:
    hier_id: str
    cgroup_relative_path: str
    controllers: List[str]


def get_process_cgroups(process: Optional[psutil.Process] = None) -> List[ProcCgroup]:
    """
    Get the cgroups of a process in [(hier id., controllers, path)] parsed form.
    If process is None, gets the cgroups of the current process.
    """

    def parse_line(line: str) -> ProcCgroup:
        hier_id, controller_list, cgroup_path = line.split(":", maxsplit=2)
        return ProcCgroup(hier_id, cgroup_path, controller_list.split(","))

    process = process or psutil.Process()
    text = read_proc_file(process, "cgroup").decode()
    return [parse_line(line) for line in text.splitlines()]


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
        cgroup_index = self.path.parts.index(cgroup_name)
        if cgroup_index != -1:
            return Path(*self.path.parts[: cgroup_index + 1])
        return None

    def _create_subcgroup(self, cgroup_name: str) -> Optional[Path]:
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
        controller_path_parts = self.path.parts
        return cgroup_name in controller_path_parts

    def get_pids_in_cgroup(self) -> set[int]:
        return {int(proc) for proc in self.read_from_interface_file(CGROUP_PROCS_FILE).split()}

    def assign_process_to_cgroup(self, pid: int = 0) -> None:
        if not self.path.exists():
            raise FileNotFoundError("Cgroup doesn't exist")

        self.write_to_interface_file(CGROUP_PROCS_FILE, str(pid))

    def read_from_interface_file(self, interface_name: str) -> str:
        interface_path = self.path / interface_name
        return interface_path.read_text()

    def write_to_interface_file(self, interface_name: str, data: str) -> None:
        interface_path = self.path / interface_name
        interface_path.write_text(data)

    @abstractmethod
    def create_subcgroup(self, controller: str, cgroup_name: str) -> CgroupCore:
        """
        Create a new CGroup lower in the hierarchy.
        If a cgroup with the given name is already in hierarchy, return its path.
        """
        pass


class CgroupCoreV1(CgroupCore):
    def create_subcgroup(self, controller: str, cgroup_name: str) -> CgroupCore:
        subcgroup_path = self._create_subcgroup(cgroup_name)
        assert subcgroup_path is not None
        return CgroupCoreV1(subcgroup_path)


CGROUP_SUPPORTED_CONTROLLERS = "cgroup.controllers"
CGROUP_ENABLED_CONTROLLERS = "cgroup.subtree_control"


class CgroupCoreV2(CgroupCore):
    def is_controller_supported(self, controller: str):
        return controller in self.read_from_interface_file(CGROUP_SUPPORTED_CONTROLLERS).split()

    def enable_controller(self, controller: str):
        assert self.is_controller_supported(controller), "Controller not supported"
        enabled_controllers_file = self.path / CGROUP_ENABLED_CONTROLLERS
        enabled_controllers_file.write_text(f"+{controller}{os.linesep}")

    def disable_controller(self, controller: str):
        enabled_controllers_file = self.path / CGROUP_ENABLED_CONTROLLERS
        enabled_controllers_file.write_text(f"-{controller}{os.linesep}")

    def create_subcgroup(self, controller: str, cgroup_name: str) -> CgroupCore:
        subcgroup_path = self._create_subcgroup(cgroup_name)
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


def create_cgroup_from_path(controller: str, cgroup_path: Path) -> CgroupCore:
    cgroup_mount = get_cgroup_mount(controller)
    assert cgroup_mount is not None, "Could not find cgroup mount point"

    try:
        cgroup_path.relative_to(cgroup_mount.path)
    except ValueError:
        cgroup_path = cgroup_mount.path / cgroup_path

    if isinstance(cgroup_mount, CgroupCoreV1):
        return CgroupCoreV1(cgroup_path)
    else:
        return CgroupCoreV2(cgroup_path)


def get_current_process_cgroup(controller: str) -> CgroupCore:
    cgroup_mount = get_cgroup_mount(controller)
    assert cgroup_mount is not None, "Could not find cgroup mount point"
    is_v1 = isinstance(cgroup_mount, CgroupCoreV1)

    for process_cgroup in get_process_cgroups():
        if is_v1 and controller in process_cgroup.controllers:
            return CgroupCoreV1(cgroup_mount.path / process_cgroup.cgroup_relative_path[1:])
        elif not is_v1 and process_cgroup.hier_id == "0":
            return CgroupCoreV2(cgroup_mount.path / process_cgroup.cgroup_relative_path[1:])
    raise Exception(f"{controller!r} not found")

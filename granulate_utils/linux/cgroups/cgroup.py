#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from pathlib import Path
from typing import List, Mapping, Optional, Tuple

import psutil

from granulate_utils.linux import ns
from granulate_utils.linux.mountinfo import iter_mountinfo
from granulate_utils.linux.process import read_proc_file

SUBSYSTEMS = {
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


def get_cgroups(process: Optional[psutil.Process] = None) -> List[Tuple[str, List[str], str]]:
    """
    Get the cgroups of a process in [(hier id., controllers, path)] parsed form.
    If process is None, gets the cgroups of the current process.
    """

    def parse_line(line: str) -> Tuple[str, List[str], str]:
        hier_id, controller_list, cgroup_path = line.split(":", maxsplit=2)
        return hier_id, controller_list.split(","), cgroup_path

    process = process or psutil.Process()
    text = read_proc_file(process, "cgroup").decode()
    return [parse_line(line) for line in text.splitlines()]


def find_v1_hierarchies(resolve_host_root_links: bool = True) -> Mapping[str, str]:
    """
    Finds all the mounted hierarchies for all currently enabled cgroup v1 controllers.
    :return: A mapping from cgroup subsystem names to their respective hierarchies.
    """
    hierarchies = {}
    for mount in iter_mountinfo(1):
        # The mount source is always "cgroup" by convention, we don't really have to check it.
        if mount.mount_source != "cgroup" or mount.filesystem_type != "cgroup":
            continue
        controllers = set(mount.super_options) & SUBSYSTEMS
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


def get_cgroup_mount(controller: str, resolve_host_root_links: bool = True) -> Optional[str]:
    """
    Returns the folder that the requested controller is mounted to, or None if no such controller mount was found
    If no v1 mount was found for the requested controller and there is a v2 mount (either unified or hybrid),
    the v2 mountpoint is returned (as all controllers share the same hierarchy in v2)
    """
    v1_paths = find_v1_hierarchies(resolve_host_root_links)
    v2_path = find_v2_hierarchy(resolve_host_root_links)
    if v1_paths and controller in v1_paths:
        # Either v1 or hybrid with the requested controller bound to a v1 hierarchy
        return v1_paths[controller]
    if v2_path:
        # v2 (unified) - all controllers are in a single unified hierarchy
        return v2_path
    # No cgroup mount of the requested controller
    return None


def is_known_controller(controller: str) -> bool:
    return controller in SUBSYSTEMS


def get_cgroup_relative_path(subsystem: str) -> str:
    """
    Get the subsystem cgroup path relative to the cgroup sysfs path
    :param subsystem: the subsystem
    :return: the relative path, with a '/' prefix
    """
    hierarchy_details = get_cgroups()
    for line in hierarchy_details:
        if subsystem in line[1]:
            return line[2]
    raise Exception(f"{subsystem!r} not found")


class CgroupUtils:
    _v1_hierarchies: Optional[Mapping[str, str]] = None

    @classmethod
    def get_cgroup_hierarchies(cls) -> Mapping[str, str]:
        if cls._v1_hierarchies is None:
            cls._v1_hierarchies = find_v1_hierarchies()
        return cls._v1_hierarchies

    @classmethod
    def get_current_cgroup_path(cls, subsystem: str) -> Path:
        """
        Get the full path of the subsystem cgroup
        :param subsystem: the subsystem
        :return: full path
        """
        cgroup_sysfs_path = cls.get_cgroup_hierarchies()[subsystem]
        cgroup_relative_path = get_cgroup_relative_path(subsystem)[1:]
        return Path(cgroup_sysfs_path) / cgroup_relative_path

    @classmethod
    def is_in_cgroup_hierarchy(cls, cgroup_path: Path, cgroup_name: str) -> bool:
        return cgroup_name in cgroup_path.parts

    @classmethod
    def is_in_current_cgroup_hierarchy(cls, subsystem: str, cgroup_name: str) -> bool:
        """
        Check if current process has the given cgroup_name in its hierarchy for the subsystem
        """
        current_cgroup_path = cls.get_current_cgroup_path(subsystem)
        return cls.is_in_cgroup_hierarchy(current_cgroup_path, cgroup_name)

    @classmethod
    def create_subcgroup(cls, subsystem: str, cgroup_name: str, parent_cgroup_path: Optional[Path] = None) -> Path:
        """
        Create a new sub-CGroup under another Cgroup.
        If cgroup_name is already in parent_cgroup hierarchy, return its path.
        :param parent_cgroup_path: If None, use current process cgroup for passed subsystem
        :return: The path for the created/found Cgroup
        """
        if parent_cgroup_path is None:
            parent_cgroup_path = cls.get_current_cgroup_path(subsystem)
        if cls.is_in_cgroup_hierarchy(parent_cgroup_path, cgroup_name):
            subsystem_index = parent_cgroup_path.parts.index(cgroup_name)
            new_cgroup_path = Path(*parent_cgroup_path.parts[: subsystem_index + 1])
        else:
            new_cgroup_path = parent_cgroup_path / cgroup_name
            new_cgroup_path.mkdir(exist_ok=True)
        return new_cgroup_path

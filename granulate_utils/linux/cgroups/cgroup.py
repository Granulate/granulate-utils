#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

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


def find_v1_hierarchies() -> Mapping[str, str]:
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
            hierarchy = ns.resolve_host_root_links(mount.mount_point)
            for controller in controllers:
                hierarchies[controller] = hierarchy
    return hierarchies


def find_v2_hierarchy() -> Optional[str]:
    """
    Finds the mounted unified hierarchy for cgroup v2 controllers.
    """
    cgroup2_mounts = [mount for mount in iter_mountinfo(1) if mount.filesystem_type == "cgroup2"]
    if not cgroup2_mounts:
        return None
    if len(cgroup2_mounts) > 1:
        raise Exception("More than one cgroup2 mount found!")
    return ns.resolve_host_root_links(cgroup2_mounts[0].mount_point)

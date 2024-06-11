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

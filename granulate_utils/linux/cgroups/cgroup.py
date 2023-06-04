#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import List, Literal, Mapping, Optional, Union

import psutil
from psutil import Process

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


def find_v1_hierarchies() -> Mapping[str, str]:
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
    path = cgroup2_mounts[0].mount_point
    path = ns.resolve_host_root_links(path)
    return path


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

    def __init__(self, cgroup_abs_path: Path, cgroup_mount_path: Path):
        self.cgroup_abs_path = cgroup_abs_path
        self.cgroup_mount_path = cgroup_mount_path

    def _create_subcgroup(self, cgroup_name: str) -> Path:
        new_cgroup_path = self.cgroup_abs_path / cgroup_name
        new_cgroup_path.mkdir(exist_ok=True)
        return new_cgroup_path

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
        return interface_path.read_text()

    def write_to_interface_file(self, interface_name: str, data: str) -> None:
        interface_path = self.cgroup_abs_path / interface_name
        interface_path.write_text(data)

    @abstractmethod
    def get_subcgroup(self, controller: ControllerType, cgroup_name: str) -> CgroupCore:
        """
        Return a CGroup which has the given name.
        If current cgroup is of a different name, create a subcgroup with the given name.
        """
        pass

    @property
    def is_v1(self) -> bool:
        return False

    @property
    def is_v2(self) -> bool:
        return False

    @classmethod
    def convert_outer_value_to_inner(cls, val: int) -> str:
        return str(val)

    @classmethod
    def convert_inner_value_to_outer(cls, val: str) -> int:
        return int(val)


class CgroupCoreV1(CgroupCore):
    def get_subcgroup(self, controller: ControllerType, cgroup_name: str) -> CgroupCore:
        assert controller in CONTROLLERS

        if self.cgroup_abs_path.name == cgroup_name:
            return self

        return CgroupCoreV1(self._create_subcgroup(cgroup_name), self.cgroup_mount_path)

    @property
    def is_v1(self) -> bool:
        return True


CGROUP_V2_SUPPORTED_CONTROLLERS = "cgroup.controllers"
CGROUP_V2_DELEGATED_CONTROLLERS = "cgroup.subtree_control"


class CgroupCoreV2(CgroupCore):
    def is_controller_supported(self, controller: ControllerType):
        return controller in self.read_from_interface_file(CGROUP_V2_SUPPORTED_CONTROLLERS).split()

    def _delegate_controller(self, controller: ControllerType) -> None:
        if self.is_controller_delegated(controller):
            return

        assert self.is_controller_supported(
            controller
        ), f"Controller '{controller}' is not supported under {self.cgroup_abs_path}"
        self.write_to_interface_file(CGROUP_V2_DELEGATED_CONTROLLERS, f"+{controller}")

    def is_controller_delegated(self, controller: ControllerType):
        return controller in self.read_from_interface_file(CGROUP_V2_DELEGATED_CONTROLLERS).split()

    def _get_parent_cgroup_for_controller(self, controller: ControllerType) -> CgroupCoreV2:
        if self.cgroup_abs_path != self.cgroup_mount_path:
            parent_cgroup = CgroupCoreV2(self.cgroup_abs_path.parent, self.cgroup_mount_path)
            # If controller can't be delegated - fallback to root cGroup
            if parent_cgroup.is_controller_supported(controller):
                return parent_cgroup
        return CgroupCoreV2(self.cgroup_mount_path, self.cgroup_mount_path)

    def get_subcgroup(self, controller: ControllerType, cgroup_name: str) -> CgroupCore:
        """
        Create a subcgroup. There are 2 cases:
        1. Current cGroup is the root cGroup - in this case, we create our cGroup under current cGroup.
        2. Current cGroup isn't the root cGroup - in this case, we create a cGroup under the parent of the
           current cGroup.
        """
        if self.cgroup_abs_path.name == cgroup_name:
            parent_cgroup = self._get_parent_cgroup_for_controller(controller)
            assert (
                parent_cgroup.cgroup_abs_path != self.cgroup_abs_path.parent
            ), "current cGroup doesn't support '{controller}' Controller"
            parent_cgroup._delegate_controller(controller)
            return self

        parent_cgroup = self._get_parent_cgroup_for_controller(controller)
        parent_cgroup._delegate_controller(controller)

        return CgroupCoreV2(parent_cgroup._create_subcgroup(cgroup_name), self.cgroup_mount_path)

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
    def is_v2(self) -> bool:
        return True


def get_cgroup_mount(controller: ControllerType) -> Optional[CgroupCore]:
    """
    Returns a CgroupCore object for requested controller, or None if no such controller mount was found.
    If no v1 mount was found for the requested controller and there is a v2 mount (either unified or hybrid),
    a CgroupCoreV2 is returned (as all controllers share the same hierarchy in v2)
    """
    v1_paths = find_v1_hierarchies()
    v2_path = find_v2_hierarchy()
    if controller in v1_paths:
        # Either v1 or hybrid with the requested controller bound to a v1 hierarchy
        return CgroupCoreV1(Path(v1_paths[controller]), Path(v1_paths[controller]))
    if v2_path:
        # v2 (unified) - check given controller is supported in cgroup v2
        cgroup_v2 = CgroupCoreV2(Path(v2_path), Path(v2_path))
        if cgroup_v2.is_controller_supported(controller):
            return cgroup_v2
    # No cgroup mount of the requested controller
    return None


def get_cgroup_mount_checked(controller: ControllerType) -> CgroupCore:
    cgroup_mount = get_cgroup_mount(controller)
    if cgroup_mount is None:
        raise CgroupControllerNotMounted(controller_name=controller)
    return cgroup_mount


def get_cgroup_from_path(controller: ControllerType, cgroup_path_or_full_path: Path) -> CgroupCore:
    cgroup_mount = get_cgroup_mount_checked(controller)

    try:
        cgroup_path_or_full_path.relative_to(cgroup_mount.cgroup_abs_path)
        # it's a full path
        cgroup_abs_path = cgroup_path_or_full_path
    except ValueError:
        cgroup_path_or_full_path = Path(cgroup_path_or_full_path.as_posix().lstrip("/"))
        # it's a path relative to controller mount point
        cgroup_abs_path = cgroup_mount.cgroup_abs_path / cgroup_path_or_full_path

    if cgroup_mount.is_v1:
        return CgroupCoreV1(cgroup_abs_path, cgroup_mount.cgroup_mount_path)
    else:
        return CgroupCoreV2(cgroup_abs_path, cgroup_mount.cgroup_mount_path)


def get_cgroup_for_process(controller: ControllerType, process: Optional[psutil.Process] = None) -> CgroupCore:
    """
    Get a CgrouopCore object for a given process. If process is None return for current process.
    """
    cgroup_mount = get_cgroup_mount_checked(controller)

    for process_cgroup in get_process_cgroups(process):
        if cgroup_mount.is_v1 and controller in process_cgroup.controllers:
            assert process_cgroup.hier_id != "0"
            return CgroupCoreV1(
                cgroup_mount.cgroup_abs_path / process_cgroup.relative_path.lstrip("/"), cgroup_mount.cgroup_mount_path
            )
        elif cgroup_mount.is_v2:
            assert process_cgroup.hier_id == "0"
            return CgroupCoreV2(
                cgroup_mount.cgroup_abs_path / process_cgroup.relative_path.lstrip("/"),
                cgroup_mount.cgroup_mount_path,
            )
    raise Exception(f"{controller!r} not found")


def get_cgroup_core(
    controller: ControllerType, cgroup: Optional[Union[Path, CgroupCore, Process]] = None
) -> CgroupCore:
    if cgroup is None or isinstance(cgroup, Process):
        # return current CgroupCore
        return get_cgroup_for_process(controller, cgroup)
    elif isinstance(cgroup, Path):
        # create CgroupCore from path
        cgroup = get_cgroup_from_path(controller, cgroup)

    assert isinstance(cgroup, CgroupCore), f"Unable to get CgroupCore from given input type {type(cgroup)}"
    return cgroup

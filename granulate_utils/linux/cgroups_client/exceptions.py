#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from pathlib import Path


class SkippedCgroup(Exception):
    def __init__(self, current_cgroup: str, new_cgroup_name: str) -> None:
        super().__init__(f"Skipping creating new {new_cgroup_name} cgroup since current cgroup is: {current_cgroup}")


class UnsupportedCgroup(Exception):
    def __init__(self, cgroup_type: str) -> None:
        super().__init__(f"cgroup {cgroup_type} is unsupported in current release")


class MissingCgroup(Exception):
    def __init__(self, cgroup_type: str, pid_cgroup_file: str) -> None:
        super().__init__(f"{cgroup_type} cgroup is missing from {pid_cgroup_file}")


class MissingController(Exception):
    def __init__(self, controller_path: Path) -> None:
        super().__init__(f"{controller_path.as_posix()} controller is missing")

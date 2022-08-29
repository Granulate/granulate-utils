#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from pathlib import Path


class SkippedCgroup(Exception):
    def __init__(self, current_cgroup: str, custom_cgroup: str) -> None:
        super().__init__(
            f"Skipping the creation of new {custom_cgroup!r} cgroup since the current cgroup is {current_cgroup!r}"
        )


class UnsupportedCgroup(Exception):
    def __init__(self, cgroup_type: str) -> None:
        super().__init__(f"cgroup {cgroup_type!r} is unsupported")


class MissingCgroup(Exception):
    def __init__(self, cgroup_type: str, pid_cgroup_file: Path) -> None:
        super().__init__(f"{cgroup_type!r} cgroup is missing from {pid_cgroup_file.as_posix()!r}")


class MissingController(Exception):
    def __init__(self, controller_path: Path) -> None:
        super().__init__(f"{controller_path.as_posix()} controller is missing")

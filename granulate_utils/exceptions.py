#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import signal
import subprocess
from typing import Any, List, Union


class UnsupportedNamespaceError(Exception):
    def __init__(self, nstype: str):
        super().__init__(f"Namespace {nstype!r} is not supported by this kernel")
        self.nstype = nstype


class CouldNotAcquireMutex(Exception):
    def __init__(self, name) -> None:
        super().__init__(f"Could not acquire mutex {name!r}. Another process might be holding it.")


class CriNotAvailableError(Exception):
    pass


class NoContainerRuntimesError(Exception):
    pass


class ContainerNotFound(Exception):
    def __init__(self, container_id: str) -> None:
        super().__init__(f"Could not find container with id {container_id!r}")


class ProcessStoppedException(Exception):
    pass


class CalledProcessError(subprocess.CalledProcessError):
    def __str__(self) -> str:
        if self.returncode and self.returncode < 0:
            try:
                base = f"Command '{self.cmd}' died with {signal.Signals(-self.returncode)!r}."
            except ValueError:
                base = f"Command '{self.cmd}' died with unknown signal {-self.returncode}."
        else:
            base = f"Command '{self.cmd}' returned non-zero exit status {self.returncode}. "
        return f"{base}\nstdout: {self.stdout}\nstderr: {self.stderr}"


class CalledProcessTimeoutError(CalledProcessError):
    def __init__(
        self, timeout: float, returncode: int, cmd: Union[str, List[str]], output: Any = str, stderr: Any = str
    ):
        super().__init__(returncode, cmd, output, stderr)
        self.timeout = timeout

    def __str__(self) -> str:
        return f"Timed out after {self.timeout} seconds\n" + super().__str__()

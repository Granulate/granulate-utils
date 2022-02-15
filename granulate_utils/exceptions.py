#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#


class UnsupportedNamespaceError(Exception):
    def __init__(self, nstype: str):
        super().__init__(f"Namespace {nstype!r} is not supported by this kernel")
        self.nstype = nstype


class CouldNotAcquireMutex(Exception):
    def __init__(self, name) -> None:
        super().__init__(f"Could not acquire mutex {name!r}. Another process might be holding it.")


class CRINotAvailable(Exception):
    pass

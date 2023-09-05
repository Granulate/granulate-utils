#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("granulate-utils")
except PackageNotFoundError:
    # package is not installed
    __version__ = "0.0.1"

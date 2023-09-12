#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from typing import Any


class ExtraException(Exception):
    """
    Derive from this exception to be able to pass **extra kwargs to the logger.
    """

    def __init__(self, *args: Any, **extra: Any) -> None:
        super().__init__(*args)
        self.extra = extra

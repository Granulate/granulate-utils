#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from typing import List

def bytes_to_gigabytes(value: float) -> float:
    return value / 1024 ** 3

def gigabytes_to_bytes(value: float) -> float:
    return value * 1024 ** 3

def split_and_filter(content: str) -> List[str]:
    return list(filter(None, content.split('\n')))
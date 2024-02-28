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

import re
from collections import namedtuple
from typing import Optional

from granulate_utils.linux import COMM_PATTERN

# see show_signal() for x86, e.g:
# "a[613450]: segfault at 0 ip 000056087e9aa136 sp 00007fffab66a9f0 error 6 in a[56087e9aa000+1000]"
SHOW_SIGNAL_X86 = re.compile(
    rf"(?:<\d>)?(?:\[(?P<timestamp>\d+\.\d+)\] )?(?:traps: )?(?P<comm>{COMM_PATTERN})\[(?P<pid>\d+)\]:?"
    r" (?P<desc>.*) ip(?::| )(?P<ip>[0-9a-f]+) sp(?::| )(?P<sp>[0-9a-f]+) error(?::| )"
    r"(?P<error>[0-9a-f]+)(?: in (?P<vma_info>.+\[[0-9a-f]+\+[0-9a-f]+\]))?"
)
# and arm64_show_signal() for Aarch64, e.g:
# "a[160760]: unhandled exception: DABT (lower EL), ESR 0x92000044, level 0 translation fault in a[aaaab0b60000+1000]"
SHOW_SIGNAL_AARCH64 = re.compile(
    rf"(?:<\d>)?(?:\[(?P<timestamp>\d+\.\d+)\] )?(?P<comm>{COMM_PATTERN})\[(?P<pid>\d+)\]:"
    r" unhandled exception: (?:(?P<desc>.*) )?in (?P<vma_info>.+\[[0-9a-f]+\+[0-9a-f]+\])"
)

SignalEntry = namedtuple("SignalEntry", "timestamp pid comm desc error_code vma_info")


def get_signal_entry(dmesg_line: str) -> Optional[SignalEntry]:
    m = SHOW_SIGNAL_X86.search(dmesg_line)
    if m is None:
        m = SHOW_SIGNAL_AARCH64.search(dmesg_line)

    if m is not None:
        d = m.groupdict()
        ts = d["timestamp"]
        return SignalEntry(
            float(ts) if ts is not None else None,
            int(d["pid"]),
            d["comm"],
            d["desc"],
            d.get("error"),
            d["vma_info"],
        )
    else:
        return None

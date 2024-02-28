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
import threading
from typing import List


class MessagesBuffer:
    """
    A list of strings limited by the total length of all items.
    Keeps count of current number of items, and dropped and added items.

    This class is threadsafe and uses a single reenterent lock
    which can be found in `self.lock`.
    """

    def __init__(self, max_total_length: int, overflow_drop_factor: float):
        assert max_total_length > 0, "max_total_length must be positive!"
        self.max_total_length = max_total_length  # maximum size of buffer in bytes
        self.overflow_drop_factor = overflow_drop_factor  # drop this percentage of messages upon overflow
        self.total_length = 0
        self.buffer: List[str] = []
        self.lengths: List[int] = []
        self.head_serial_no = 0
        self.dropped = 0

        self.lock = threading.RLock()

    @property
    def count(self) -> int:
        """Number of strings currently in the buffer."""
        with self.lock:
            return len(self.buffer)

    @property
    def utilized(self) -> float:
        """Total length used divided by maximum total length."""
        with self.lock:
            return self.total_length / self.max_total_length

    @property
    def next_serial_no(self) -> int:
        """The serial number of the next item to be inserted."""
        with self.lock:
            return self.head_serial_no + self.count

    def append(self, item: str) -> None:
        with self.lock:
            assert len(item) < self.max_total_length, "item is too long!"
            self.buffer.append(item)
            self.lengths.append(len(item))
            self.total_length += len(item)
            self._handle_overflow_locked()

    def _handle_overflow_locked(self) -> None:
        if self.total_length >= self.max_total_length:
            self.drop(max(1, int(self.overflow_drop_factor * self.count)))

    def drop(self, n: int):
        """
        Drop n messages from the buffer.
        :return: How many messages were actually dropped.
        """
        with self.lock:
            return self._drop_locked(n)

    def _drop_locked(self, n: int) -> int:
        assert n > 0, "n must be positive!"
        if self.count == 0:
            return 0
        if n > self.count:
            n = self.count
        self.head_serial_no += n
        self.dropped += n
        self.total_length -= sum(self.lengths[:n])
        del self.lengths[:n]
        del self.buffer[:n]
        return n

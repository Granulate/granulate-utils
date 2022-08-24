#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
from typing import List


class MessagesBuffer:
    """
    A list of strings limited by the total length of all items.
    Keeps count of current number of items, and dropped and added items.
    Not thread-safe on its own!
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

    @property
    def count(self) -> int:
        """Number of strings currently in the buffer."""
        return len(self.buffer)

    @property
    def utilized(self) -> float:
        """Total length used divided by maximum total length."""
        return self.total_length / self.max_total_length

    @property
    def next_serial_no(self) -> int:
        """The serial number of the next item to be inserted."""
        return self.head_serial_no + self.count

    def append(self, item: str) -> None:
        assert len(item) < self.max_total_length, "item is too long!"
        self.buffer.append(item)
        self.lengths.append(len(item))
        self.total_length += len(item)
        self.handle_overflow()

    def handle_overflow(self) -> None:
        if self.total_length >= self.max_total_length:
            dropped = self.drop(max(1, int(self.overflow_drop_factor * self.count)))
            self.dropped += dropped

    def drop(self, n: int) -> int:
        """
        Drop n messages from the buffer.
        :return: How many messages were actually dropped.
        """
        assert n > 0, "n must be positive!"
        if self.count == 0:
            return 0
        if n > self.count:
            n = self.count
        self.head_serial_no += n
        self.total_length -= sum(self.lengths[:n])
        del self.lengths[:n]
        del self.buffer[:n]
        return n

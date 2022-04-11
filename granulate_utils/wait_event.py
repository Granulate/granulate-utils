#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import time
from threading import Event
from typing import Callable

from granulate_utils.exceptions import StopEventSetException


def wait_event(timeout: float, stop_event: Event, condition: Callable[[], bool], interval: float = 0.1) -> None:
    end_time = time.monotonic() + timeout
    while True:
        if condition():
            break

        if stop_event.wait(interval):
            raise StopEventSetException()

        if time.monotonic() > end_time:
            raise TimeoutError()

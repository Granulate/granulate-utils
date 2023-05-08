#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, _base, as_completed
from contextlib import contextmanager
from typing import Any, Callable, Collection, Generator


@contextmanager
def wrap_thread_pool(pool: ThreadPoolExecutor) -> Generator[ThreadPoolExecutor, None, None]:
    try:
        yield pool
    except _base.TimeoutError as e:
        # Translate _base.TimeoutError to be a standard TimeoutError, this was only fixed in python 3.11
        # so we achieve here the same behavior for older python versions
        # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.TimeoutError
        raise TimeoutError(*e.args) from e
    finally:
        pool.shutdown(wait=False)


def call_in_parallel(
    callables: Collection[Callable[[], Any]], timeout: float, max_threads: int = 10
) -> Generator[Future, None, None]:
    """
    Call the given callables in parallel and generate their futures in correspondence to their finishing order.

    Note that the underlying threads are not daemonized and this function does not wait for all calls to finish,
    this means that if the caller does not iterate through all futures -
    some callables may continue executing in the background (potentially preventing the process from closing).
    Therefore it's important to make sure the callables themselves have timeouts.

    :raises TimeoutError: If __next__() is called and the result isn't available after timeout seconds
    from the call to call_in_parallel()
    """
    with wrap_thread_pool(ThreadPoolExecutor(max_workers=min(len(callables), max_threads))) as pool:
        futures = {pool.submit(callable) for callable in callables}
        for completed in as_completed(futures, timeout):
            yield completed

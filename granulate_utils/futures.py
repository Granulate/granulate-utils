from concurrent.futures import Future, ThreadPoolExecutor, _base, as_completed
from contextlib import contextmanager
from typing import Callable, Collection, Generator, TypeVar

T = TypeVar("T")


@contextmanager
def wrap_thread_pool(pool: ThreadPoolExecutor) -> Generator[ThreadPoolExecutor, None, None]:
    try:
        yield pool
    except _base.TimeoutError as e:
        raise TimeoutError(*e.args) from e
    finally:
        pool.shutdown(wait=False)


def call_in_parallel(
    callables: Collection[Callable[[], T]], timeout: float, max_threads: int = 10
) -> Generator[Future[T], None, None]:
    """
    Call the given callables in parallel and generate their futures in correspondence to their finishing order.

    Please note that the underlying threads are not daemonized and this function does not wait for all calls to finish,
    this means that if the caller does not invoke future.result() for all of the calls -
    the other callables will continue executing in the background (potentially preventing the process from closing).
    Therefore it's important to make sure the callables themselves have timeouts.
    """
    with wrap_thread_pool(ThreadPoolExecutor(max_workers=min(len(callables), max_threads))) as pool:
        futures = {pool.submit(callable) for callable in callables}
        for completed in as_completed(futures, timeout):
            yield completed

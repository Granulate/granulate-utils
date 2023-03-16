import time
import pytest

from granulate_utils.futures import call_in_parallel


def wait(seconds: int) -> int:
    time.sleep(seconds)
    return seconds


def test_return_first_result_sanity() -> None:
    assert 1 == next(call_in_parallel((lambda: wait(1), lambda: wait(2), lambda: wait(3)), timeout=5)).result()
    assert 1 == next(call_in_parallel((lambda: wait(3), lambda: wait(2), lambda: wait(1)), timeout=5)).result()
    for i, future in enumerate(call_in_parallel((lambda: wait(0), lambda: wait(1), lambda: wait(2)), timeout=5)):
        assert i == future.result()


def test_return_first_result_timeout() -> None:
    with pytest.raises(TimeoutError, match='futures unfinished'):
        next(call_in_parallel((lambda: wait(2), lambda: wait(2)), timeout=1)).result()


def test_return_first_result_exception_handling() -> None:
    def throwing_first() -> None:
        raise Exception('throwing')

    with pytest.raises(Exception, match='throwing'):
        next(call_in_parallel((throwing_first, lambda: wait(2)), timeout=5)).result()

    def throwing_last() -> None:
        time.sleep(2)
        raise Exception('throwing')

    assert 1 == next(call_in_parallel((throwing_last, lambda: wait(1)), timeout=5)).result()

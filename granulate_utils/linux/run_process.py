#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import logging
import os
import signal
import subprocess
import time
from functools import partial
from threading import Event
from typing import Any, Callable, List, Optional, Tuple, Union

from granulate_utils.exceptions import CalledProcessError, CalledProcessTimeoutError, ProcessStoppedException
from granulate_utils.linux.process import prctl

PR_SET_PDEATHSIG = 1

LoggerType = Union[logging.Logger, logging.LoggerAdapter]


def set_child_termination_on_parent_death(logger: LoggerType) -> None:
    try:
        prctl(PR_SET_PDEATHSIG, signal.SIGTERM)
    except OSError as e:
        logger.warning(f"Failed to set parent-death signal on child process. errno: {e.errno}, strerror: {e.strerror}")


def _wrap_callbacks(callbacks: List[Callable]) -> Callable:
    # Expects array of callback.
    # Returns one callback that call each one of them, and returns the retval of last callback
    def wrapper() -> Any:
        ret = None
        for cb in callbacks:
            ret = cb()

        return ret

    return wrapper


def start_process(
    cmd: List[str], logger: LoggerType, term_on_parent_death: bool = True, **kwargs: Any
) -> subprocess.Popen:
    logger.debug(f"Running command: ({' '.join(cmd)})")

    cur_preexec_fn = kwargs.pop("preexec_fn", os.setpgrp)
    if term_on_parent_death:
        cur_preexec_fn = _wrap_callbacks([partial(set_child_termination_on_parent_death, logger), cur_preexec_fn])

    popen = subprocess.Popen(
        cmd,
        stdout=kwargs.pop("stdout", subprocess.PIPE),
        stderr=kwargs.pop("stderr", subprocess.PIPE),
        stdin=subprocess.PIPE,
        preexec_fn=cur_preexec_fn,
        **kwargs,
    )
    return popen


def _reap_process(
    process: subprocess.Popen,
    kill_signal: signal.Signals,
    logger: LoggerType,
) -> Tuple[int, str, str]:
    # kill the process and read its output so far
    process.send_signal(kill_signal)
    process.wait()
    logger.debug(f"({process.args!r}) was killed by us with signal {kill_signal} due to timeout or stop request")
    stdout, stderr = process.communicate()
    returncode = process.poll()
    assert returncode is not None  # only None if child has not terminated
    return returncode, stdout, stderr


def run_process(
    cmd: List[str],
    logger: LoggerType,
    stop_event: Event = None,
    suppress_log: bool = False,
    check: bool = True,
    timeout: int = None,
    kill_signal: signal.Signals = signal.SIGKILL,
    communicate: bool = True,
    stdin: bytes = None,
    **kwargs: Any,
) -> "subprocess.CompletedProcess[bytes]":
    stdout = None
    stderr = None
    reraise_exc: Optional[BaseException] = None
    with start_process(cmd, logger, **kwargs) as process:
        try:
            communicate_kwargs = dict(input=stdin) if stdin is not None else {}
            if stop_event is None:
                assert timeout is None, f"expected no timeout, got {timeout!r}"
                if communicate:
                    # wait for stderr & stdout to be closed
                    stdout, stderr = process.communicate(timeout=timeout, **communicate_kwargs)
                else:
                    # just wait for the process to exit
                    process.wait()
            else:
                end_time = (time.monotonic() + timeout) if timeout is not None else None
                while True:
                    try:
                        if communicate:
                            stdout, stderr = process.communicate(timeout=1, **communicate_kwargs)
                        else:
                            process.wait(timeout=1)
                        break
                    except subprocess.TimeoutExpired:
                        if stop_event.is_set():
                            raise ProcessStoppedException from None
                        if end_time is not None and time.monotonic() > end_time:
                            assert timeout is not None
                            raise
        except subprocess.TimeoutExpired:
            returncode, stdout, stderr = _reap_process(process, kill_signal, logger)
            assert timeout is not None
            reraise_exc = CalledProcessTimeoutError(timeout, returncode, cmd, stdout, stderr)
        except BaseException as e:  # noqa
            returncode, stdout, stderr = _reap_process(process, kill_signal, logger)
            reraise_exc = e
        retcode = process.poll()
        assert retcode is not None  # only None if child has not terminated

    result: subprocess.CompletedProcess[bytes] = subprocess.CompletedProcess(process.args, retcode, stdout, stderr)

    logger.debug(f"({process.args!r}) exit code: {result.returncode}")
    if not suppress_log:
        if result.stdout:
            logger.debug(f"({process.args!r}) stdout: {result.stdout.decode()!r}")
        if result.stderr:
            logger.debug(f"({process.args!r}) stderr: {result.stderr.decode()!r}")
    if reraise_exc is not None:
        raise reraise_exc
    elif check and retcode != 0:
        raise CalledProcessError(retcode, process.args, output=stdout, stderr=stderr)
    return result

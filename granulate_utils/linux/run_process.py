#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import logging
import os
import signal
import subprocess
import time
from threading import Event
from typing import Any, Callable, List, Optional, Tuple, Union

from granulate_utils.exceptions import (
    CalledProcessError,
    CalledProcessTimeoutError,
    ProcessStoppedException,
)
from granulate_utils.linux.process import prctl
from granulate_utils.wait_event import wait_event

LoggerType = Union[logging.Logger, logging.LoggerAdapter]


def _wrap_callbacks(callbacks: List[Callable]) -> Callable:
    # Expects array of callback.
    # Returns one callback that call each one of them, and returns the retval of last callback
    def wrapper() -> Any:
        ret = None
        for cb in callbacks:
            ret = cb()

        return ret

    return wrapper


class RunProcess:
    def __init__(
        self,
        cmd: List[str],
        logger: LoggerType,
        stop_event: Event = None,
        suppress_log: bool = False,
        check: bool = True,
        timeout: int = None,
        kill_signal: signal.Signals = signal.SIGKILL,
        communicate: bool = True,
        stdin: bytes = None,
    ) -> None:
        self.cmd = cmd
        self.logger = logger
        self.stop_event = stop_event
        self.suppress_log = suppress_log
        self.check = check
        self.timeout = timeout
        self.kill_signal = kill_signal
        self.communicate = communicate
        self.stdin = stdin

        self.process: Optional[subprocess.Popen] = None

    def start(self, term_on_parent_death: bool = True, **popen_kwargs: Any) -> subprocess.Popen:
        assert self.process is None, "process already started!"
        self.logger.debug(f"Running command: ({' '.join(self.cmd)})")

        cur_preexec_fn = popen_kwargs.pop("preexec_fn", os.setpgrp)
        if term_on_parent_death:
            cur_preexec_fn = _wrap_callbacks([self._set_child_termination_on_parent_death, cur_preexec_fn])

        self.process = subprocess.Popen(
            self.cmd,
            stdout=popen_kwargs.pop("stdout", subprocess.PIPE),
            stderr=popen_kwargs.pop("stderr", subprocess.PIPE),
            stdin=subprocess.PIPE,
            preexec_fn=cur_preexec_fn,
            **popen_kwargs,
        )
        return self.process

    def _set_child_termination_on_parent_death(self) -> None:
        PR_SET_PDEATHSIG = 1
        try:
            prctl(PR_SET_PDEATHSIG, signal.SIGTERM)
        except OSError as e:
            self.logger.warning(
                f"Failed to set parent-death signal on child process. errno: {e.errno}, strerror: {e.strerror}"
            )

    def reap(self, signal: signal.Signals = None) -> int:
        assert self.process is not None, "process not started!"
        # kill the process and read its output so far
        self.process.send_signal(self.kill_signal if signal is None else signal)
        self.process.wait()
        self.logger.debug(
            f"({self.process.args!r}) was killed by us with signal {self.kill_signal} due to timeout or stop request"
        )
        return self.process.poll()

    def reap_and_read_output(
        self,
    ) -> Tuple[int, str, str]:
        returncode = self.reap()
        stdout, stderr = self.process.communicate()
        assert returncode is not None  # only None if child has not terminated
        return returncode, stdout, stderr

    def run(self) -> "subprocess.CompletedProcess[bytes]":
        stdout = None
        stderr = None
        reraise_exc: Optional[BaseException] = None
        with self.start():
            assert self.process is not None, "process was not started?"
            try:
                communicate_kwargs = dict(input=self.stdin) if self.stdin is not None else {}
                if self.stop_event is None:
                    assert self.timeout is None, f"expected no timeout, got {self.timeout!r}"
                    if self.communicate:
                        # wait for stderr & stdout to be closed
                        stdout, stderr = self.process.communicate(timeout=self.timeout, **communicate_kwargs)
                    else:
                        # just wait for the process to exit
                        self.process.wait()
                else:
                    end_time = (time.monotonic() + self.timeout) if self.timeout is not None else None
                    while True:
                        try:
                            if self.communicate:
                                stdout, stderr = self.process.communicate(timeout=1, **communicate_kwargs)
                            else:
                                self.process.wait(timeout=1)
                            break
                        except subprocess.TimeoutExpired:
                            if self.stop_event.is_set():
                                raise ProcessStoppedException from None
                            if end_time is not None and time.monotonic() > end_time:
                                assert self.timeout is not None
                                raise
            except subprocess.TimeoutExpired:
                returncode, stdout, stderr = self.reap_and_read_output()
                assert self.timeout is not None
                reraise_exc = CalledProcessTimeoutError(self.timeout, returncode, self.cmd, stdout, stderr)
            except BaseException as e:  # noqa
                returncode, stdout, stderr = self.reap_and_read_output()
                reraise_exc = e
            retcode = self.process.poll()
            assert retcode is not None  # only None if child has not terminated

        result: subprocess.CompletedProcess[bytes] = subprocess.CompletedProcess(
            self.process.args, retcode, stdout, stderr
        )

        self.logger.debug(f"({self.process.args!r}) exit code: {result.returncode}")
        if not self.suppress_log:
            if result.stdout:
                self.logger.debug(f"({self.process.args!r}) stdout: {result.stdout.decode()!r}")
            if result.stderr:
                self.logger.debug(f"({self.process.args!r}) stderr: {result.stderr.decode()!r}")
        if reraise_exc is not None:
            raise reraise_exc
        elif self.check and retcode != 0:
            raise CalledProcessError(retcode, self.process.args, output=stdout, stderr=stderr)
        return result

    def poll(self, timeout: float) -> None:
        assert self.stop_event is not None, "stop_event must be set to use this function!"
        assert self.process is not None, "process was not started?"
        process = self.process  # helps mypy
        wait_event(timeout, self.stop_event, lambda: process.poll() is not None)

    def get_popen(self) -> subprocess.Popen:
        return self.process


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
) -> "subprocess.CompletedProcess[bytes]":
    return RunProcess(cmd, logger, stop_event, suppress_log, check, timeout, kill_signal, communicate, stdin).run()

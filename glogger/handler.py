#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import logging
import time
import traceback
from datetime import datetime
from json import JSONEncoder
from logging import Handler, LogRecord
from typing import Any, Dict

from glogger.messages_buffer import MessagesBuffer
from glogger.sender import Sender

from .stdout_logger import get_stdout_logger


class BatchRequestsHandler(Handler):
    """
    logging.Handler that accumulates logs in a buffer and flushes them periodically or when
    threshold is reached. The logs are transformed into dicts which are sent as JSON to the server.
    Requests to the server are serialized in a dedicated thread to reduce congestion.
    Once a handler is created, it immediately starts flushing any log messages asynchronously.
    The handler can be stopped using `stop()`. Once stopped, it cannot be restarted.
    """

    TRUNCATED_KEY = "truncated"
    EXCEPTION_KEY = "exception"
    EXTRA_KEY = "extra"
    TEXT_KEY = "text"
    MESSAGE_KEY = "message"
    SERIAL_NO_KEY = "serial_no"

    def __init__(
        self,
        sender: Sender,
        *,
        continue_from: int = 0,
        max_message_size: int = 1 * 1024 * 1024,  # 1mb
        max_total_length: int = 5 * 1024 * 1024,  # 5mb,
        overflow_drop_factor: float = 0.25,
    ):
        """
        Create a new BatchRequestsHandler and start flushing messages in the background.

        :param sender: The sender to use for this handler, already initialized

        :param continue_from: Will be used as starting serial number.
        :param max_message_size: Upper limit on length of single message in bytes.
        :param max_total_length: Upper limit on total length of all buffered messages in bytes.
        :param overflow_drop_factor: Percentage of messages to be dropped when buffer becomes full.
        """
        super().__init__(logging.DEBUG)
        self.sender = sender
        self.max_message_size = max_message_size  # maximum message size

        self.stdout_logger = get_stdout_logger()
        self.jsonify = JSONEncoder(separators=(",", ":")).encode  # compact, no whitespace
        self.messages_buffer = MessagesBuffer(max_total_length, overflow_drop_factor)
        self.messages_buffer.head_serial_no = continue_from

        self.sender.start(self.messages_buffer, self.get_metadata)

    def emit(self, record: LogRecord) -> None:
        self.messages_buffer.append(self._format_record(record))

    def close(self) -> None:
        self.sender.stop()
        super().close()

    def __del__(self) -> None:
        self.close()

    def get_metadata(self) -> Dict:
        """Called to get metadata per batch."""
        return {}

    def _format_record(self, record: LogRecord) -> str:
        extra = self.get_extra_fields(record)
        with self.messages_buffer.lock:
            next_serial_no = self.messages_buffer.next_serial_no

        dict = {
            "severity": self._levelno_to_severity(record.levelno),
            "timestamp": time.time() * 1000,  # pass in milleseconds
            self.TEXT_KEY: {
                "logger_name": record.name,
                self.SERIAL_NO_KEY: next_serial_no,
                self.MESSAGE_KEY: record.getMessage(),
                self.TRUNCATED_KEY: False,
                "lineno": record.lineno,
                "pathname": record.pathname,
                "funcname": record.funcName,
                "thread": record.thread,
                "timestamp": datetime.utcfromtimestamp(record.created).isoformat(),
                self.EXCEPTION_KEY: self._get_exception_traceback(record),
                self.EXTRA_KEY: extra,
            },
        }

        try:
            result = self.jsonify(dict)
        except TypeError:
            # We don't want to fail a record because of extra serialization, so we test if
            # the extra was the problem and try again if not.
            self.stdout_logger.exception(f"Can't serialize extra (extra={extra!r}), sending empty extra")

            dict[self.EXTRA_KEY] = {self.TRUNCATED_KEY: True}
            result = self.jsonify(dict)

        return self._truncate_dict(dict, result)

    def _get_exception_traceback(self, record: LogRecord) -> str:
        if record.exc_text:
            # Use cached exc_text if available.
            return record.exc_text
        elif record.exc_info:
            return self._format_exception(record.exc_info)
        return ""

    def _format_exception(self, exc_info) -> str:
        return "\n".join(traceback.format_exception(*exc_info))

    def _levelno_to_severity(self, levelno: int):
        # From Python Side
        #   50 - CRITICAL, 40 - ERROR, 30 - WARNING, 20 - INFO, 10 - DEBUG, 0 - NOSET
        # From gLogger Side
        #   1 – Debug, 2 – Verbose, 3 – Info, 4 – Warn, 5 – Error, 6 – Critical
        if levelno >= 50:
            return 6
        elif levelno >= 40:
            return 5
        elif levelno >= 30:
            return 4
        elif levelno >= 20:
            return 3
        elif levelno >= 10:
            return 2
        return 1

    def get_extra_fields(self, record: LogRecord) -> dict:
        """
        Override to add extra fields to formatted record.
        Default implementation returns `record.extra` if present.
        """
        return record.__dict__.get("extra", {})

    def _truncate_dict(self, dict: Dict[str, Any], dict_str: str = None) -> str:
        if dict_str is None:
            dict_str = self.jsonify(dict)

        if len(dict_str) < self.max_message_size:
            return dict_str

        # We keep the logic simple, first try to remove exception, then try extra, then try message
        dict[self.TEXT_KEY][self.TRUNCATED_KEY] = True
        for key in [self.EXCEPTION_KEY, self.EXTRA_KEY, self.MESSAGE_KEY]:
            if key in dict[self.TEXT_KEY]:
                dict[self.TEXT_KEY].pop(key)
                return self._truncate_dict(dict)

        # If this is not enough, return constant message that will definitely successed and indicate issue
        new_text = {
            self.SERIAL_NO_KEY: dict[self.TEXT_KEY][self.SERIAL_NO_KEY],
            self.TRUNCATED_KEY: True,
        }
        dict[self.TEXT_KEY] = new_text
        return self.jsonify(dict)

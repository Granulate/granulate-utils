#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import gzip
import json
import logging
import threading
import time
import uuid
from datetime import datetime
from logging import Handler, LogRecord
from typing import List, NamedTuple, Tuple, Union

import backoff
import requests
from requests import RequestException

from granulate_utils.glogger.messages_buffer import MessagesBuffer

SERVER_SEND_ERROR_MESSAGE = "Error posting to server"

logger = logging.getLogger(__name__)


class Batch(NamedTuple):
    """A collection of messages sent to the server."""

    ident: str
    logs: List[str]
    size: int
    head_serial_no: int
    lost: int


class BatchRequestsHandler(Handler):
    """
    logging.Handler that accumulates logs in a buffer and flushes them periodically or when
    threshold is reached. The logs are transformed into dicts. Requests to the server are serialized
    in a dedicated thread to reduce congestion on the server.
    """

    scheme = "https"

    # If Tuple[float, float], then the first value is connection-timeout and the second read-timeout.
    # See https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
    request_timeout: Union[float, Tuple[float, float]] = 1.5

    def __init__(
        self,
        application_name: str,
        auth_token: str,
        server_address: str,
        max_message_size=1 * 1024 * 1024,  # 1mb
        max_total_length=5 * 1024 * 1024,  # 5mb
        flush_interval=10.0,
        flush_threshold=0.8,
        overflow_drop_factor=0.25,
        max_send_tries=5,
    ):
        super().__init__(logging.DEBUG)
        self.max_message_size = max_message_size  # maximum message size
        self.flush_interval = flush_interval  # maximum amount of seconds between flushes
        self.flush_threshold = flush_threshold  # force flush if buffer size reaches this percentage of capacity
        self.max_send_tries = max_send_tries  # maximum number of times to retry sending logs if request fails
        self.messages_buffer = MessagesBuffer(max_total_length, overflow_drop_factor)
        self.stop_event = threading.Event()
        self.time_fn = time.time
        self.server_address = server_address
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Application-Name": application_name,
                "X-Token": auth_token,
            }
        )
        self.last_flush_time = 0.0
        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True, name="Logs flusher")
        self.start()

    def start(self) -> None:
        """Called to start the flush thread."""
        self.flush_thread.start()

    def emit(self, record: LogRecord) -> None:
        # Called while lock is acquired
        item = self.dump_record_locked(record)
        self.messages_buffer.append(item)

    def dump_record_locked(self, record: LogRecord) -> str:
        # `format` is required to fill in some record members such as `message` and `asctime`.
        self.format(record)
        record_dict = self.dictify_record(record)
        if record.truncated:  # type: ignore
            record_dict["truncated"] = True
        record_dict["serial_no"] = self.messages_buffer.next_serial_no
        return json.dumps(record_dict)

    def format(self, record: LogRecord) -> str:
        """Override to customize record fields."""
        s = super().format(record)
        if len(record.message) > self.max_message_size:
            record.message = record.message[: self.max_message_size]
            record.truncated = True  # type: ignore
        else:
            record.truncated = False  # type: ignore
        return s

    def dictify_record(self, record):
        formatted_timestamp = datetime.utcfromtimestamp(record.created).isoformat()
        return {
            "severity": record.levelno,
            "timestamp": formatted_timestamp,
            "logger_name": record.name,
            "message": record.message,
        }

    def _flush_loop(self) -> None:
        self.last_flush_time = self.time_fn()
        while not self.stop_event.is_set():
            if self.should_flush():
                self.flush()
            self.stop_event.wait(0.5)

    @property
    def time_since_last_flush(self) -> float:
        return self.time_fn() - self.last_flush_time

    def should_flush(self) -> bool:
        return (
            self.messages_buffer.count > 0
            and (self.messages_buffer.utilized >= self.flush_threshold)
            or (self.time_since_last_flush >= self.flush_interval)
        )

    def flush(self) -> None:
        # Allow configuring max_tries via class member. Alternatively we could decorate _send_logs in __init__ but that
        # would be easier to miss and less flexible.
        send = backoff.on_exception(backoff.expo, exception=RequestException, max_tries=self.max_send_tries)(
            self._send_logs
        )

        self.last_flush_time = self.time_fn()
        try:
            batch, response = send()
            response.raise_for_status()
        except Exception:
            logger.exception(SERVER_SEND_ERROR_MESSAGE)
        else:
            self.drop_sent_batch(batch)

    def drop_sent_batch(self, sent_batch: Batch) -> None:
        # we don't override createLock(), so lock is not None
        with self.lock:  # type: ignore
            dropped = self.messages_buffer.head_serial_no - sent_batch.head_serial_no
            n = len(sent_batch.logs) - dropped
            if n > 0:
                self.messages_buffer.drop(n)
            self.messages_buffer.lost -= sent_batch.lost

    def _send_logs(self) -> Tuple[Batch, requests.Response]:
        """
        :return: (batch that was sent, response)
        """
        # Upon every retry we will remake the batch, in case we are able to batch more messages together.
        batch = self.make_batch()
        batch_data = {
            "batch_id": batch.ident,
            "metadata": self.get_metadata(),
            "lost": batch.lost,
            "logs": "<LOGS_JSON>",
        }
        # batch.logs is a list of json.dump()ed strings so ",".join() it into the final json string instead of json-ing
        # the list.
        data = json.dumps(batch_data).replace('"<LOGS_JSON>"', f"[{','.join(batch.logs)}]").encode("utf-8")
        data = gzip.compress(data, compresslevel=6)
        response = self.session.post(
            f"{self.scheme}://{self.server_address}/api/v1/logs",
            data=data,
            headers={
                "Content-Encoding": "gzip",
                "Content-Type": "application/json",
            },
            timeout=self.request_timeout,
        )
        return batch, response

    def get_metadata(self) -> dict:
        """Called to get metadata per batch."""
        return {}

    def make_batch(self) -> Batch:
        # we don't override createLock(), so lock is not None
        with self.lock:  # type: ignore
            return Batch(
                uuid.uuid4().hex,
                self.messages_buffer.buffer[:],
                self.messages_buffer.total_length,
                self.messages_buffer.head_serial_no,
                self.messages_buffer.lost,
            )

    def stop(self, timeout: float = 60) -> bool:
        """
        Signals to stop flushing messages asynchronously.
        Blocks until current flushing operation has finished or `stop_timeout` seconds passed.
        :return: Whether timeout has been reached.
        """
        self.stop_event.set()
        self.flush_thread.join(timeout)
        return not self.flush_thread.is_alive()

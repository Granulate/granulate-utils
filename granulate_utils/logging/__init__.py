#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
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

REQUEST_MAX_TRIES = 5

logger = logging.getLogger(__name__)


class Batch(NamedTuple):
    """A collection of messages sent to the server."""

    logs: List[str]
    size: int
    head_serial_no: int


class MessagesBuffer:
    """
    A list of strings limited by the total length of all items.
    Keeps count of current number of items, and dropped and added items.
    Not thread-safe on its own!
    """

    def __init__(self, max_total_length: int, overflow_drop_factor: float):
        self.max_total_length = max_total_length
        self.overflow_drop_factor = overflow_drop_factor
        self.total_length = 0
        self.buffer: List[str] = []
        self.lengths: List[int] = []
        self.head_serial_no = 0
        self.next_serial_no = 0

    @property
    def count(self) -> int:
        return len(self.buffer)

    @property
    def utilization(self) -> float:
        return self.total_length / self.max_total_length

    def append(self, item):
        self.buffer.append(item)
        self.lengths.append(len(item))
        self.total_length += len(item)
        self.next_serial_no += 1
        self.check_overflow()

    def check_overflow(self) -> None:
        if self.total_length >= self.max_total_length:
            self.drop(int(self.overflow_drop_factor * self.count))

    def drop(self, n: int):
        assert n > 0, "n must be positive!"
        if self.count == 0:
            return
        if n > self.count:
            n = self.count
        self.head_serial_no += n
        self.total_length -= sum(self.lengths[:n])
        del self.lengths[:n]
        del self.buffer[:n]


class BatchRequestsHandler(Handler):
    """
    logging.Handler that accumulates logs in a buffer and flushes them periodically or when
    threshold is reached. The logs are transformed into dicts. Requests to the server are serialized
    in a dedicated thread to reduce congestion on the server.
    """

    scheme = "https"

    # If Tuple[float, float], then the first value is connection-timeout and the second read-timeout.
    # See https://docs.python-requests.org/en/latest/api/#requests.request
    request_timeout: Union[float, Tuple[float, float]] = 1.5

    def __init__(
        self,
        server_address: str,
        max_message_size=1 * 1024 * 1024,  # 1mb
        max_total_length=5 * 1024 * 1024,  # 5mb
        flush_interval=10.0,
        flush_threshold=0.8,
        overflow_drop_factor=0.25,
    ):
        super().__init__(logging.DEBUG)
        self.max_message_size = max_message_size  # maximum message size
        self.capacity = max_total_length  # maximum size of buffer in bytes
        self.flush_interval = flush_interval  # maximum amount of seconds between flushes
        self.flush_threshold = flush_threshold  # force flush if buffer size reaches this percentage of capacity
        self.overflow_drop_factor = overflow_drop_factor  # drop this percentage of messages upon overflow
        self.messages_buffer = MessagesBuffer(self.capacity, self.overflow_drop_factor)
        self.stop_event = threading.Event()
        self.time_fn = time.time
        self.server_address = server_address
        self.session = requests.Session()
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
            record.truncated = True
        else:
            record.truncated = False
        return s

    def dictify_record(self, record):
        formatted_timestamp = datetime.utcfromtimestamp(record.created).isoformat()
        return {
            "level": record.levelname,
            "timestamp": formatted_timestamp,
            "logger_name": record.name,
            "message": record.message,
        }

    def _flush_loop(self) -> None:
        self.last_flush_time = self.time_fn()
        while not self.stop_event.is_set():
            if self.should_flush():
                self.flush()
            self.stop_event.wait(1.0)

    @property
    def time_since_last_flush(self) -> float:
        return self.time_fn() - self.last_flush_time

    def should_flush(self) -> bool:
        return (
            self.messages_buffer.count > 0
            and (self.messages_buffer.utilization >= self.flush_threshold)
            or (self.time_since_last_flush >= self.flush_interval)
        )

    def flush(self) -> None:
        self.last_flush_time = self.time_fn()
        try:
            batch, response = self._send_logs()
            response.raise_for_status()
        except Exception:
            logger.exception("Error posting to server")
        else:
            self.drop_sent_batch(batch)

    def drop_sent_batch(self, sent_batch: Batch) -> None:
        # we don't override createLock(), so lock is not None
        with self.lock:  # type: ignore
            dropped = self.messages_buffer.head_serial_no - sent_batch.head_serial_no
            n = len(sent_batch.logs) - dropped
            if n > 0:
                self.messages_buffer.drop(n)

    @backoff.on_exception(backoff.expo, exception=RequestException, max_tries=REQUEST_MAX_TRIES)
    def _send_logs(self) -> Tuple[Batch, requests.Response]:
        """
        :return: (batch that was sent, response)
        """
        # Upon every retry we will remake the batch, in case we are able to batch more messages together.
        batch = self.make_batch()
        data = {
            "id": uuid.uuid4().hex,
            "metadata": self.get_metadata(),
            "logs": '<LOGS_JSON>',
        }
        # batch.logs is a list of strings so ",".join() it into the final json string instead of json-ing the list.
        body = json.dumps(data).replace('"<LOGS_JSON>"', f"[{','.join(batch.logs)}]")
        response = self.session.post(f"{self.scheme}://{self.server_address}/", data=body.encode('utf-8'),
                                     headers={'Content-Type': 'application/json'}, timeout=self.request_timeout)
        return batch, response

    def get_metadata(self) -> dict:
        """Called to get metadata per batch."""
        return {}

    def make_batch(self) -> Batch:
        # we don't override createLock(), so lock is not None
        with self.lock:  # type: ignore
            return Batch(
                self.messages_buffer.buffer[:], self.messages_buffer.total_length, self.messages_buffer.head_serial_no
            )

    def stop(self) -> bool:
        """
        Signals the communicator to stop receiving new messages.
        Blocks until the communicator has finished processing all messages in the queue (or a `timeout` is reached).
        """
        self.stop_event.set()
        self.flush_thread.join(60.0)
        return not self.flush_thread.is_alive()
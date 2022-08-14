#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import logging
import threading
import time
import uuid
from datetime import datetime
from logging import Handler, LogRecord
from typing import Any, Dict, List, Tuple, Union

import backoff
import requests
from requests import RequestException

REQUEST_MAX_TRIES = 5

logger = logging.getLogger(__name__)


class BatchRequestsHandler(Handler):
    """
    logging.Handler that accumulates logs in a buffer and flushes them periodically or when
    threshold is reached. The logs are transformed into dicts. Requests to the server are serialized
    in a dedicated thread to reduce congestion on the server.
    """

    scheme = "https"
    capacity = 100 * 1000  # max number of records to buffer
    flush_interval = 10.0  # maximum amount of seconds between flushes
    flush_threshold = 0.8  # force flush if buffer size reaches this percentage of capacity
    overflow_drop_factor = 1 - flush_threshold  # drop this percentage of capacity upon overflow

    # If Tuple[float, float], then the first value is connection-timeout and the second read-timeout.
    # See https://docs.python-requests.org/en/latest/api/#requests.request
    request_timeout: Union[float, Tuple[float, float]] = 1.5

    def __init__(self, server_address: str):
        super().__init__(logging.DEBUG)
        self.head_serial_no: int = -1
        self.next_serial_no: int = 0
        self.buffer: List[dict] = []
        self.server_address = server_address
        self.session = requests.Session()
        self.stop_event = threading.Event()
        self.time_fn = time.time
        self.last_flush_time = 0.0
        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True, name="Logs flusher")
        self.setup()
        self.flush_thread.start()

    def setup(self) -> None:
        """Called to perform setup before the flush thread is started."""
        pass

    def emit(self, record: LogRecord) -> None:
        # Called while lock is acquired
        item = self.dictify_record(record)
        self.buffer.append(item)
        self.next_serial_no += 1
        self.check_overflow()

    def check_overflow(self) -> None:
        if len(self.buffer) >= self.capacity:
            drop_count = int(self.overflow_drop_factor * self.capacity)
            del self.buffer[:drop_count]

    def dictify_record(self, record: LogRecord) -> Dict[str, Any]:
        # `format` is required to fill in some record members such as `message` and `asctime`.
        self.format(record)
        formatted_timestamp = datetime.utcfromtimestamp(record.created).isoformat()
        return {
            "serial_no": self.next_serial_no,
            "message": record.message,
            "level": record.levelname,
            "timestamp": formatted_timestamp,
            "logger_name": record.name,
        }

    def _flush_loop(self) -> None:
        self.last_flush_time = self.time_fn()
        while not self.stop_event.is_set():
            if self.should_flush():
                self._flush()
            self.stop_event.wait(1.0)

    def should_flush(self) -> bool:
        delta_time = self.time_fn() - self.last_flush_time
        return (len(self.buffer) >= self.flush_threshold * self.capacity) or (delta_time >= self.flush_interval)

    def _flush(self) -> None:
        self.last_flush_time = self.time_fn()
        try:
            response = self._send_logs()
            response.raise_for_status()
            last_ingested_serial_no = response.json()["last_ingested_serial_no"]
        except Exception:
            logger.error("Error posting to server")
        else:
            self.remove_up_to_serial_no(last_ingested_serial_no + 1)

    @backoff.on_exception(backoff.expo, exception=RequestException, max_tries=REQUEST_MAX_TRIES)
    def _send_logs(self) -> requests.Response:
        # Upon every retry we will remake the batch, in case we are able to batch more messages together.
        data = {
            "id": uuid.uuid4().hex,
            "metadata": self.get_metadata(),
            "logs": self.make_batch(),
        }
        return self.session.post(f"{self.scheme}://{self.server_address}/", json=data, timeout=self.request_timeout)

    def get_metadata(self) -> dict:
        """Called to get metadata per batch."""
        return {}

    def make_batch(self) -> list:
        self.acquire()
        batch = self.buffer[:]
        self.release()
        return batch

    def remove_up_to_serial_no(self, serial_no: int) -> None:
        self.acquire()
        n = serial_no - self.head_serial_no
        if n > 0:
            del self.buffer[:n]
            self.head_serial_no = serial_no
        self.release()

    def stop(self) -> bool:
        """
        Signals the communicator to stop receiving new messages.
        Blocks until the communicator has finished processing all messages in the queue (or a `timeout` is reached).
        """
        self.stop_event.set()
        self.flush_thread.join(60.0)
        return not self.flush_thread.is_alive()

#
# Copyright (C) 2023 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import gzip
import threading
import time
import uuid
from json import JSONEncoder
from typing import Callable, Dict, List, NamedTuple, Optional, Tuple, Union

import requests
from requests import Session
from requests.auth import HTTPBasicAuth

from glogger.messages_buffer import MessagesBuffer

from .stdout_logger import get_stdout_logger

SENDER_CONNECTION_ERROR_MESSAGE = "REMOTE_LOGGER: Failed establishing connection to logs server, check log server url"
SENDER_TIMEOUT_MESSAGE = "REMOTE_LOGGER: Timeout occurred while sending logs to server"
SENDER_UNAUTHORIZED_MESSAGE = "REMOTE_LOGGER: Authentication error while sending logs to server, check gprofiler token"
SENDER_UNKNOWN_HTTP_ERROR_MESSAGE = "REMOTE_LOGGER: Unexpected HTTP error while posting logs to server"
SENDER_UNKNOWN_ERROR_MESSAGE = "REMOTE_LOGGER: Unexpected error posting logs to server"


class SendBatch(NamedTuple):
    ident: str
    logs: List[str]
    size: int
    head_serial_no: int
    lost_logs_count: int


class AuthToken(str):
    pass


class BasicAuthCredentials(NamedTuple):
    username: str
    password: str


class Sender:
    # If Tuple[float, float], then the first value is connection-timeout and the second read-timeout.
    # See https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
    request_timeout: Union[float, Tuple[float, float]] = (1.5, 10)

    def __init__(
        self,
        application_name: str,
        server_address: str,
        *,
        auth: Union[AuthToken, BasicAuthCredentials] = None,
        scheme: str = "https",
        send_interval: float = 30.0,
        send_threshold: float = 0.8,
        send_min_interval: float = 10.0,
        max_send_tries: int = 3,
        verify: bool = True,
    ):
        """
        Create a new Sender and start flushing log messages in a background thread.

        :param application_name: Unique identifier requests coming from this handler.
        :param auth: The auth to use for this handler. One of AuthToken or BasicAuthCredentials.
        :param server_address: Address of server where to send messages.
        :param scheme: The scheme to use as string ('http' or 'https')
        :param send_interval: Seconds between sending batches.
        :param send_threshold: Force send when buffer utilization reaches this percentage.
        :param send_min_interval: The minimal interval between each sends.
        :param max_send_tries: Number of times to retry sending a batch if sending fails.
        """

        self.application_name = application_name
        self.send_interval = send_interval
        self.send_threshold = send_threshold
        self.send_min_interval = send_min_interval

        self.max_send_tries = max_send_tries
        self.stdout_logger = get_stdout_logger()
        self.set_address(server_address, scheme=scheme)
        self.jsonify = JSONEncoder(separators=(",", ":"), default=repr).encode  # compact, no whitespace
        self.session = Session()

        # Set up auth
        if isinstance(auth, BasicAuthCredentials):
            self.session.auth = HTTPBasicAuth(*auth)
        elif isinstance(auth, AuthToken):
            self.session.headers["X-Token"] = str(auth)

        self.session.verify = verify
        self.messages_buffer: Optional[MessagesBuffer] = None
        self.metadata_callback: Callable[[], Dict] = lambda: {}

    def set_address(self, server_address: str, *, scheme: str = "https") -> None:
        """
        Set the server address to send logs to.
        :param server_address: Address of server where to send messages.
        :param scheme: The scheme to use as string ('http' or 'https')
        """
        self.server_uri = f"{scheme}://{server_address}/api/v1/logs"

    def start(self, messages_buffer: MessagesBuffer, metadata_callback: Callable[[], Dict]) -> None:
        assert self.messages_buffer is None, "Call start once"
        self.messages_buffer = messages_buffer
        self.metadata_callback = metadata_callback

        self.last_send_time = 0.0
        self.stop_event = threading.Event()
        self.sending_thread = threading.Thread(target=self._send_loop, daemon=True, name="gLogger Logs Sending Thread")
        self.sending_thread.start()

    def stop(self, timeout: float = 10) -> bool:
        """
        Signals to stop flushing messages asynchronously.
        Blocks until current flushing operation has finished or `stop_timeout` seconds passed.
        :return: Whether thread terminated within allotted timeout.
        """
        if self.stop_event.is_set():
            return True
        else:
            self.stop_event.set()
            self.sending_thread.join(timeout)
            return not self.sending_thread.is_alive()

    def _send_loop(self) -> None:
        assert self.messages_buffer is not None

        self.last_send_time = time.monotonic()
        while not self.stop_event.is_set():
            if self._should_send():
                self.send()
            self.stop_event.wait(self.send_min_interval)

        # send all remaining messages before terminating:
        # Not thread-safe but we're fine with it as read only
        if self.messages_buffer.count > 0:
            self.send()

    def _should_send(self) -> bool:
        assert self.messages_buffer is not None

        time_since_last_send = time.monotonic() - self.last_send_time
        return self.messages_buffer.count > 0 and (
            (self.messages_buffer.utilized >= self.send_threshold) or (time_since_last_send >= self.send_interval)
        )

    def send(self) -> None:
        self.last_send_time = time.monotonic()
        try:
            batch = self._send_once()
            self._drop_sent_batch(batch)
        except requests.exceptions.ConnectionError:
            self.stdout_logger.error(SENDER_CONNECTION_ERROR_MESSAGE)
        except requests.exceptions.Timeout:
            self.stdout_logger.error(SENDER_TIMEOUT_MESSAGE)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 401:
                self.stdout_logger.error(SENDER_UNAUTHORIZED_MESSAGE)
            elif err.response.status_code == 500:
                self.stdout_logger.error(
                    f"REMOTE_LOGGER: Received 500 from server, gprofiler token is probably invalid / missing. "
                    f"error: {str(err)}"
                )
            else:
                self.stdout_logger.exception(SENDER_UNKNOWN_HTTP_ERROR_MESSAGE)
        except Exception:
            self.stdout_logger.exception(SENDER_UNKNOWN_ERROR_MESSAGE)

    def _drop_sent_batch(self, batch: SendBatch) -> None:
        assert self.messages_buffer is not None
        with self.messages_buffer.lock:
            # The previous lost count has been accounted by the server:
            self.messages_buffer.dropped -= batch.lost_logs_count

            # Number of messages dropped while we were busy flushing:
            dropped_inadvertently = self.messages_buffer.head_serial_no - batch.head_serial_no
            remaining = len(batch.logs) - dropped_inadvertently
            if remaining > 0:
                # Drop the remainder:
                self.messages_buffer.drop(remaining)

            # Account for all the messages in the batch that were considered dropped:
            self.messages_buffer.dropped -= len(batch.logs)

    def _send_once(self):
        batch = self._make_batch()
        protocol_data = {
            "batch_id": batch.ident,
            "metadata": self.metadata_callback(),
            "lost_logs_count": batch.lost_logs_count,
            "logs": "<LOGS_JSON>",
        }
        # batch.logs is a list of json strings so ','.join() it into the final json string instead of json-ing the list.
        data = self.jsonify(protocol_data).replace('"<LOGS_JSON>"', f'[{",".join(batch.logs)}]').encode("utf-8")
        self._send_once_to_server(data)

        return batch

    def _send_once_to_server(self, data: bytes) -> None:
        headers = {
            "Content-Encoding": "gzip",
            "Content-Type": "application/json",
            "X-Application-Name": self.application_name,
        }

        # Default compression level (9) is slowest. Level 6 trades a bit of compression for speed.
        data = gzip.compress(data, compresslevel=6)

        response = self.session.post(
            self.server_uri,
            data=data,
            headers=headers,
            timeout=self.request_timeout,
        )
        response.raise_for_status()

    def _make_batch(self) -> SendBatch:
        assert self.messages_buffer is not None
        with self.messages_buffer.lock:
            return SendBatch(
                uuid.uuid4().hex,
                self.messages_buffer.buffer[:],
                self.messages_buffer.total_length,
                self.messages_buffer.head_serial_no,
                # The current dropped counter indicates lost messages. Any messages dropped from now on will also be
                # considered dropped until proven to have been flushed successfully.
                self.messages_buffer.dropped,
            )

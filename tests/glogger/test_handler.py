#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import gzip
import json
import logging
import random
import time
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, HTTPServer
from logging import ERROR, WARNING
from threading import Thread

from requests.adapters import HTTPAdapter
from requests.models import PreparedRequest, Response
from requests.structures import CaseInsensitiveDict

from glogger.handler import SERVER_SEND_ERROR_MESSAGE, BatchRequestsHandler


class HttpBatchRequestsHandler(BatchRequestsHandler):
    scheme = "http"
    request_timeout = 0.2

    def __init__(self, *args, **kwargs):
        super().__init__("app", "token", *args, max_send_tries=1, **kwargs)


class GzipRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def parse_request(self) -> bool:
        v = super().parse_request()
        if v:
            length = int(self.headers.get("Content-Length", "0"))
            self.body = self.rfile.read(length)
            if self.headers.get("Content-Encoding") == "gzip":
                self.body = gzip.decompress(self.body)
        return v


class LogsServer(HTTPServer):
    timeout = 5.0
    disable_nagle_algorithm = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed = 0

    def process_request(self, request, client_address):
        super().process_request(request, client_address)
        self.processed += 1

    @property
    def authority(self):
        addr = self.server_address
        return f"{addr[0]}:{addr[1]}"


class MockAdapter(HTTPAdapter):
    def __init__(self):
        super().__init__()
        self.sends = 0
        self.errors = 0
        self.successes = 0

    def send(self, request: PreparedRequest, *args, **kwargs) -> Response:
        self.sends += 1

        response = Response()
        response.request = request
        response.url = request.url or ""
        response.status_code = 502
        response.reason = "Internal Server Error"
        response.headers = CaseInsensitiveDict()
        response.encoding = "utf-8"
        try:
            assert isinstance(request.body, bytes)
            json_data = json.loads(gzip.decompress(request.body))
            assert_serial_nos_ok([log["serial_no"] for log in json_data["logs"]])
        except Exception as e:
            response.status_code = 400
            response.reason = str(e)
            self.errors += 1
        else:
            response.status_code = 200
            response.reason = "OK"
            self.successes += 1
        finally:
            return response


def get_logger(handler):
    # use granulate_utils logger as parent so we also capture logs from within in the same handler.
    utils_logger = logging.getLogger("glogger")
    for h in utils_logger.handlers[:]:
        utils_logger.removeHandler(h)
        h.close()
    utils_logger.addHandler(handler)
    utils_logger.setLevel(10)
    logger = utils_logger.getChild(random.randbytes(8).hex())
    return logger


def assert_buffer_attributes(handler, **kwargs):
    mb = handler.messages_buffer
    for k in kwargs:
        assert getattr(mb, k) == kwargs[k]


def assert_serial_nos_ok(serial_nos):
    assert serial_nos == list(sorted(serial_nos)), "bad order!"
    assert len(set(serial_nos)) == len(serial_nos), "have duplicates!"


def test_max_buffer_size_lost_one():
    """Test total length limit works by checking that a record is dropped from the buffer when limit is reached."""
    with ExitStack() as exit_stack:
        # we don't need a real port for this one
        handler = HttpBatchRequestsHandler(
            "localhost:61234", max_total_length=4000, flush_interval=9999, flush_threshold=0.95
        )
        exit_stack.callback(handler.stop)

        logger = get_logger(handler)
        logger.info("A" * 1500)
        logger.info("A" * 1500)
        logger.info("A" * 1500)
        # Check that one message was dropped, and an additional warning message was added
        assert_buffer_attributes(handler, dropped=1)
        last_message = json.loads(handler.messages_buffer.buffer[-1])
        assert last_message["severity"] == WARNING
        assert last_message["message"] == "Maximum total length (4000) exceeded. Dropped 1 messages."


def test_max_buffer_size_lost_many():
    """Test total length limit works by checking that a record is dropped from the buffer when limit is reached."""
    with ExitStack() as exit_stack:
        # we don't need a real port for this one
        handler = HttpBatchRequestsHandler("localhost:61234", max_total_length=10000, overflow_drop_factor=0.5)
        exit_stack.callback(handler.stop)

        logger = get_logger(handler)
        logger.info("0" * 1000)
        logger.info("1" * 1000)
        logger.info("2" * 1000)
        logger.info("3" * 1000)
        logger.info("4" * 1000)
        logger.info("5" * 1000)
        logger.info("6" * 1000)
        logger.info("7" * 1000)
        logger.info("8" * 1000)
        # Check that four messages were dropped, and an additional warning message was added
        assert_buffer_attributes(handler, dropped=4)
        last_message = json.loads(handler.messages_buffer.buffer[-1])
        assert last_message["severity"] == WARNING
        assert last_message["message"] == "Maximum total length (10000) exceeded. Dropped 4 messages."


def test_json_fields():
    """Test handler sends valid JSON."""

    class ReqHandler(GzipRequestHandler):
        def do_POST(self):
            assert self.headers["Content-Type"] == "application/json"
            json_data = json.loads(self.body)
            assert isinstance(json_data, dict)
            assert set(json_data.keys()) == {"batch_id", "metadata", "logs", "lost"}
            logs = json_data["logs"]
            assert isinstance(logs, list)
            for log_item in logs:
                assert isinstance(log_item, dict)
                assert set(log_item.keys()) == {"serial_no", "severity", "timestamp", "logger_name", "message"}
            self.send_response(200, "OK")
            self.end_headers()

    with ExitStack() as exit_stack:
        logs_server = LogsServer(("localhost", 0), ReqHandler)
        exit_stack.callback(logs_server.server_close)

        handler = HttpBatchRequestsHandler(logs_server.authority, max_total_length=4000, flush_threshold=0.7)
        exit_stack.callback(handler.stop)

        logger = get_logger(handler)
        logger.info("A" * 1000)
        logger.info("B" * 1000)
        logger.info("C" * 1000)
        logs_server.handle_request()
        assert logs_server.processed > 0


def test_error_flushing():
    """Test handler logs a message when it get an error response from server."""

    class ErrorRequestHandler(GzipRequestHandler):
        def do_POST(self):
            self.send_error(403, "Forbidden")

    with ExitStack() as exit_stack:
        logs_server = LogsServer(("localhost", 0), ErrorRequestHandler)
        exit_stack.callback(logs_server.server_close)

        handler = HttpBatchRequestsHandler(logs_server.authority, max_total_length=10000)
        exit_stack.callback(handler.stop)

        logger = get_logger(handler)
        logger.warning("A" * 3000)
        logger.warning("B" * 3000)
        logger.warning("C" * 3000)
        logs_server.handle_request()
        assert logs_server.processed > 0
        # wait for the flush thread to log the error:
        time.sleep(0.5)
        last_message = json.loads(handler.messages_buffer.buffer[-1])
        assert last_message["severity"] == ERROR
        assert last_message["message"] == SERVER_SEND_ERROR_MESSAGE


def test_truncate_long_message():
    """Test message is truncated and marked accordingly if it's longer than max message size."""
    with ExitStack() as exit_stack:
        # we don't need a real port for this one
        handler = HttpBatchRequestsHandler("localhost:61234", max_message_size=1000)
        exit_stack.callback(handler.stop)

        logger = get_logger(handler)
        logger.info("A" * 2000)
        assert_buffer_attributes(handler, count=1)
        s = handler.messages_buffer.buffer[0]
        # Check that the json is within the limit
        assert len(s) <= 1000
        # Check that it's still valid
        m = json.loads(s)
        # Check that it's marked accordingly
        assert m["truncated"] is True


def test_identifiers():
    """Test message serial numbers are always consecutive and do not repeat."""
    with ExitStack() as exit_stack:
        # we don't need a real port for this one
        handler = HttpBatchRequestsHandler("localhost:61234", max_total_length=10000)
        exit_stack.callback(handler.stop)

        logger = get_logger(handler)
        for i in range(1000):
            logger.info("A" * random.randint(50, 600))
            if i % 7 == 0:
                logs = handler.make_batch().logs
                serial_nos = [json.loads(log)["serial_no"] for log in logs]
                assert_serial_nos_ok(serial_nos)


def test_flush_when_length_threshold_reached():
    """Test that logs are flushed when max length threshold is reached."""

    class ReqHandler(GzipRequestHandler):
        def do_POST(self):
            json_data = json.loads(self.body)
            logs = json_data["logs"]
            assert logs, "no logs!"
            self.send_response(200, "OK")
            self.end_headers()

    with ExitStack() as exit_stack:
        logs_server = LogsServer(("localhost", 0), ReqHandler)
        exit_stack.callback(logs_server.server_close)

        # set the interval very high because we want flush to only happen on length trigger
        handler = HttpBatchRequestsHandler(logs_server.authority, max_total_length=10000, flush_interval=999999.0)
        exit_stack.callback(handler.stop)

        logger = get_logger(handler)
        logger.info("A" * 1000)
        logger.info("B" * 2000)
        logger.info("C" * 3000)
        logger.info("D" * 4000)
        logs_server.handle_request()
        assert logs_server.processed > 0


def test_multiple_threads():

    """Test that multiple threads writing simultaneously do not corrupt the buffer."""

    with ExitStack() as exit_stack:
        handler = HttpBatchRequestsHandler("localhost:61234", flush_interval=1.0)
        mock_adapter = MockAdapter()
        handler.session.mount("http://", mock_adapter)
        exit_stack.callback(handler.stop)

        logger = get_logger(handler)

        def log_func(end_time):
            while time.time() < end_time:
                logger.info("A" * random.randint(50, 1000))
                logger.info("A" * random.randint(50, 2000))
                logger.info("A" * random.randint(50, 4000))
                time.sleep(random.uniform(0, 0.1))

        end_time = time.time() + 10.0
        threads = [
            Thread(target=log_func, name="Log thread 1", args=(end_time,)),
            Thread(target=log_func, name="Log thread 2", args=(end_time,)),
            Thread(target=log_func, name="Log thread 3", args=(end_time,)),
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert mock_adapter.sends > 5
        assert mock_adapter.successes == mock_adapter.sends
        assert mock_adapter.errors == 0

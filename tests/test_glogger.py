import json
import logging
import random
import time
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from granulate_utils.glogger import BatchRequestsHandler


class HttpBatchRequestsHandler(BatchRequestsHandler):
    scheme = "http"


class LogsServer(HTTPServer):
    timeout = 5.0

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


def assert_buffer_attributes(handler, **kwargs):
    mb = handler.messages_buffer
    for k in kwargs:
        assert getattr(mb, k) == kwargs[k]


def assert_serial_nos_ok(serial_nos):
    assert serial_nos == list(sorted(serial_nos)), "bad order!"
    assert len(set(serial_nos)) == len(serial_nos), "have duplicates!"


def test_max_buffer_size():
    """Test total length limit works by checking that a record is dropped from the buffer when limit is reached."""
    with ExitStack() as exit_stack:
        # we don't need a real port for this one
        handler = HttpBatchRequestsHandler("localhost:61234", max_total_length=4000, flush_threshold=0.9)
        exit_stack.callback(handler.stop)

        logging.basicConfig(force=True, handlers=[handler], level=0)
        logging.info("A" * 1500)
        assert_buffer_attributes(handler, count=1, head_serial_no=0, next_serial_no=1)
        assert handler.messages_buffer.total_length > 1500
        logging.info("A" * 1500)
        assert_buffer_attributes(handler, count=2, head_serial_no=0, next_serial_no=2)
        assert handler.messages_buffer.total_length > 3000
        logging.info("A" * 1500)
        assert_buffer_attributes(handler, count=2, head_serial_no=1, next_serial_no=3)
        assert handler.messages_buffer.total_length > 3000


def test_content_type_json():
    """Test handler sends valid JSON."""

    class ReqHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            assert self.headers["Content-Type"] == "application/json"
            json_data = json.load(self.rfile)
            assert isinstance(json_data, dict)
            assert set(json_data.keys()) == {"batch_id", "metadata", "logs"}
            logs = json_data["logs"]
            assert isinstance(logs, list)
            for log_item in logs:
                assert isinstance(log_item, dict)
                assert set(log_item.keys()) == {"serial_no", "level", "timestamp", "logger_name", "message"}
            self.send_response(200, "OK")
            self.end_headers()

    with ExitStack() as exit_stack:
        logs_server = LogsServer(("localhost", 0), ReqHandler)
        exit_stack.callback(logs_server.server_close)

        handler = HttpBatchRequestsHandler(logs_server.authority, max_total_length=10000)
        exit_stack.callback(handler.stop)

        logging.basicConfig(force=True, handlers=[handler], level=0)
        logging.info("A" * 1000)
        logging.info("B" * 2000)
        logging.info("C" * 3000)
        logging.info("D" * 4000)
        logs_server.handle_request()
        assert logs_server.processed > 0


def test_truncate_long_message():
    """Test message is truncated and marked accordingly if it's longer than max message size."""
    with ExitStack() as exit_stack:
        # we don't need a real port for this one
        handler = HttpBatchRequestsHandler("localhost:61234", max_message_size=1000)
        exit_stack.callback(handler.stop)

        logging.basicConfig(force=True, handlers=[handler], level=0)
        logging.info("A" * 2000)
        assert_buffer_attributes(handler, count=1)
        s = handler.messages_buffer.buffer[0]
        m = json.loads(s)
        assert len(m["message"]) <= 1000
        assert m["truncated"] is True


def test_identifiers():
    """Test message serial numbers are always consecutive and do not repeat."""
    with ExitStack() as exit_stack:
        # we don't need a real port for this one
        handler = HttpBatchRequestsHandler("localhost:61234", max_total_length=10000)
        exit_stack.callback(handler.stop)

        logging.basicConfig(force=True, handlers=[handler], level=0)
        for i in range(1000):
            logging.info("A" * random.randint(50, 600))
            if i % 7 == 0:
                logs = handler.make_batch().logs
                serial_nos = [json.loads(log)["serial_no"] for log in logs]
                assert_serial_nos_ok(serial_nos)


def test_flush_when_length_threshold_reached():
    """Test that logs are flushed when max length threshold is reached."""

    class ReqHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            json_data = json.load(self.rfile)
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

        logging.basicConfig(force=True, handlers=[handler], level=0)
        logging.info("A" * 1000)
        logging.info("B" * 2000)
        logging.info("C" * 3000)
        logging.info("D" * 4000)
        logs_server.handle_request()
        assert logs_server.processed > 0


def test_multiple_threads():
    """Test that multiple threads writing simultaneously do not corrupt the buffer."""

    class MockSession:
        def __init__(self):
            self.posts = 0
            self.errors = 0
            self.successes = 0

        def post(self, uri, *, data, **kwargs):
            self.posts += 1
            try:
                json_data = json.loads(data.decode("utf-8"))
                assert_serial_nos_ok([log["serial_no"] for log in json_data["logs"]])
                self.successes += 1
            except Exception:
                self.errors += 1

    with ExitStack() as exit_stack:
        session = MockSession()
        handler = HttpBatchRequestsHandler("localhost:61234", flush_interval=2.0)
        handler.session = session
        exit_stack.callback(handler.stop)

        logging.basicConfig(force=True, handlers=[handler], level=0)

        def log_func(end_time):
            while time.time() < end_time:
                logging.info("A" * random.randint(50, 4000))
                logging.info("A" * random.randint(50, 4000))
                logging.info("A" * random.randint(50, 4000))

        end_time = time.time() + 20.0
        threads = [
            Thread(target=log_func, name="Log thread 1", args=(end_time,)),
            Thread(target=log_func, name="Log thread 2", args=(end_time,)),
            Thread(target=log_func, name="Log thread 3", args=(end_time,)),
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert session.posts > 5
        assert session.successes == session.posts
        assert session.errors == 0

# gLogger SDK

gLogger provides a `logging` `Handler` implementation that sends logs in batches to a remote server
asynchronously.

## Usage

The most trivial way to use gLogger is like so: 

```python
import logging
from glogger.handler import BatchRequestsHandler

handler = BatchRequestsHandler("my_app", AUTH_TOKEN, "logs.example.com")
logging.basicConfig(handlers=[handler])
```

For more fine-grained control you can subclass `BatchRequestsHandler` and provide additional
message fields and common metadata:

```python
from glogger.handler import BatchRequestsHandler

class MyHandler(BatchRequestsHandler):
    def get_metadata(self) -> dict:
        return {
            "service_name": "skynet",
            "cpus": os.cpu_count()
        }

    def get_extra_fields(self, record: LogRecord) -> dict:
        return {
            "thread_id": record.thread,
        }
```

# gLogger SDK

gLogger provides a `logging.Handler` implementation that sends logs in batches to a remote server
asynchronously.

## Usage

The most trivial way to use gLogger is like so:

```python
import logging
from glogger.handler import BatchRequestsHandler
from glogger.sender import Sender, AuthToken

handler = BatchRequestsHandler(Sender("my_app", "logs.example.com", auth=AuthToken(AUTH_TOKEN)))
logging.basicConfig(handlers=[handler])
```

For more fine-grained control you can subclass `BatchRequestsHandler` and provide additional
message fields and common metadata:

```python
import logging
from glogger.handler import BatchRequestsHandler

class MyHandler(BatchRequestsHandler):
    def get_metadata(self) -> dict:
        return {
            "service_name": "skynet",
            "cpus": os.cpu_count(),
        }

    def get_extra_fields(self, record: logging.LogRecord) -> dict:
        return {
            "thread_id": record.thread,
        }
```

## Extra message attributes

If you use the `glogger.extra.ExtraAdapter` for logging you can easily add and reference custom
message attributes:

```python
import logging
from glogger.extra_adapter import ExtraAdapter

adapter = ExtraAdapter(logging.getLogger(__name__))
adapter.info('Cogito, ergo sum', author='René Descartes')
```

`BatchRequestsHandler` will automatically report these attributes along with the message, if used.

### Custom dynamic attributes:

You can also provide custom dynamic attributes by overriding the `ExtraAdapter.get_extra()` method:

```python
import logging
from glogger.extra_adapter import ExtraAdapter


class CustomExtraAdapter(ExtraAdapter):
    def get_extra(self, **kwargs):
        return {'extra_count': len(kwargs.get('extra', {}))}
```

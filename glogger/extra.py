import logging
from typing import Any, MutableMapping, Tuple


class ExtraAdapter(logging.LoggerAdapter):
    """
    Logging adapter that adds an attribute named `_extra` containing a dict of all extra fields of a record.
    This allows referring to the extras dict in a Formatter, when the extra field names differ among records.
    """

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> Tuple[Any, MutableMapping[str, Any]]:
        if kwargs.get("extra") is not None:
            kwargs["extra"].update(self.extra)
        else:
            kwargs["extra"] = dict(self.extra)
        kwargs["extra"]["_extra"] = dict(kwargs["extra"])  # such that logging.Formatter will accept `_extra`
        return msg, kwargs

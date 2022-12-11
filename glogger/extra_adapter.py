import logging
from typing import Any, Dict, Mapping, MutableMapping, Tuple


class ExtraAdapter(logging.LoggerAdapter):
    """
    This adapter:
    1. Allows enriching messages with static and dynamic extra attributes, and
    2. Adds an attribute named "extra" to each record that contains all the extra attributes.
    """

    logging_kwargs = {"exc_info", "stack_info", "stacklevel", "extra"}

    def __init__(self, logger: logging.Logger, extra: Mapping[str, object] = None):
        # If default extra not provided, use empty dict:
        super().__init__(logger, extra=extra or {})

    def get_extra(self, **kwargs) -> Mapping[str, Any]:
        """
        Get extra attributes for a message. kwargs contains all the kwargs passed to the logging call, with any
        non-standard kwargs available under an "extra" key. Default implementation merges existing attributes with
        default ones provided at initialization.
        """
        return {**self.extra, **kwargs.get("extra", {})}

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> Tuple[Any, MutableMapping[str, Any]]:
        # Partition the kwargs into logging kwargs and extra kwargs:
        logging_kwargs: Dict[str, Any] = {}
        other_kwargs = {}
        for k, v in kwargs.items():
            if k in self.logging_kwargs:
                logging_kwargs[k] = v
            else:
                other_kwargs[k] = v

        # Merge other kwargs into extra:
        extra: Mapping[str, object] = {**logging_kwargs.get("extra", {}), **other_kwargs}
        if extra:
            logging_kwargs["extra"] = extra

        extra = self.get_extra(**logging_kwargs)
        # Retain all extras as attributes on the record, and add "extra" attribute that contains all the extras:
        logging_kwargs.update({"extra": {**extra, **{"extra": extra}}})
        return msg, logging_kwargs

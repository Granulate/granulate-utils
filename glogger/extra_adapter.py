#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import logging
import sys
from typing import Any, Dict, Mapping, MutableMapping, Tuple


class ExtraAdapter(logging.LoggerAdapter):
    """
    This adapter:
    1. Allows enriching messages with static and dynamic extra attributes, and
    2. Adds an attribute named "extra" to each record that contains all the extra attributes.
    """

    logging_kwargs = {"exc_info", "stack_info", "stacklevel", "extra"}

    def __init__(self, logger: logging.Logger, extra: Mapping[str, Any] = None):
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
        extra: Mapping[str, Any] = {**logging_kwargs.get("extra", {}), **other_kwargs}
        if extra:
            logging_kwargs["extra"] = extra

        extra = self.get_extra(**logging_kwargs)

        if logging_kwargs.get("exc_info") is True:
            # If exc_info is True, and the exception is subclassing ExtraException, then add the extra attributes
            # from the exception to the extra attributes of the record:
            exc_info = sys.exc_info()
            # If exc_info is True, then there must be an exception:
            assert exc_info is not None
            if (exc_extra := getattr(exc_info[1], "extra", None)) is not None and isinstance(exc_extra, dict):
                # Merge 'extra' attributes from exception into extra, the exception's extra attributes take precedence:
                extra = {**extra, **exc_extra}

        # Retain all extras as attributes on the record, and add "extra" attribute that contains all the extras:
        logging_kwargs.update({"extra": {**extra, "extra": extra}})
        return msg, logging_kwargs

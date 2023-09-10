import logging
from typing import Any, Mapping

from glogger.extra_adapter import ExtraAdapter
from glogger.extra_exception import ExtraException


class CustomException(ExtraException):
    def __init__(self, **extra: Any) -> None:
        super().__init__("My Exception", **extra)


class CustomGetExtraAdapter(ExtraAdapter):
    def get_extra(self, **kwargs) -> Mapping[str, Any]:
        extra = super().get_extra(**kwargs)
        assert type(extra) is dict
        extra.update({"neo": "keanu reeves"})
        return extra


def test_other_kwargs_in_extra(caplog):
    caplog.set_level(logging.INFO)
    ExtraAdapter(logging.getLogger()).info("test message", test=6)
    record = caplog.records[0]
    assert record.test == 6
    assert hasattr(record, "extra")
    assert record.extra == dict(test=6)


def test_logging_kwargs_not_present_in_extra(caplog):
    caplog.set_level(logging.INFO)
    ExtraAdapter(logging.getLogger()).info("test message", stacklevel=8, stack_info=True, test=6)
    record = caplog.records[0]
    assert record.test == 6
    assert hasattr(record, "extra")
    assert record.extra == dict(test=6)


def test_custom_get_extra(caplog):
    caplog.set_level(logging.INFO)
    CustomGetExtraAdapter(logging.getLogger()).info("test message", test=6)
    record = caplog.records[0]
    assert record.test == 6
    assert record.neo == "keanu reeves"
    assert hasattr(record, "extra")
    assert record.extra == dict(test=6, neo="keanu reeves")


def test_extra_exception(caplog):
    caplog.set_level(logging.INFO)
    try:
        raise CustomException(test=6)
    except Exception as e:
        assert e.extra == {"test": 6}
        ExtraAdapter(logging.getLogger()).exception("test message")
    record = caplog.records[0]
    assert record.message == "test message"
    assert record.test == 6
    assert hasattr(record, "extra")
    assert record.extra == dict(test=6)

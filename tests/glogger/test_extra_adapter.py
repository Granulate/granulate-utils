import logging
from typing import Any, Mapping

import pytest

from glogger.extra_adapter import ExtraAdapter
from glogger.extra_exception import ExtraException


class CustomException(Exception):
    def __init__(self) -> None:
        super().__init__("My Exception")
        self.extra = {"test": 6}


class CustomGetExtraAdapter(ExtraAdapter):
    def get_extra(self, **kwargs) -> Mapping[str, Any]:
        extra = super().get_extra(**kwargs)
        assert type(extra) is dict
        extra.update({"neo": "keanu reeves"})
        return extra


def test_other_kwargs_in_extra(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    ExtraAdapter(logging.getLogger()).info("test message", test=6)
    record = caplog.records[0]
    assert getattr(record, "test") == 6
    assert hasattr(record, "extra")
    assert getattr(record, "extra") == dict(test=6)


def test_logging_kwargs_not_present_in_extra(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    ExtraAdapter(logging.getLogger()).info("test message", stacklevel=8, stack_info=True, test=6)
    record = caplog.records[0]
    assert getattr(record, "test") == 6
    assert hasattr(record, "extra")
    assert getattr(record, "extra") == dict(test=6)


def test_custom_get_extra(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    CustomGetExtraAdapter(logging.getLogger()).info("test message", test=6)
    record = caplog.records[0]
    assert getattr(record, "test") == 6
    assert getattr(record, "neo") == "keanu reeves"
    assert hasattr(record, "extra")
    assert getattr(record, "extra") == dict(test=6, neo="keanu reeves")


@pytest.mark.parametrize(
    "exc", [CustomException(), ExtraException("My Exception", test=6)], ids=["CustomException", "ExtraException"]
)
def test_extra_from_exception(caplog: pytest.LogCaptureFixture, exc: Exception) -> None:
    caplog.set_level(logging.INFO)
    try:
        raise exc
    except Exception:
        ExtraAdapter(logging.getLogger()).exception("test message")
    record = caplog.records[0]
    assert record.message == "test message"
    assert getattr(record, "test") == 6
    assert hasattr(record, "extra")
    assert getattr(record, "extra") == dict(test=6)

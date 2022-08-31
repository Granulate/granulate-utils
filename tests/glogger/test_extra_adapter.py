import logging
from typing import Any, Mapping

from glogger.extra_adapter import ExtraAdapter


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

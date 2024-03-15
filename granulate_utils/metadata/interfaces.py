from typing import Generator, Protocol


class ProcessedStringField(Protocol):
    @classmethod
    def __get_validators__(cls) -> Generator:
        ...

    @property
    def plaintext(self) -> str:
        ...

    @property
    def text(self) -> str:
        ...


def process_string(value: str) -> ProcessedStringField:
    return ProcessedString(value=value)


class ProcessedString(str):
    def __init__(self, *, value: str) -> None:
        self._value = value

    @classmethod
    def __get_validators__(cls) -> Generator:
        yield lambda _class, val: val

    @property
    def plaintext(self) -> str:
        return self._value

    @property
    def text(self) -> str:
        return self._value

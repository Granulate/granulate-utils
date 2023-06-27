from __future__ import annotations

from abc import ABC
from typing import Any, Callable, ContextManager, Dict, List, Tuple, TypeVar
from unittest.mock import Mock, mock_open, patch

from requests_mock.mocker import Mocker

from granulate_utils.config_feeder.client.bigdata import get_node_info
from granulate_utils.config_feeder.core.models.node import NodeInfo

T = TypeVar("T", bound="NodeMockBase")


class NodeMockBase(ABC):
    def __init__(self) -> None:
        self._files: Dict[str, str] = {}
        self._stdout: Dict[str, bytes | str] = {}
        self._requests: List[Tuple[str, str, Dict[str, Any]]] = []
        self._contexts: List[Tuple[ContextManager[Any], Callable[[Any], None] | None]] = []

    @property
    def node_info(self) -> NodeInfo:
        node_info = get_node_info()
        assert node_info is not None
        return node_info

    def mock_file(self: T, fname: str, content: str) -> T:
        self._files[fname] = content
        return self

    def mock_command_stdout(self: T, cmd: str, stdout: bytes | str) -> T:
        self._stdout[cmd] = stdout
        return self

    def mock_http_response(self: T, method: str, url: str, response: Dict[str, Any]) -> T:
        self._requests.append((method, url, response))
        return self

    def add_context(self: T, ctx: ContextManager[Any], fn: Callable[[Any], None] | None = None) -> T:
        self._contexts.append((ctx, fn))
        return self

    def _mock_file_open(self, fname: str) -> Any:
        if fname not in self._files:
            raise FileNotFoundError(f"File {fname} not found in mock")
        return mock_open(read_data=self._files[fname]).return_value

    async def _mock_create_subprocess_stdout(self, *args: Any, **kwargs: Any) -> Mock:
        mock_proc = Mock()

        async def mock_communicate() -> Tuple[bytes | str, bytes]:
            cmd = args[0]
            if cmd not in self._stdout:
                raise Exception(f"Command '{cmd}' not found in mock")
            return self._stdout[cmd], b""

        mock_proc.communicate.side_effect = mock_communicate
        return mock_proc

    def _mock_subprocess_run_stdout(self, *args: Any, **kwargs: Any) -> Mock:
        mock = Mock()
        cmd = " ".join(args[0])
        mock.stdout = self._stdout[cmd]
        return mock

    def _mock_http_response(self, mock: Any) -> None:
        for method, url, response in self._requests:
            mock.request(method, url, **response)

    def __enter__(self: T) -> T:
        self.add_context(
            patch(
                "builtins.open",
                lambda fname, *args: self._mock_file_open(str(fname)),
            )
        )
        self.add_context(
            patch(
                "asyncio.create_subprocess_shell",
                self._mock_create_subprocess_stdout,
            )
        )
        self.add_context(
            patch(
                "subprocess.run",
                self._mock_subprocess_run_stdout,
            ),
        )
        self.add_context(Mocker(), self._mock_http_response)

        for ctx, fn in self._contexts:
            value = ctx.__enter__()
            if fn is not None:
                fn(value)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_tb: Any) -> None:
        for ctx, _ in self._contexts:
            ctx.__exit__(None, None, None)

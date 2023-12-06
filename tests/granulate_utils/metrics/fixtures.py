from __future__ import annotations

from typing import Any, ContextManager, Dict, List, Set, Tuple, TypeVar
from unittest.mock import Mock, mock_open, patch

T = TypeVar("T", bound="YarnNodeMock")

MOCK_DIR = "/home/hadoop/hadoop"


class YarnNodeMock:
    def __init__(
        self,
        *,
        yarn_site_xml: str = "",
        is_node_manager: bool = False,
        hostname: str = "",
        ip: str = "",
    ) -> None:
        self._files: Dict[str, str] = {}
        self._dirs: Set[str] = set()
        self._stdout: Dict[str, bytes | str] = {}
        self._requests: List[Tuple[str, str, Dict[str, Any]]] = []
        self._contexts: Set[ContextManager[Any]] = set()
        self._hostname: str = ""
        self._ip: str = ""
        self._mock = Mock()

        self.mock_file("/home/hadoop/hadoop/etc/hadoop/yarn-site.xml", yarn_site_xml)
        self.mock_dir(MOCK_DIR)
        self.mock_command_stdout(
            "ps -ax",
            f"""12345 ?  Sl  0:04 java
                -Dyarn.home.dir={MOCK_DIR}
                -Dyarn.log.file=rm.log
                org.apache.hadoop.yarn.server.resourcemanager.ResourceManager""",
        )
        self._mock_is_node_manager_running(is_node_manager=is_node_manager)
        self.mock_hostname(hostname=hostname)
        self.mock_ip(ip=ip)

    def mock_ip(self, ip: str = "") -> None:
        self._ip = ip
        self._contexts.add(
            patch(
                "socket.socket",
                self._mock_local_ip,
            )
        )

    def mock_hostname(self, hostname: str = "") -> None:
        self._hostname = hostname
        self._contexts.add(
            patch(
                "os.uname",
                return_value=(
                    "Linux",
                    self._hostname,
                    "5.15.0-79-generic",
                    "#86-Ubuntu SMP Mon Jul 10 16:07:21 UTC 2023",
                    "x86_64",
                ),
            )
        )

    def mock_file(self, fname: str, content: str) -> None:
        self._files[fname] = content
        self._contexts.add(
            patch(
                "builtins.open",
                lambda fname, *args: self._mock_file_open(str(fname)),
            )
        )
        self._contexts.add(patch("pathlib.Path.is_file", lambda path: str(path) in self._files))

    def mock_dir(self, dname: str) -> None:
        self._dirs.add(dname)
        self._contexts.add(
            patch(
                "pathlib.Path.is_dir",
                lambda path: str(path) in self._dirs,
            )
        )

    def mock_command_stdout(self, cmd: str, stdout: bytes | str):
        self._stdout[cmd] = stdout
        self._contexts.add(
            patch(
                "subprocess.run",
                self._mock_subprocess_run_stdout,
            ),
        )

    def _mock_file_open(self: T, fname: str) -> Any:
        if fname not in self._files:
            raise FileNotFoundError(f"File {fname} not found in mock")
        return mock_open(read_data=self._files[fname]).return_value

    def _mock_subprocess_run_stdout(self: T, *args: Any, **kwargs: Any) -> Mock:
        cmd = " ".join(args[0])
        self._mock.stdout = self._stdout[cmd]
        return self._mock

    def _mock_local_ip(self, *args: Any, **kwargs: Any) -> Mock:
        self._mock.getsockname.return_value = (self._ip, 0)
        return self._mock

    def _mock_is_node_manager_running(self, is_node_manager: bool = False) -> None:
        self._contexts.add(
            patch(
                "granulate_utils.metrics.yarn.utils.is_node_manager_running",
                return_value=is_node_manager,
            )
        )

    def __enter__(self: T) -> T:
        for ctx in self._contexts:
            ctx.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_tb: Any) -> None:
        for ctx in self._contexts:
            ctx.__exit__(None, None, None)

#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from __future__ import annotations

import os
import re
import signal
from dataclasses import dataclass
from itertools import dropwhile
from typing import Any, Dict, Iterable, List, Literal, Optional, Union

from packaging.version import Version

NATIVE_FRAMES_REGEX = re.compile(r"^Native frames:[^\n]*\n(.*?)\n\n", re.MULTILINE | re.DOTALL)
"""
See VMError::print_native_stack.
Example:
    Native frames: (J=compiled Java code, j=interpreted, Vv=VM code, C=native code)
    C  [libc.so.6+0x18e4e1]
    C  [libasyncProfiler.so+0x1bb4e]  Profiler::dump(std::ostream&, Arguments&)+0xce
    C  [libasyncProfiler.so+0x1bcae]  Profiler::runInternal(Arguments&, std::ostream&)+0x9e
    C  [libasyncProfiler.so+0x1c242]  Profiler::run(Arguments&)+0x212
    C  [libasyncProfiler.so+0x48d81]  Agent_OnAttach+0x1e1
    V  [libjvm.so+0x7ea65b]
    V  [libjvm.so+0x2f5e62]
    V  [libjvm.so+0xb08d2f]
    V  [libjvm.so+0xb0a0fa]
    V  [libjvm.so+0x990552]
    C  [libpthread.so.0+0x76db]  start_thread+0xdb
"""

SIGINFO_REGEX = re.compile(r"^siginfo: ([^\n]*)", re.MULTILINE | re.DOTALL)
"""
See os::print_siginfo
Example:
    siginfo: si_signo: 11 (SIGSEGV), si_code: 0 (SI_USER), si_pid: 537787, si_uid: 0
"""

CONTAINER_INFO_REGEX = re.compile(r"^container \(cgroup\) information:\n(.*?)\n\n", re.MULTILINE | re.DOTALL)
"""
See os::Linux::print_container_info
Example:
    container (cgroup) information:
    container_type: cgroupv1
    cpu_cpuset_cpus: 0-15
    cpu_memory_nodes: 0
    active_processor_count: 16
    cpu_quota: -1
    cpu_period: 100000
    cpu_shares: -1
    memory_limit_in_bytes: -1
    memory_and_swap_limit_in_bytes: -2
    memory_soft_limit_in_bytes: -1
    memory_usage_in_bytes: 26905034752
    memory_max_usage_in_bytes: 27891224576
"""

VM_INFO_REGEX = re.compile(r"^vm_info: ([^\n]*)", re.MULTILINE | re.DOTALL)
"""
This is the last line printed in VMError::report.
Example:
    vm_info: OpenJDK 64-Bit Server VM (25.292-b10) for linux-amd64 JRE (1.8.0_292-8u292-b10-0ubuntu1~18.04-b10), ...
"""

"""
Match /libjvm.so files. Not ended with $ because it might be suffixed with " (deleted)", in case
Java was e.g upgrade and the files were replaced on disk.
I could use (?: \\(deleted\\))?$ but I'm afraid it'll be too complex for some of the "grep"s that need to
handle this regex, haha.
"""
DETECTED_JAVA_PROCESSES_REGEX = r"^.+/libjvm\.so"


def locate_hotspot_error_file(nspid: int, cmdline: List[str]) -> Iterable[str]:
    """
    Locate a fatal error log written by the Hotspot JVM, if one exists.
    See https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/felog001.html.

    :return: Candidate paths (relative to process working directory) ordered by dominance.
    """
    for arg in cmdline:
        if arg.startswith("-XX:ErrorFile="):
            _, error_file = arg.split("=", maxsplit=1)
            yield error_file.replace("%p", str(nspid))
            break
    default_error_file = f"hs_err_pid{nspid}.log"
    yield default_error_file
    yield f"/tmp/{default_error_file}"


def is_java_fatal_signal(sig: Union[int, signal.Signals]) -> bool:
    # SIGABRT is what JVMs (at least HotSpot) exit with upon a VM error (e.g after writing the hs_err file).
    # SIGKILL is the result of OOM.
    # SIGSEGV is added because in some extreme cases, the signal handler (which usually ends up with SIGABRT)
    # causes another SIGSEGV (possibly in some loop), and eventually Java really dies with SIGSEGV.
    # Other signals (such as SIGTERM which is common) are ignored until proven relevant
    # to hard errors such as crashes. (SIGTERM, for example, is used as containers' stop signal)
    if isinstance(sig, int):
        signo = sig
    else:
        signo = sig.value
    return signo in (signal.SIGABRT.value, signal.SIGKILL.value, signal.SIGSEGV.value)


def java_exit_code_to_signo(exit_code: int) -> Optional[int]:
    if os.WIFSIGNALED(exit_code):
        return os.WTERMSIG(exit_code)
    elif exit_code == 0x8F00:
        # java exits with 143 upon SIGTERM
        return signal.SIGTERM.value
    else:
        # not a signal
        return None


VmType = Literal["HotSpot", "Zing", "OpenJ9", None]


@dataclass
class JvmVersion:
    version: Version
    build: int
    name: str
    vm_type: VmType
    zing_major: Optional[int] = None  # non-None if Zing


# Parse java version information from "java -version" output
def parse_jvm_version(version_string: str) -> JvmVersion:
    # Example java -version output:
    #   openjdk version "1.8.0_265"
    #   OpenJDK Runtime Environment (AdoptOpenJDK)(build 1.8.0_265-b01)
    #   OpenJDK 64-Bit Server VM (AdoptOpenJDK)(build 25.265-b01, mixed mode)
    # We are taking the version from the first line, and the build number and vm name from the last line

    lines = version_string.splitlines()

    # the version always starts with "openjdk version" or "java version". strip all lines
    # before that.
    lines = list(dropwhile(lambda l: not ("openjdk version" in l or "java version" in l), lines))

    # version is always in quotes
    _, version_str, _ = lines[0].split('"')
    # matches the build string from e.g (build 25.212-b04, mixed mode) -> "25.212-b04"
    m = re.search(r"\(build ([^,)]+?)(?:,|\))", version_string)
    assert m is not None, f"did not find build_str in {version_string!r}"
    build_str = m.group(1)

    if (
        any(version_str.endswith(suffix) for suffix in ("-internal", "-ea", "-ojdkbuild"))
        or re.search(r"-zing_[\d\.]+$", version_str) is not None
    ):
        # strip those suffixes to keep the rest of the parsing logic clean
        version_str = version_str.rsplit("-")[0]

    version_list = version_str.split(".")
    if version_list[0] == "1":
        # For java 8 and prior, versioning looks like
        # 1.<major>.0_<minor>-b<build_number>
        # For example 1.8.0_242-b12 means 8.242 with build number 12
        assert len(version_list) == 3, f"Unexpected number of elements for old-style java version: {version_list!r}"
        major = version_list[1]
        if "_" in version_list[-1]:
            minor = version_list[-1].split("_")[-1]
            version = Version(f"{major}.{minor}")
        else:
            assert version_list[-1] == "0", f"Unexpected minor? {version_list!r}"
            version = Version(major)
        # find the -b or -ojdkbuild-
        if "-b" in build_str:
            build_split = build_str.split("-b")
            # it can appear multiple times, e.g "(build 1.8.0_282-8u282-b08-0ubuntu1~16.04-b08)"
            assert len(build_split) >= 2, f"Unexpected number of occurrences of '-b' in {build_str!r}"
        else:
            build_split = build_str.split("-ojdkbuild-")
            assert len(build_split) == 2, f"Unexpected number of occurrences of '-ojdkbuild-' in {build_str!r}"
        build = int(build_split[-1])
    else:
        # Since java 9 versioning became more normal, and looks like
        # <version>+<build_number>
        # For example, 11.0.11+9
        version = Version(version_str)
        assert "+" in build_str, f"Did not find expected build number prefix in new-style java version: {build_str!r}"
        # The goal of the regex here is to read the build number until a non-digit character is encountered,
        # since additional information can be appended after it, such as the platform name
        matched = re.match(r"\d+", build_str[build_str.find("+") + 1 :])
        assert matched, f"Unexpected build number format in new-style java version: {build_str!r}"
        build = int(matched[0])

    # There is no real format here, just use the entire description string
    vm_name = lines[2].split("(build")[0].strip()

    if vm_name.startswith("OpenJDK"):
        vm_type: VmType = "HotSpot"
    elif vm_name.startswith("Zing"):
        vm_type = "Zing"
    elif vm_name == "Eclipse OpenJ9 VM":
        vm_type = "OpenJ9"
    else:
        # TODO: additional types?
        vm_type = None

    if vm_type == "Zing":
        # name is e.g Zing 64-Bit Tiered VM Zing22.04.1.0+1
        # or (Zing 21.12.0.0-b2-linux64) from the azul/prime:1.8.0-312-2-21.12.0.0 image.
        m = re.search(r"Zing ?(\d+)\.", vm_name)
        if m is None:
            # Zing <= 20 versions have a different format
            # this matches the "20" out of (build 1.8.0-zing_20.03.0.0-b1).
            m = re.search(r"\(build[^\)]+zing_(\d+)\.[^\(]+\)", version_string)
            assert m is not None, f"Missing old format of Zing version? {version_string!r}"
        zing_major: Optional[int] = int(m.group(1))
    else:
        zing_major = None

    return JvmVersion(version, build, vm_name, vm_type, zing_major)


@dataclass
class JvmFlag:
    name: str
    type: str
    value: str
    origin: str
    kind: List[str]

    vm_flags_pattern = re.compile(
        r"(?P<flag_type>\S+)\s+"
        r"(?P<flag_name>\S+)\s+"
        r"(?P<flag_equal_sign_prefix>:)?= "
        r"(?P<flag_value>\S*)\s+"
        r"{(?P<flag_kind>.+?)}"
        r"(?:\s*{(?P<flag_origin_jdk_9>.*)})?"
    )

    def to_dict(self) -> Dict[str, Dict[str, Union[str, List[str]]]]:
        return {self.name: {"type": self.type, "value": self.value, "origin": self.origin, "kind": self.kind}}

    @classmethod
    def from_dict(cls, jvm_flag_dict: Dict[str, Any]) -> JvmFlag:
        name, flag_dict = list(jvm_flag_dict.items())[0]
        return JvmFlag(name=name, **flag_dict)

    @classmethod
    def from_str(cls, line: str) -> Optional[JvmFlag]:
        """
        The output of VM.flags -all format on jdk 8:
        bool UseCompressedClassPointers               := true                                {lp64_product}
        flag_type flag_name                           := flag_value                          {flag_kind}
        ":=" indicates non default origin for the flag, while "=" indicates default origin

        The output of VM.flags -all format on jdk 9+:
        bool OptoScheduling                           = false                               {C2 pd product} {default}
        flag_type flag_name                           = flag_value                          {flag_kind} {flag_origin}

        flag_kind is space separated list of kinds, e.g. "C2 pd product"

        possible flag kinds:
        "product", "manageable", "diagnostic", "experimental", "notproduct", "develop", "lp64_product", "rw", "pd", "JVMCI", "C1", "C2", "ARCH"
        https://github.com/openjdk/jdk17u/blob/2fe42855c48c49b515b97312ce64a5a8ef3af407/src/hotspot/share/runtime/flags/jvmFlag.cpp#L338 # noqa: E501

        possible flag types:
        "bool", "int", "uint", "intx", "uintx", "uint64_t", "size_t", "double", "ccstr", "ccstrlist"
        https://github.com/openjdk/jdk17u/blob/2fe42855c48c49b515b97312ce64a5a8ef3af407/src/hotspot/share/runtime/flags/jvmFlag.hpp#L134 # noqa: E501

        possible flag origins:
        default, non-default, command line, environment, config file, management, ergonomic, attach, internal, jimage, "command line, ergonomic" (flag is set from command line and aligned by ergonomic) # noqa: E501
        https://github.com/openjdk/jdk17u/blob/2fe42855c48c49b515b97312ce64a5a8ef3af407/src/hotspot/share/runtime/flags/jvmFlag.hpp#L36 # noqa: E501

        KNOWN ISSUES:
        - we don't parse the flag value for ccstrlist type flags (e.g. -XX:CompileCommand='A' -XX:CompileCommand='B')
        """

        match = cls.vm_flags_pattern.search(line)
        if match is None:
            return None

        # get the flag origin if jvm 9+, otherwise get is the flag from non default origin as described above
        flag_origin_jdk_9 = match.group("flag_origin_jdk_9")

        flag_equal_sign_prefix = match.group("flag_equal_sign_prefix")
        flag_is_non_default_origin_only_jdk_8 = flag_equal_sign_prefix == ":"

        is_jdk_8 = flag_is_non_default_origin_only_jdk_8 or flag_origin_jdk_9 is None

        if is_jdk_8:
            if flag_is_non_default_origin_only_jdk_8:
                flag_origin = "non-default"
            else:
                flag_origin = "default"
        else:
            flag_origin = flag_origin_jdk_9

        # split the list of space separated flag_kinds as described above
        flag_kind = match.group("flag_kind").split()

        return cls(
            name=match.group("flag_name"),
            type=match.group("flag_type"),
            value=match.group("flag_value"),
            origin=flag_origin,
            kind=sorted(flag_kind),
        )


def parse_jvm_flags(jvm_flags_string: str) -> List[JvmFlag]:
    return [flag for line in jvm_flags_string.splitlines() if (flag := JvmFlag.from_str(line)) is not None]

#
# Copyright (C) 2023 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import hashlib
from contextlib import contextmanager
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Iterator, List, Optional, TypeVar, Union, cast

import psutil
from elftools.elf.elffile import ELFError, ELFFile
from elftools.elf.sections import NoteSection
from typing_extensions import ParamSpec

__all__ = ["ELFError"]

P = ParamSpec("P")
R = TypeVar("R")


def wrap_as_nosuchprocess(exc: FileNotFoundError) -> Union[FileNotFoundError, psutil.NoSuchProcess]:
    # Check if filename is /proc/{pid}/*
    if exc.filename.startswith("/proc/"):
        if exc.filename.split("/")[2].isalnum():
            # Take pid from /proc/{pid}/*
            pid = int(exc.filename.split("/")[2])
            # Check if number from /proc/{pid} is actually a pid number
            with open("/proc/sys/kernel/pid_max") as pid_max_file:
                pid_max = int(pid_max_file.read())
            if pid <= pid_max:
                # Check if pid is running
                if not psutil.pid_exists(pid):
                    return psutil.NoSuchProcess(pid)
    return exc


class LibcType(Enum):
    DYNAMIC_GLIBC = auto()
    DYNAMIC_MUSL = auto()
    STATIC_LIBC = auto()
    STATIC_NO_LIBC = auto()


ELFType = Union[ELFFile, Path, str]


@contextmanager
def open_elf(elf: ELFType) -> Iterator[ELFFile]:
    if isinstance(elf, ELFFile):
        yield elf
    else:
        try:
            with open(elf, "rb") as f:
                yield ELFFile(f)
        except FileNotFoundError as e:
            raise wrap_as_nosuchprocess(e) from e


def get_elf_arch(elf: ELFType) -> str:
    """
    Gets the file architecture embedded in the ELF file section
    """
    with open_elf(elf) as elff:
        return elff.get_machine_arch()


def elf_arch_to_uname_arch(arch: str) -> str:
    """
    Translates from the value returned by get_elf_arch to the value you'd receive from "uname -m"
    """
    return {
        "x64": "x86_64",
        "AArch64": "aarch64",
    }[arch]


def get_elf_buildid(elf: ELFType, section: str, note_check: Callable[[Any], bool]) -> Optional[str]:
    """
    Gets the build ID embedded in an ELF file note section as a string,
    or None if not present.
    Lambda argument is used to verify that note meets caller's requirements.
    """
    with open_elf(elf) as elff:
        note_section = elff.get_section_by_name(section)
        if note_section is None or not isinstance(note_section, NoteSection):
            return None

        for note in note_section.iter_notes():
            if note_check(note):
                return cast(str, note.n_desc)
        else:
            return None


def get_elf_id(elf: ELFType) -> str:
    """
    Gets an identifier for this ELF.
    We prefer to use buildids. If a buildid does not exist for an ELF file,
    we instead grab its SHA1.
    """
    with open_elf(elf) as elff:
        buildid = get_elf_buildid(elff, ".note.gnu.build-id", lambda note: note.n_type == "NT_GNU_BUILD_ID")
        if buildid is not None:
            return f"buildid:{buildid}"

        # hash in one chunk
        with open(elff.stream.name, "rb") as f:
            return f"sha1:{hashlib.sha1(f.read()).hexdigest()}"


def read_elf_va(elf: ELFType, va: int, size: int) -> Optional[bytes]:
    with open_elf(elf) as elff:
        for section in elff.iter_sections():
            section_start = section.header.sh_addr
            section_end = section.header.sh_addr + section.header.sh_size
            if section_start <= va and section_end >= va + size:
                offset_from_section = va - section_start
                return section.data()[offset_from_section : offset_from_section + size]
        return None


def read_elf_symbol(elf: ELFType, sym_name: str, size: int) -> Optional[bytes]:
    with open_elf(elf) as elff:
        addr = get_symbol_addr(elff, sym_name)
        if addr is None:
            return None
        return read_elf_va(elff, addr, size)


def is_statically_linked(elf: ELFType) -> bool:
    with open_elf(elf) as elff:
        for segment in elff.iter_segments():
            if segment.header.p_type == "PT_DYNAMIC":
                return False
    return True


def get_symbol_addr(elf: ELFType, sym_name: str) -> Optional[int]:
    with open_elf(elf) as elff:
        symtab = elff.get_section_by_name(".symtab")
        if symtab is None:
            return None
        symbols = symtab.get_symbol_by_name(sym_name)
        if symbols is None:
            return None
        if len(symbols) != 1:
            raise Exception(f"Multiple symbols match the same name: {sym_name!r}")
        return symbols[0].entry.st_value


def get_dt_needed(elf: ELFType) -> Optional[List[str]]:
    with open_elf(elf) as elff:
        dynamic_section = elff.get_section_by_name(".dynamic")
        if dynamic_section is None:
            return None
        return [tag.needed for tag in dynamic_section.iter_tags() if tag.entry.d_tag == "DT_NEEDED"]


def get_libc_type(elf: ELFType) -> LibcType:
    with open_elf(elf) as elff:
        dt_needed = get_dt_needed(elff)
        if dt_needed is not None:
            found_glibc = False
            found_musl = False
            for needed in dt_needed:
                if "libc.so.6" in needed:
                    found_glibc = True
                if "ld-linux" in needed:
                    found_glibc = True
                if "libc.musl" in needed:
                    found_musl = True
                if "ld-musl" in needed:
                    found_musl = True
            if found_glibc and found_musl:
                raise Exception(f"Found both musl and glibc in the same binary: {elff.stream.name!r}")
            if found_musl:
                return LibcType.DYNAMIC_MUSL
            if found_glibc:
                return LibcType.DYNAMIC_GLIBC
            raise Exception(f"Found a dynamic binary without a libc: {elff.stream.name!r}")

        # This symbol exists in both musl and glibc, and is even used by musl to recognize a DSO as a libc.
        # They even comment that it works on both musl and glibc.
        # https://github.com/bminor/musl/blob/dc9285ad1dc19349c407072cc48ba70dab86de45/ldso/dynlink.c#L1143-L1152
        if get_symbol_addr(elff, "__libc_start_main") is not None:
            return LibcType.STATIC_LIBC

        return LibcType.STATIC_NO_LIBC


def elf_is_stripped(elf: ELFType) -> bool:
    with open_elf(elf) as elff:
        return elff.get_section_by_name(".symtab") is None

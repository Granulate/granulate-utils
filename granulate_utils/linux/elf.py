#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import hashlib
from contextlib import contextmanager
from enum import Enum, auto
from functools import wraps
from pathlib import Path
from typing import Callable, List, Optional, TypeVar, Union, cast

import psutil
from elftools.elf.elffile import ELFError, ELFFile  # type: ignore
from elftools.elf.sections import NoteSection  # type: ignore
from typing_extensions import ParamSpec

__all__ = ["ELFError"]

P = ParamSpec("P")
R = TypeVar("R")


def raise_nosuchprocess(func: Callable[P, R]) -> Callable[P, R]:
    @wraps(func)
    def inner(*args: P.args, **kwargs: P.kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            # Check if filename is /proc/{pid}/*
            if e.filename.startswith("/proc/"):
                if e.filename.split("/")[2].isalnum():
                    # Take pid from /proc/{pid}/*
                    pid = int(e.filename.split("/")[2])
                    # Check if number from /proc/{pid} is actually a pid number
                    with open("/proc/sys/kernel/pid_max") as pid_max_file:
                        pid_max = int(pid_max_file.read())
                    if pid <= pid_max:
                        # Check if pid is running
                        if not psutil.pid_exists(pid):
                            raise psutil.NoSuchProcess(pid)
            raise e

    return inner


class LibcType(Enum):
    DYNAMIC_GLIBC = auto()
    DYNAMIC_MUSL = auto()
    STATIC_LIBC = auto()
    STATIC_NO_LIBC = auto()


ELFType = Union[ELFFile, Path, str]


@contextmanager
def open_elf(elf: ELFType) -> ELFFile:
    if isinstance(elf, ELFFile):
        yield elf
    else:
        with open(elf, "rb") as f:
            yield ELFFile(f)


def get_elf_arch(elf: ELFType) -> str:
    """
    Gets the file architecture embedded in the ELF file section
    """
    with open_elf(elf) as elf:
        return elf.get_machine_arch()


def get_elf_buildid(elf: ELFType, section: str, note_check: Callable[[NoteSection], bool]) -> Optional[str]:
    """
    Gets the build ID embedded in an ELF file note section as a hex string,
    or None if not present.
    Lambda argument is used to verify that note meets caller's requirements.
    """
    with open_elf(elf) as elf:
        build_id_section = elf.get_section_by_name(section)
        if build_id_section is None or not isinstance(build_id_section, NoteSection):
            return None

        for note in build_id_section.iter_notes():
            if note_check(note):
                return cast(str, note.n_desc)
        else:
            return None


@raise_nosuchprocess
def get_elf_id(elf: ELFType) -> str:
    """
    Gets an identifier for this ELF.
    We prefer to use buildids. If a buildid does not exist for an ELF file,
    we instead grab its SHA1.
    """
    with open_elf(elf) as elf:
        buildid = get_elf_buildid(elf, ".note.gnu.build-id", lambda note: note.n_type == "NT_GNU_BUILD_ID")
        if buildid is not None:
            return f"buildid:{buildid}"

        # hash in one chunk
        with open(elf.stream.name, "rb") as f:
            return f"sha1:{hashlib.sha1(f.read()).hexdigest()}"


def read_elf_va(elf: ELFType, va: int, size: int) -> Optional[bytes]:
    with open_elf(elf) as elf:
        for section in elf.iter_sections():
            section_start = section.header.sh_addr
            section_end = section.header.sh_addr + section.header.sh_size
            if section_start <= va and section_end >= va + size:
                offset_from_section = va - section_start
                return section.data()[offset_from_section : offset_from_section + size]
        return None


def read_elf_symbol(elf: ELFType, sym_name: str, size: int) -> Optional[bytes]:
    with open_elf(elf) as elf:
        addr = get_symbol_addr(elf, sym_name)
        if addr is None:
            return None
        return read_elf_va(elf, addr, size)


@raise_nosuchprocess
def is_statically_linked(elf: ELFType) -> bool:
    with open_elf(elf) as elf:
        for segment in elf.iter_segments():
            if segment.header.p_type == "PT_DYNAMIC":
                return False
    return True


def get_symbol_addr(elf: ELFType, sym_name: str) -> Optional[int]:
    with open_elf(elf) as elf:
        symtab = elf.get_section_by_name(".symtab")
        if symtab is None:
            return None
        symbols = symtab.get_symbol_by_name(sym_name)
        if symbols is None:
            return None
        if len(symbols) != 1:
            raise Exception(f"Multiple symbols match the same name: {sym_name!r}")
        return symbols[0].entry.st_value


def get_dt_needed(elf: ELFType) -> Optional[List[str]]:
    with open_elf(elf) as elf:
        dynamic_section = elf.get_section_by_name(".dynamic")
        if dynamic_section is None:
            return None
        return [tag.needed for tag in dynamic_section.iter_tags() if tag.entry.d_tag == "DT_NEEDED"]


def get_libc_type(elf: ELFType) -> LibcType:
    with open_elf(elf) as elf:
        dt_needed = get_dt_needed(elf)
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
                raise Exception(f"Found both musl and glibc in the same binary: {elf.stream.name!r}")
            if found_musl:
                return LibcType.DYNAMIC_MUSL
            if found_glibc:
                return LibcType.DYNAMIC_GLIBC
            raise Exception(f"Found a dynamic binary without a libc: {elf.stream.name!r}")

        # This symbol exists in both musl and glibc, and is even used by musl to recognize a DSO as a libc.
        # They even comment that it works on both musl and glibc.
        # https://github.com/bminor/musl/blob/dc9285ad1dc19349c407072cc48ba70dab86de45/ldso/dynlink.c#L1143-L1152
        if get_symbol_addr(elf, "__libc_start_main") is not None:
            return LibcType.STATIC_LIBC

        return LibcType.STATIC_NO_LIBC


@raise_nosuchprocess
def elf_is_stripped(elf: ELFType) -> bool:
    with open_elf(elf) as elf:
        return elf.get_section_by_name(".symtab") is None

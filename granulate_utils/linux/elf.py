#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import hashlib
from typing import Optional, cast

from elftools.elf.elffile import ELFFile  # type: ignore
from elftools.elf.sections import NoteSection  # type: ignore


def get_elf_arch(path: str) -> str:
    """

    Gets the file architacture embedded in an ELF file section
    """
    with open(path, "rb") as f:
        elf = ELFFile(f)
        return elf.get_machine_arch()


def get_elf_buildid(path: str) -> Optional[str]:
    """
    Gets the build ID embedded in an ELF file section as a hex string,
    or None if not present.
    """
    with open(path, "rb") as f:
        elf = ELFFile(f)
        build_id_section = elf.get_section_by_name(".note.gnu.build-id")
        if build_id_section is None or not isinstance(build_id_section, NoteSection):
            return None

        for note in build_id_section.iter_notes():
            if note.n_type == "NT_GNU_BUILD_ID":
                return cast(str, note.n_desc)
        else:
            return None


def get_elf_id(path: str) -> str:
    """
    Gets an identifier for this ELF.
    We prefer to use buildids. If a buildid does not exist for an ELF file,
    we instead grab its SHA1.
    """
    buildid = get_elf_buildid(path)
    if buildid is not None:
        return f"buildid:{buildid}"

    # hash in one chunk
    with open(path, "rb") as f:
        return f"sha1:{hashlib.sha1(f.read()).hexdigest()}"


def _read_va_from_elf(elf: ELFFile, va: int, size: int) -> Optional[bytes]:
    for section in elf.iter_sections():
        section_start = section.header.sh_addr
        section_end = section.header.sh_addr + section.header.sh_size
        if section_start <= va and section_end >= va + size:
            offset_from_section = va - section_start
            return section.data()[offset_from_section : offset_from_section + size]
    return None


def read_elf_va(path: str, va: int, size: int) -> Optional[bytes]:
    with open(path, "rb") as f:
        elf = ELFFile(f)
        return _read_va_from_elf(elf, va, size)


def read_elf_symbol(path: str, sym_name: str, size: int) -> Optional[bytes]:
    with open(path, "rb") as f:
        elf = ELFFile(f)
        symtab = elf.get_section_by_name(".symtab")
        if symtab is None:
            return None
        symbols = symtab.get_symbol_by_name(sym_name)
        if symbols is None or len(symbols) != 1:
            return None
        return _read_va_from_elf(elf, symbols[0].entry.st_value, size)


def is_statically_linked(path: str) -> bool:
    with open(path, "rb") as f:
        elf = ELFFile(f)
        for segment in elf.iter_segments():
            if segment.header.p_type == "PT_DYNAMIC":
                return False
    return True

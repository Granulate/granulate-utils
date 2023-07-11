#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from psutil import Process

from granulate_utils.linux.process import is_kernel_thread, is_process_basename_matching
from granulate_utils.exceptions import NotANodeProcess


def is_node_process(process: Process) -> bool:
    try:
        return not is_kernel_thread(process) and is_process_basename_matching(process, r"^node$")
    except:
        raise NotANodeProcess(process)

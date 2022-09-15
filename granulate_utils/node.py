#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

from psutil import Process

from granulate_utils.linux.process import is_process_basename_matching


def is_node_process(process: Process) -> bool:
    return is_process_basename_matching(process, r"^node$")

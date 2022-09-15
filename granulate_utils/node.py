#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#

import os

from psutil import Process

from granulate_utils.linux.process import process_exe


def is_node_process(process: Process) -> bool:
    return os.path.basename(process_exe(process)) == "node"

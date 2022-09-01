#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import logging
import sys


# Use the stdout logger because we don't want to log
# recursively from inside the logger implementation
def get_stdout_logger():
    stdout_logger = logging.getLogger(__name__ + "_stdout")
    stdout_logger.propagate = False
    stdout_logger.addHandler(logging.StreamHandler(sys.stdout))
    stdout_logger.setLevel(logging.INFO)
    return stdout_logger

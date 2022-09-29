#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import logging
import sys

_LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


# Use the stdout logger because we don't want to log
# recursively from inside the logger implementation
def get_stdout_logger():
    stdout_logger = logging.getLogger(__name__ + "_stdout")
    stdout_logger.propagate = False
    formatter = logging.Formatter(_LOGGING_FORMAT)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_logger.addHandler(stdout_handler)
    stdout_logger.setLevel(logging.INFO)
    return stdout_logger

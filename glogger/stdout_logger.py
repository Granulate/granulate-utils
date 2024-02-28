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

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
from abc import ABC, abstractmethod
from typing import Iterable, Tuple, Type

from granulate_utils.linux import get_kernel_release

KernelMessage = Tuple[float, int, str]


class KernelMessagesProvider(ABC):
    @abstractmethod
    def iter_new_messages(self) -> Iterable[KernelMessage]:
        pass

    def on_missed(self):
        """Gets called when some kernel messages are missed."""
        pass


class EmptyKernelMessagesProvider(KernelMessagesProvider):
    def iter_new_messages(self):
        return []


DefaultKernelMessagesProvider: Type[KernelMessagesProvider]

if get_kernel_release() >= (3, 5):
    from granulate_utils.linux.devkmsg import DevKmsgProvider

    DefaultKernelMessagesProvider = DevKmsgProvider
else:
    DefaultKernelMessagesProvider = EmptyKernelMessagesProvider

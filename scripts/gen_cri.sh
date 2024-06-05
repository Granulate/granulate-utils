#!/bin/bash
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
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GENERATED_DIR="$SCRIPT_DIR/../granulate_utils/generated"

protoc() {
    python3 -m grpc_tools.protoc "$@"
}

mkdir -p "$GENERATED_DIR/containers/cri"
cd "$GENERATED_DIR"
touch __init__.py containers/__init__.py containers/cri/__init__.py

cd containers/cri/
# released at Oct 26, 2018
wget -O gogo.proto https://raw.githubusercontent.com/gogo/protobuf/v1.3.2/gogoproto/gogo.proto
protoc -I. --python_out=. gogo.proto

mkdir -p v1 v1alpha2

# Support kubernetes 1.22-1.28
# 1.22 is the last version with v1alpha2 API, corresponding to containerd v1.5
# all containerd's v1.6+ support v1 API regardless of kubernetes version
base_url=https://raw.githubusercontent.com/kubernetes/cri-api/kubernetes-1.25.16/pkg/apis/runtime
wget -O v1/api.proto "$base_url/v1/api.proto"
wget -O v1alpha2/api.proto "$base_url/v1alpha2/api.proto"
# patch gogo import:
# '.bak' needed for BSD sed on Mac
sed -i'.bak' s,github.com/gogo/protobuf/gogoproto/gogo.proto,gogo.proto, v1/api.proto v1alpha2/api.proto
protoc -I. --python_out=. --grpc_python_out=. v1/api.proto v1alpha2/api.proto
# patch imports in generated code:
sed -i'.bak' "s,import gogo_pb2,import granulate_utils.generated.containers.cri.gogo_pb2," v1/api_pb2.py v1alpha2/api_pb2.py
sed -i'.bak' 's,from v1,from granulate_utils.generated.containers.cri.v1,' v1/api_pb2_grpc.py
sed -i'.bak' 's,from v1alpha2,from granulate_utils.generated.containers.cri.v1alpha2,' v1alpha2/api_pb2_grpc.py
rm gogo.proto */*.proto */*.bak

# when importing v1/v1alpha2 immediately import the generated modules:
printf "from . import api_pb2, api_pb2_grpc\n" > v1/__init__.py
printf "from . import api_pb2, api_pb2_grpc\n" > v1alpha2/__init__.py

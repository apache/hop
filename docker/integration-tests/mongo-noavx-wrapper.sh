#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Installed in place of a MongoDB binary. The real binary is
# /usr/lib/hop-it/<basename>. Hosts with AVX run it directly. Hosts without
# AVX (and HOP_MONGO_FORCE_QEMU=1) run it under qemu-user, which emulates the
# AVX instructions MongoDB 5+ is built with. Without this, mongod dies with
# SIGILL (exit 132) on those agents.

name=$(basename "$0")
real="/usr/lib/hop-it/${name}"

# AVX is an x86 feature. On other architectures (arm64 on Apple Silicon, Graviton) the image
# ships a native binary and /proc/cpuinfo has no "flags" line, so always run it directly.
if [ "$(uname -m)" != "x86_64" ]; then
  exec "$real" "$@"
fi

if [ "${HOP_MONGO_FORCE_QEMU:-}" != "1" ] && grep -qE '^flags.* avx( .*|$)' /proc/cpuinfo; then
  exec "$real" "$@"
fi

echo "hop-it: starting ${name} under qemu-x86_64 because AVX is not available" >&2
exec /usr/bin/qemu-x86_64-static -cpu max "$real" "$@"

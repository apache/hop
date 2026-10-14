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
# MongoDB 5+ is built with AVX. On a CPU that does not advertise that flag,
# the official image's mongod dies with SIGILL (exit 132) and the mongo
# integration tests never start. qemu-user emulates those instructions when
# AVX is missing. Hosts that have AVX keep the native binary.

FROM debian:stable-slim AS qemu

RUN apt-get update \
    && apt-get install -y --no-install-recommends qemu-user-static \
    && rm -rf /var/lib/apt/lists/*

FROM mongo:8

USER root

COPY --from=qemu /usr/bin/qemu-x86_64-static /usr/bin/qemu-x86_64-static
COPY mongo-noavx-wrapper.sh /usr/lib/hop-it/mongo-noavx-wrapper.sh

# The official entrypoint starts "mongod" by PATH. /usr/local/bin is before
# /usr/bin, but replacing the binary itself also covers an absolute exec.
# mongosh is a Node.js shell and does not need this wrapper.
RUN mv /usr/bin/mongod /usr/lib/hop-it/mongod \
    && install -m 755 /usr/lib/hop-it/mongo-noavx-wrapper.sh /usr/bin/mongod \
    && chmod 755 /usr/bin/qemu-x86_64-static

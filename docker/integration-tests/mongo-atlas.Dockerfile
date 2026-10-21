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
# Same AVX workaround as mongo.Dockerfile. The Atlas Local image runs
# MongoDB 8, which dies with SIGILL (exit 132) on a CPU without AVX.
# mongot is a Java process with a scalar fallback, so only mongod is wrapped.
# The image's runner looks mongod up on PATH and execs /usr/bin/mongod.

FROM debian:stable-slim AS qemu

RUN apt-get update \
    && apt-get install -y --no-install-recommends qemu-user-static \
    && rm -rf /var/lib/apt/lists/*

FROM mongodb/mongodb-atlas-local

USER root

COPY --from=qemu /usr/bin/qemu-x86_64-static /usr/bin/qemu-x86_64-static
COPY mongo-noavx-wrapper.sh /usr/lib/hop-it/mongo-noavx-wrapper.sh

RUN mv /usr/bin/mongod /usr/lib/hop-it/mongod \
    && install -m 755 /usr/lib/hop-it/mongo-noavx-wrapper.sh /usr/bin/mongod \
    && chmod 755 /usr/bin/qemu-x86_64-static

USER mongod

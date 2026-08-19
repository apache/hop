#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Sidecar X server for Hop SWT UI tests. Maven, JDK and GTK stay on the host;
# this image only provides a virtual display the host Surefire JVMs can use.
FROM debian:bookworm-slim

LABEL maintainer="Apache Hop Team"

ENV DEBIAN_FRONTEND=noninteractive \
    DISPLAY_NUM=99

RUN apt-get update \
  && apt-get install --no-install-recommends --assume-yes xvfb \
  && rm -rf /var/lib/apt/lists/*

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod 755 /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]

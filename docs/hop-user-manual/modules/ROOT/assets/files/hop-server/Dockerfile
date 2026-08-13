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

# Sample image: copy a Hop project into the official long-lived Hop Server image.
# Pin a released Hop version. Do not use "latest" in production.
# Build from the root of your project repository:
#   docker build -t registry.example.com/your-project-hop-server:${GIT_SHA} .

FROM apache/hop:2.20.0

COPY --chown=hop:hop . /your-project

# Optional extra JDBC drivers the official image does not ship:
# COPY --chown=hop:hop lib/jdbc/*.jar /opt/hop/lib/jdbc/

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

# Single-node HDFS + YARN for the native Spark engine.
# Hadoop runs on the image's own Java; YARN containers (Spark AM and executors) run on the
# Java 21 runtime in /opt/java21, which the Hop classes in the fat jar need.

# apache/hadoop only publishes linux/amd64. Pin the Java stage to the same platform, or on an arm64
# host (Apple Silicon) it pulls an arm64 Java that the emulated amd64 container cannot run.
FROM --platform=linux/amd64 eclipse-temurin:21-jre AS java21

FROM --platform=linux/amd64 apache/hadoop:3.4.2
COPY --from=java21 /opt/java/openjdk /opt/java21
COPY --chmod=755 start.sh /opt/start-hadoop-yarn.sh
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
CMD ["/opt/start-hadoop-yarn.sh"]

#!/usr/bin/env bash
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

# Starts HDFS (namenode + datanode) and YARN (resourcemanager + nodemanager) in one container.
set -euo pipefail

if [ ! -d /tmp/hadoop/dfs/name/current ]; then
  hdfs namenode -format -force -nonInteractive
fi
hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager

# Wait for HDFS, then create the folders Spark and the tests use
until hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; do sleep 2; done
hdfs dfs -mkdir -p /user /tmp /data /output
hdfs dfs -chmod -R 1777 /user /tmp /data /output
echo "HDFS and YARN are up"

tail -F "${HADOOP_LOG_DIR:-/opt/hadoop/logs}"/*.log

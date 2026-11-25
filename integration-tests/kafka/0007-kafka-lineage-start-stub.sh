#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Starts the OpenLineage collector and DataHub stand-in of main-0007-kafka-lineage on port 18081,
# where the metadata of the openlineage-stub sink and the datahub-stub data catalog point.
LOG=/tmp/kafka-lineage-stub.log
if [ -f /tmp/kafka-lineage-stub.pid ]; then
  # Left running by an earlier run in this container
  kill "$(cat /tmp/kafka-lineage-stub.pid)" 2>/dev/null
  sleep 1
fi
rm -f "${LOG}"
touch "${LOG}"
nohup python3 -I "$(dirname "$0")/0007-kafka-lineage-stub.py" 18081 "${LOG}" > /tmp/kafka-lineage-stub.out 2>&1 &
echo $! > /tmp/kafka-lineage-stub.pid

for i in $(seq 1 30); do
  if curl -s -o /dev/null "http://127.0.0.1:18081/"; then
    echo "Lineage stub is up on port 18081"
    exit 0
  fi
  sleep 1
done
echo "The lineage stub did not start:"
cat /tmp/kafka-lineage-stub.out
exit 1

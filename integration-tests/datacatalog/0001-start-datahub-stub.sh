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

# Starts the DataHub GraphQL stub (datahub-stub.py) in the background and waits until it answers.

PORT="${DATAHUB_STUB_PORT:-18080}"
LOG=/tmp/datahub-stub-requests.log
rm -f "${LOG}"
nohup python3 -I "$(dirname "$0")/datahub-stub.py" "${PORT}" "${LOG}" > /tmp/datahub-stub.out 2>&1 &
echo $! > /tmp/datahub-stub.pid

for i in $(seq 1 30); do
  if curl -s -o /dev/null "http://127.0.0.1:${PORT}/"; then
    echo "DataHub stub is up on port ${PORT}"
    exit 0
  fi
  sleep 1
done
echo "The DataHub stub did not start:"
cat /tmp/datahub-stub.out
exit 1

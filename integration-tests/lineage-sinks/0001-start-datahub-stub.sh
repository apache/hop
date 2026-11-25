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

# Starts the DataHub stand-in (datahub-ingest-stub.py) in the background and waits until it answers.
# Argument: the java.io.tmpdir of the Hop run. The file the unit test's child workflow writes there
# is removed first, so the check knows that workflow ran.

PORT="${DATAHUB_STUB_PORT:-18080}"
rm -f "${1:-/tmp}/lineage-sinks-it/unit-test-child-workflow.txt"
LOG=/tmp/lineage-sinks-proposals.log
OPENLINEAGE_LOG=/tmp/lineage-sinks-openlineage.log
rm -f "${LOG}" "${OPENLINEAGE_LOG}"
touch "${LOG}" "${OPENLINEAGE_LOG}"
nohup python3 -I "$(dirname "$0")/datahub-ingest-stub.py" "${PORT}" "${LOG}" "${OPENLINEAGE_LOG}" > /tmp/lineage-sinks-stub.out 2>&1 &
echo $! > /tmp/lineage-sinks-stub.pid

for i in $(seq 1 30); do
  if curl -s -o /dev/null "http://127.0.0.1:${PORT}/"; then
    echo "DataHub stand-in is up on port ${PORT}"
    exit 0
  fi
  sleep 1
done
echo "The DataHub stand-in did not start:"
cat /tmp/lineage-sinks-stub.out
exit 1

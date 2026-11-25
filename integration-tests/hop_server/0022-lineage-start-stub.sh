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

# Starts the OpenLineage collector of main-0022-lineage-on-hop-server on port 18092. The Hop Server
# reaches this container as lineage-stub (a network alias), where the openlineage-stub sink points.
LOG=/tmp/hop-server-lineage-stub.log
PID_FILE=/tmp/hop-server-lineage-stub.pid
if [ -f "${PID_FILE}" ]; then
  # Left running by an earlier run in this container. Only stopped when that pid still is the stub:
  # the stub may have died and its pid gone to another process.
  OLD_PID="$(cat "${PID_FILE}")"
  if [ -n "${OLD_PID}" ] && tr '\0' ' ' < "/proc/${OLD_PID}/cmdline" 2>/dev/null | grep -q "0022-lineage-stub.py"; then
    kill "${OLD_PID}" 2>/dev/null
    for i in $(seq 1 10); do
      kill -0 "${OLD_PID}" 2>/dev/null || break
      sleep 1
    done
  fi
  rm -f "${PID_FILE}"
fi
# Port 18092 has to be free, or the new stub can't bind and the health check below reaches an old one
for i in $(seq 1 10); do
  curl -s -o /dev/null "http://127.0.0.1:18092/" || break
  if [ "${i}" = 10 ]; then
    echo "Port 18092 is still in use, the lineage stub can't start"
    exit 1
  fi
  sleep 1
done
rm -f "${LOG}"
touch "${LOG}"
nohup python3 -I "$(dirname "$0")/0022-lineage-stub.py" 18092 "${LOG}" > /tmp/hop-server-lineage-stub.out 2>&1 &
echo $! > "${PID_FILE}"

for i in $(seq 1 30); do
  if ! kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
    break
  fi
  if curl -s -o /dev/null "http://127.0.0.1:18092/"; then
    echo "Lineage stub is up on port 18092"
    exit 0
  fi
  sleep 1
done
echo "The lineage stub did not start:"
cat /tmp/hop-server-lineage-stub.out
exit 1

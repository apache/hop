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
#

# Runs a pipeline that only ends when it is stopped, sends hop-run SIGTERM and checks that the
# end of the run still reaches Marquez: the run may not be left RUNNING there (issue #9168).
#
# Arguments: <Marquez base URL> <OpenLineage namespace>

set -u

MARQUEZ_API="${1}"
NAMESPACE="${2}"
JOB="0012-openlineage-sigterm"
RUNS_URL="${MARQUEZ_API}/api/v1/namespaces/${NAMESPACE}/jobs/${JOB}/runs?limit=1"
WORK="$(mktemp -d)"
LOG="${WORK}/hop-run.log"

fail() {
  echo "FAILED: ${1}"
  echo "----- lineage environment -----"
  env | grep '^HOP_LINEAGE' | sort
  echo "----- hop-run output -----"
  cat "${LOG}"
  kill -KILL "${PID}" 2>/dev/null
  exit 1
}

# The state and id of the most recent run of the job in Marquez, empty when there is none.
latestRun() {
  curl -s "${RUNS_URL}" |
    python3 -c 'import json, sys
try:
    runs = json.load(sys.stdin).get("runs", [])
except ValueError:
    runs = []
print(runs[0]["state"] + " " + runs[0]["id"] if runs else "")'
}

# The Shell action hands this script the Hop variables of the workflow, which replace the lineage
# settings of the container (docker/integration-tests/integration-tests-openlineage.yaml) with
# their defaults. Set the two that switch the OpenLineage sink on again.
export HOP_LINEAGE_ENABLED=Y
export HOP_LINEAGE_SINK_IDS=openlineage

BEFORE="$(latestRun)"

hop-run.sh -e dev -r local -l Basic -f "${PROJECT_HOME}/${JOB}.hpl" >"${LOG}" 2>&1 &
PID=$!

# Wait until Marquez has the START of this run.
RUN=""
for _ in $(seq 1 120); do
  kill -0 "${PID}" 2>/dev/null || fail "hop-run ended before the run reached Marquez"
  LATEST="$(latestRun)"
  if [ -n "${LATEST}" ] && [ "${LATEST}" != "${BEFORE}" ] && [ "${LATEST%% *}" = "RUNNING" ]; then
    RUN="${LATEST#* }"
    break
  fi
  sleep 1
done
[ -n "${RUN}" ] || fail "Marquez did not show the run as RUNNING within 120s"

kill -TERM "${PID}"
wait "${PID}"
CODE=$?
[ "${CODE}" -eq 143 ] || fail "the exit code is ${CODE}, expected 143"

# hop-run delivered its lineage before it exited; give Marquez a moment to store it.
STATE=""
for _ in $(seq 1 10); do
  LATEST="$(latestRun)"
  if [ "${LATEST#* }" = "${RUN}" ]; then
    STATE="${LATEST%% *}"
    [ "${STATE}" != "RUNNING" ] && break
  fi
  sleep 1
done
case "${STATE}" in
COMPLETED | ABORTED | FAILED) ;;
*) fail "run ${RUN} is '${STATE}' in Marquez after hop-run exited, the end of the run was lost" ;;
esac

echo "Marquez has run ${RUN} of ${JOB} as ${STATE} after SIGTERM"

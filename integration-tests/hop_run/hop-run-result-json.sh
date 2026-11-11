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

# Runs the hop launcher with --result-json on a pipeline that aborts with an error, on one that
# finishes, and on a workflow that aborts on the failure hop of a failing pipeline, and checks what
# the result files say (issue #9417).

set -u

# The integration-test image only marks *.sh executable. hop sits next to hop-run.sh.
HOP="$(dirname "$(command -v hop-run.sh)")/hop"
RESULT_DIR="$(mktemp -d)"

fail() {
  echo "FAILED: ${1}"
  if [ -n "${LOG:-}" ] && [ -f "${LOG}" ]; then
    echo "----- output -----"
    cat "${LOG}"
  fi
  if [ -n "${RESULT:-}" ] && [ -f "${RESULT}" ]; then
    echo "----- ${RESULT} -----"
    cat "${RESULT}"
  fi
  exit 1
}

# <file> <result file>
run_hop() {
  local file="${1}"
  RESULT="${2}"
  LOG="$(mktemp)"
  echo "hop run -r local -f ${file} --result-json ${RESULT}"
  if [ -x "${HOP}" ]; then
    "${HOP}" run -e dev -r local -l Basic -f "${PROJECT_HOME}/${file}" --result-json "${RESULT}" >"${LOG}" 2>&1
  else
    bash "${HOP}" run -e dev -r local -l Basic -f "${PROJECT_HOME}/${file}" --result-json "${RESULT}" >"${LOG}" 2>&1
  fi
  [ -s "${RESULT}" ] || fail "hop run ${file} wrote no result file"
}

# <python expression on the result r> <what it means>. root(f) follows causedBy to the end.
check() {
  python3 -I -c '
import json, sys
r = json.load(open(sys.argv[1]))
def root(f):
    while f.get("causedBy"):
        f = f["causedBy"]
    return f
sys.exit(0 if eval(sys.argv[2]) else 1)
' "${RESULT}" "${1}" || fail "${RESULT} does not say ${2}"
}

[ -f "${HOP}" ] || fail "hop launcher not found next to hop-run.sh"

run_hop "0004-abort.hpl" "${RESULT_DIR}/abort.json"
check 'r["success"] is False' "that the run failed"
check 'r["type"] == "PIPELINE"' "that it was a pipeline"
check 'r["failure"]["phase"] == "RUN"' "in which phase it failed"
check 'root(r["failure"])["component"]["kind"] == "TRANSFORM"' "that a transform failed"
check 'root(r["failure"])["component"]["name"] == "Abort"' "which transform failed"
check 'root(r["failure"])["component"]["pluginId"] == "Abort"' "the plugin of the failed transform"
check '"Aborting the pipeline on purpose" in root(r["failure"])["cause"]["message"]' "the message the transform logged"
check 'root(r["failure"])["cause"]["redacted"] is True' "that the cause was redacted"

run_hop "0004-pass.hpl" "${RESULT_DIR}/pass.json"
check 'r["success"] is True' "that the run succeeded"
check 'r["failure"] is None' "that there was no failure"
check 'r["failures"] == []' "that nothing failed"

# The usual error handling: Abort on the failure hop. The root cause is the transform that failed
# in the pipeline, not the Abort action.
run_hop "0005-abort-after-failure.hwf" "${RESULT_DIR}/workflow.json"
check 'r["success"] is False' "that the workflow failed"
check 'r["type"] == "WORKFLOW"' "that it was a workflow"
check '[f["component"]["name"] for f in r["failures"]] == ["Run the aborting pipeline", "Abort the workflow"]' "that the Pipeline action and the Abort action failed"
check 'r["failure"]["causedBy"]["component"]["name"] == "Abort the workflow"' "that the workflow ended at the Abort action"
check 'r["failure"]["causedBy"]["causedBy"]["component"]["name"] == "Run the aborting pipeline"' "that the Abort action points at the Pipeline action"
check 'root(r["failure"])["component"]["kind"] == "TRANSFORM"' "that the failure started in a transform"
check 'root(r["failure"])["component"]["pluginId"] == "Abort"' "which transform it started in"
check 'r["failure"]["category"] == root(r["failure"])["category"]' "the category of the root cause"

rm -rf "${RESULT_DIR}"
echo "hop run --result-json says why the pipeline and the workflow failed, and that the passing pipeline succeeded"

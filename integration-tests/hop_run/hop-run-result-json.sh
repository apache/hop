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

# Runs the hop launcher with --result-json on a pipeline that aborts with an error and on one that
# finishes, and checks what the two result files say (issue #9417).

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

# <pattern> <what it means>
expect() {
  grep -Eq "${1}" "${RESULT}" || fail "${RESULT} does not say ${2}"
}

[ -f "${HOP}" ] || fail "hop launcher not found next to hop-run.sh"

run_hop "0004-abort.hpl" "${RESULT_DIR}/abort.json"
expect '"success" *: *false' "that the run failed"
expect '"type" *: *"PIPELINE"' "that it was a pipeline"
expect '"name" *: *"Abort"' "which transform failed"
expect '"pluginId" *: *"Abort"' "the plugin of the failed transform"
expect '"phase" *: *"RUN"' "in which phase it failed"
expect 'Aborting the pipeline on purpose' "the message the transform logged"

run_hop "0004-pass.hpl" "${RESULT_DIR}/pass.json"
expect '"success" *: *true' "that the run succeeded"
expect '"failure" *: *null' "that there was no failure"

rm -rf "${RESULT_DIR}"
echo "hop run --result-json says why the aborting pipeline failed and that the passing one succeeded"

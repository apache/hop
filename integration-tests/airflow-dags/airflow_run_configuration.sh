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

# Runs workflows of airflow-gui-project with its "airflow" run configuration, as Hop GUI does:
# the engine publishes the project to gui-bundle/, which the Airflow container reads as a DAG
# bundle, starts a DAG run and follows it. Checks the outcome, the parameter that reaches the run,
# and that the log of the Airflow task shows up in Hop's log.

set -u

HOP="$(dirname "$(command -v hop-run.sh)")/hop"
PROJECT="${PROJECT_HOME}/airflow-gui-project"
RESULT_DIR="$(mktemp -d)"

fail() {
  echo "FAILED: ${1}"
  if [ -n "${LOG:-}" ] && [ -f "${LOG}" ]; then
    echo "----- output -----"
    cat "${LOG}"
  fi
  exit 1
}

# <workflow> <expected exit code> [parameter...]
run_on_airflow() {
  local file="${1}" expected="${2}"
  shift 2
  local params=()
  for p in "$@"; do
    params+=(-p "${p}")
  done
  RESULT="${RESULT_DIR}/${file%.hwf}.json"
  LOG="$(mktemp)"
  echo "hop run -r airflow -f ${file} $*"
  # hop run splits -p on commas by default; the greeting has one, which has to reach the DAG run
  bash "${HOP}" --project-locations airflowgui="${PROJECT}" run -j airflowgui -r airflow -l Basic \
    -f "${PROJECT}/${file}" -ps ';' "${params[@]}" --result-json "${RESULT}" >"${LOG}" 2>&1
  local code=$?
  [ "${code}" = "${expected}" ] || fail "hop run ${file} exited with ${code}, not ${expected}"
}

# <text> : the output of the last run contains it
contains() {
  grep -qF -- "${1}" "${LOG}" || fail "the output does not contain: ${1}"
}

run_on_airflow check-greeting.hwf 0 "PRM_GREETING=hello, airflow"
contains "Published project airflowgui as a DAG bundle"
contains "Started run "
# Lines of the task log in Airflow, as the workflow on the worker wrote them
contains "check-greeting - Finished action [Check the greeting] (result=[true])"
contains "The run of DAG airflowgui__check-greeting ended: success"
# The project has parent project 'default', which the workers' hop-config.json lists with a home
# relative to the Hop installation, as the Hop client ships it
if grep -qF "Could not load parent project" "${LOG}"; then
  fail "the workers did not find parent project 'default'"
fi
[ -f "${PROJECT_HOME}/gui-bundle/hop-airflow.jar" ] || fail "no stub in gui-bundle"

run_on_airflow check-greeting.hwf 1 "PRM_GREETING=hello, somebody else"
contains "PRM_GREETING is 'hello, somebody else', not 'hello, airflow'"

# Without a value for the run, the parameter value of the Airflow DAG object applies, not the
# empty default of the workflow
run_on_airflow check-greeting.hwf 0
contains "Runs Airflow DAG check-greeting, DAG id airflowgui__check-greeting"
contains "check-greeting - Finished action [Check the greeting] (result=[true])"

run_on_airflow fails-on-airflow.hwf 1
contains "This workflow fails on purpose"
contains "The run of DAG airflowgui__fails-on-airflow ended: failed"

# Unpausing a DAG with a schedule would also start the schedule: the run configuration leaves it
# paused and starts no run
run_on_airflow scheduled-paused.hwf 1
contains "DAG airflowgui__scheduled-paused is paused and has a schedule"
if grep -qF "Started run " "${LOG}"; then
  fail "a run of the paused DAG with a schedule was started"
fi

echo "The Airflow run configuration published, ran and followed the workflows as expected"

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

# Runs the hop launcher (hop → Hop.main → System.exit) on a pipeline that aborts with an error
# and expects exit code 1, then on a pipeline that finishes and expects 0 (issue #9355).
# hop-run.sh already returns that code from HopRun.main; this checks the hop run command.

set -u

# The integration-test image only marks *.sh executable. hop sits next to hop-run.sh.
HOP="$(dirname "$(command -v hop-run.sh)")/hop"

fail() {
  echo "FAILED: ${1}"
  if [ -n "${LOG:-}" ] && [ -f "${LOG}" ]; then
    echo "----- output -----"
    cat "${LOG}"
  fi
  exit 1
}

# <file> <expected exit code>
run_hop() {
  local file="${1}"
  local expected="${2}"
  LOG="$(mktemp)"
  echo "hop run -r local -f ${file}, expecting exit code ${expected}"
  if [ -x "${HOP}" ]; then
    "${HOP}" run -e dev -r local -l Basic -f "${PROJECT_HOME}/${file}" >"${LOG}" 2>&1
  else
    bash "${HOP}" run -e dev -r local -l Basic -f "${PROJECT_HOME}/${file}" >"${LOG}" 2>&1
  fi
  local code=$?
  if [ "${code}" -ne "${expected}" ]; then
    fail "hop run ${file} exited ${code}, expected ${expected}"
  fi
  rm -f "${LOG}"
}

[ -f "${HOP}" ] || fail "hop launcher not found next to hop-run.sh"

run_hop "0004-abort.hpl" 1
run_hop "0004-pass.hpl" 0

echo "hop run exit code is 1 for the aborting pipeline and 0 for the passing pipeline"

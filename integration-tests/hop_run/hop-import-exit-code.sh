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

# Runs the hop launcher (hop → Hop.main → System.exit) on import commands that stop before
# importing anything and expects exit code 1, then on an empty folder and expects 0 (issue #9441).
# hop-import.sh already returns 1 from HopImport.main; this checks the hop import command.

set -u

# The integration-test image only marks *.sh executable. hop sits next to hop-run.sh.
BIN="$(dirname "$(command -v hop-run.sh)")"
HOP="${BIN}/hop"
HOP_IMPORT="${BIN}/hop-import.sh"

fail() {
  echo "FAILED: ${1}"
  if [ -n "${LOG:-}" ] && [ -f "${LOG}" ]; then
    echo "----- output -----"
    cat "${LOG}"
  fi
  exit 1
}

# <expected exit code> <launcher> [args...]
launch() {
  local expected="${1}"
  shift
  local shown="$*"
  local launcher="${1}"
  shift
  LOG="$(mktemp)"
  echo "${shown}, expecting exit code ${expected}"
  if [ -x "${launcher}" ]; then
    "${launcher}" "$@" >"${LOG}" 2>&1
  else
    bash "${launcher}" "$@" >"${LOG}" 2>&1
  fi
  local code=$?
  if [ "${code}" -ne "${expected}" ]; then
    fail "${shown} exited ${code}, expected ${expected}"
  fi
  rm -f "${LOG}"
  LOG=""
}

[ -f "${HOP}" ] || fail "hop launcher not found next to hop-run.sh"
[ -f "${HOP_IMPORT}" ] || fail "hop-import.sh not found next to hop-run.sh"

WORK="$(mktemp -d)"
IN="${WORK}/in"
OUT="${WORK}/out"
mkdir -p "${IN}" "${OUT}"
cleanup() {
  rm -rf "${WORK}"
}
trap cleanup EXIT

launch 1 "${HOP}" import
launch 1 "${HOP}" import -i "${IN}"
launch 1 "${HOP}" import -t nosuchtype -i "${IN}" -o "${OUT}"
launch 1 "${HOP_IMPORT}"
launch 1 "${HOP}" import -i "${WORK}/does-not-exist" -o "${OUT}"
launch 0 "${HOP}" import -t kettle -i "${IN}" -o "${OUT}"

echo "hop import exit code is 1 when nothing is imported and 0 when an empty folder is imported"

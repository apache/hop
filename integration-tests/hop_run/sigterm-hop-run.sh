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

# Starts hop-run.sh on a pipeline or workflow ($1) that only ends when it is stopped, sends it
# SIGTERM and checks that hop-run stopped the run before it exited (issue #9168).

set -u

FILE="${1}"
WORK="$(mktemp -d)"
LOG="${WORK}/hop-run.log"

hop-run.sh -e dev -r local -l Basic -f "${PROJECT_HOME}/${FILE}" >"${LOG}" 2>&1 &
PID=$!

# shellcheck source=/dev/null
. "${PROJECT_HOME}/sigterm-checks.sh"

waitForLog "${LOG}" "Starting (pipeline|workflow): " 120

# hop-run.sh execs the JVM, so the signal goes to the JVM itself.
COMMAND="$(ps -o comm= -p "${PID}")"
[ "${COMMAND}" = "java" ] || fail "hop-run.sh did not exec java, the process is '${COMMAND}'"

# Let the run get going.
sleep 3

START=$(date +%s)
kill -TERM "${PID}"
wait "${PID}"
CODE=$?
ELAPSED=$(($(date +%s) - START))

checkStopped "${LOG}" "${CODE}" "${ELAPSED}"
echo "hop-run stopped ${FILE} in ${ELAPSED}s after SIGTERM"

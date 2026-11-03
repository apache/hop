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

# Checks shared by the SIGTERM tests. Expects LOG, PID and FILE to be set.

fail() {
  echo "FAILED: ${1}"
  echo "----- output -----"
  cat "${LOG}"
  kill -KILL "${PID}" 2>/dev/null
  pkill -KILL -f "org.apache.hop.run.HopRun.*${FILE}" 2>/dev/null
  exit 1
}

# waitForLog <log file> <extended regular expression> <seconds>
waitForLog() {
  local waited=0
  until grep -q -E "${2}" "${1}"; do
    kill -0 "${PID}" 2>/dev/null || fail "the process ended before '${2}' was logged"
    [ "${waited}" -lt "${3}" ] || fail "'${2}' was not logged within ${3}s"
    sleep 1
    waited=$((waited + 1))
  done
}

# checkStopped <log file> <exit code> <seconds it took to stop>
checkStopped() {
  # 128 + 15: the JVM exits with the status of the signal that ended it.
  [ "${2}" -eq 143 ] || fail "the exit code is ${2}, expected 143"
  grep -q "Termination signal received, stopping the run" "${1}" ||
    fail "hop-run did not log that it got the termination signal"
  grep -q "The run has stopped, hop-run exits." "${1}" ||
    fail "hop-run did not log that the run stopped"
  [ "${3}" -lt 20 ] || fail "stopping took ${3}s"
  if pgrep -f "org.apache.hop.run.HopRun.*${FILE}" >/dev/null; then
    fail "the hop-run JVM is still running"
  fi
}

#!/usr/bin/env bash
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
# Run the execution test for every demo identity and summarise.
#   ./run-matrix.sh [base-url]
BASE="${1:-http://localhost:8080}"
declare -a RESULTS
for id in "" viewer:viewer operator:operator developer:developer admin:admin; do
  label="${id%%:*}"; [[ -z "${id}" ]] && label="anonymous"
  if ./exec-test.sh "${BASE}" "${id}" >/tmp/m.$$ 2>&1; then
    RESULTS+=("${label}|EXECUTED")
  else
    RESULTS+=("${label}|blocked")
  fi
done
rm -f /tmp/m.$$
echo
printf '%-12s %-12s %s\n' IDENTITY 'HOP ROLE' 'CAN RUN A PIPELINE VIA /hop/*?'
printf '%-12s %-12s %s\n' ------------ ------------ ------------------------------
declare -A ROLE=([anonymous]="-" [viewer]="Read-only" [operator]="Operator" [developer]="User" [admin]="Admin")
declare -A UI=([anonymous]="-" [viewer]="NO (no run)" [operator]="yes" [developer]="yes" [admin]="yes")
for r in "${RESULTS[@]}"; do
  who="${r%%|*}"; verdict="${r##*|}"
  printf '%-12s %-12s %s\n' "${who}" "${ROLE[$who]}" "${verdict}   [UI allows run: ${UI[$who]}]"
done
echo

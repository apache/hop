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
# Probe the Hop Server API surface exposed by a Hop Web deployment.
#
#   ./probe-api.sh <base-url> [user:password]
#
# Reports, for every /hop/* endpoint, whether it is reachable and whether the
# response is a login redirect / 401 (auth enforced) or real server output.
set -uo pipefail

BASE="${1:-http://localhost:8080}"
CREDS="${2:-}"
AUTH=(); LABEL="anonymous"
[[ -n "${CREDS}" ]] && { AUTH=(-u "${CREDS}"); LABEL="${CREDS%%:*}"; }

ENDPOINTS=(
  status addPipeline addWorkflow addExport execPipeline execWorkflow
  getExecInfo deleteExecInfo registerExecInfo registerPackage
  registerPipeline registerWorkflow removePipeline removeWorkflow
  pausePipeline prepareExec startExec startPipeline startWorkflow
  stopPipeline stopWorkflow sniffTransform pipelineStatus workflowStatus
  pipelineImage workflowImage webService asyncRun asyncStatus
)

printf '\n== %s as %s ==\n\n' "${BASE}" "${LABEL}"
printf '%-18s %-6s %-10s %s\n' ENDPOINT HTTP VERDICT NOTE
printf '%-18s %-6s %-10s %s\n' ------------------ ------ ---------- ----

for ep in "${ENDPOINTS[@]}"; do
  body=$(curl -s "${AUTH[@]}" --max-time 10 "${BASE}/hop/${ep}/?xml=Y" 2>/dev/null)
  code=$(curl -s "${AUTH[@]}" --max-time 10 -o /dev/null -w '%{http_code}' \
               "${BASE}/hop/${ep}/?xml=Y" 2>/dev/null)
  note=""
  case "${code}" in
    200|204|500)
      if grep -qi "hop-login\|<form\|sign in\|password" <<<"${body}"; then
        verdict="LOGIN"; note="served the login page"
      else
        verdict="REACHED"; note="$(head -c 60 <<<"${body}" | tr -d '\n\r' )"
      fi ;;
    301|302|303|307) verdict="REDIRECT"; note="probably -> /login" ;;
    401|403)         verdict="BLOCKED" ; note="auth enforced" ;;
    404)             verdict="404"     ; note="not registered in this build" ;;
    000)             verdict="NO-CONN" ; note="server not reachable" ;;
    *)               verdict="?"       ; note="" ;;
  esac
  printf '%-18s %-6s %-10s %s\n' "${ep}" "${code}" "${verdict}" "${note}"
done
echo

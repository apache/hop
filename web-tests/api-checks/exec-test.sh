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
# End-to-end proof: upload a pipeline over /hop/addPipeline and execute it
# over /hop/startPipeline, then read back its status. Cleans up after itself.
#
#   ./exec-test.sh <base-url> [user:password]
#
# Exit 0 = the pipeline RAN (finding confirmed for this identity)
# Exit 1 = blocked at some step (authorization held)
set -uo pipefail

BASE="${1:-http://localhost:8080}"
CREDS="${2:-}"
AUTH=(); LABEL="anonymous"
[[ -n "${CREDS}" ]] && { AUTH=(-u "${CREDS}"); LABEL="${CREDS%%:*}"; }

PAYLOAD="${PAYLOAD:-payload.xml}"
[[ -f "${PAYLOAD}" ]] || { echo "missing ${PAYLOAD} — run make-payload.sh first"; exit 2; }
NAME="${PIPELINE_NAME:-New pipeline}"

echo "=== execution test against ${BASE} as ${LABEL}"

echo "--- 1. POST /hop/addPipeline"
add=$(curl -s "${AUTH[@]}" --max-time 30 -X POST --data-binary "@${PAYLOAD}" \
        -H 'Content-Type: text/xml' "${BASE}/hop/addPipeline/?xml=Y")
id=$(sed -n 's:.*<id>\(.*\)</id>.*:\1:p' <<<"${add}" | head -1)
if [[ -z "${id}" ]]; then
  echo "    BLOCKED — no pipeline id returned"
  head -c 300 <<<"${add}"; echo; exit 1
fi
echo "    registered, id=${id}"

echo "--- 2. GET /hop/startPipeline"
start=$(curl -s "${AUTH[@]}" --max-time 30 -G "${BASE}/hop/startPipeline/" \
          --data-urlencode "name=${NAME}" --data-urlencode "id=${id}" --data 'xml=Y')
if ! grep -qi "<result>OK</result>" <<<"${start}"; then
  echo "    BLOCKED at start"; head -c 300 <<<"${start}"; echo; exit 1
fi
echo "    started"

sleep 4
echo "--- 3. GET /hop/pipelineStatus"
st=$(curl -s "${AUTH[@]}" --max-time 30 -G "${BASE}/hop/pipelineStatus/" \
       --data-urlencode "name=${NAME}" --data-urlencode "id=${id}" --data 'xml=Y')
desc=$(sed -n 's:.*<status_desc>\(.*\)</status_desc>.*:\1:p' <<<"${st}" | head -1)
errs=$(sed -n 's:.*<nr_errors>\(.*\)</nr_errors>.*:\1:p' <<<"${st}" | head -1)
echo "    status=${desc:-?} errors=${errs:-?}"
grep -o '<transformName>[^<]*</transformName>' <<<"${st}" | sed -E 's!<transformName>(.*)</transformName>!      transform ran: \1!'

echo "--- 4. cleanup /hop/removePipeline"
curl -s "${AUTH[@]}" --max-time 30 -G "${BASE}/hop/removePipeline/" \
     --data-urlencode "name=${NAME}" --data-urlencode "id=${id}" --data 'xml=Y' >/dev/null
echo "    removed"

if [[ "${desc}" == "Finished" ]]; then
  echo
  echo ">>> CONFIRMED: '${LABEL}' uploaded and executed a pipeline via the Hop Server API."
  exit 0
fi
echo; echo ">>> pipeline did not finish cleanly (status=${desc:-?})"; exit 1

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

# Reads the failed pipeline and workflow of 0002-opensearch-failing-workflow.hwf back from
# OpenSearch and checks the failure stored with them (issue #9417). The index was created by
# 0001-opensearch-create-hop-index.hpl without a mapping for the failure, the way an index made by
# an older Hop is, so this also checks that the location adds that mapping before it writes.

set -u

URL="${OPENSEARCH_URL:-https://opensearch:9200}"
USER_NAME="${OPENSEARCH_USERNAME:-admin}"
PASSWORD="${OPENSEARCH_PASSWORD:-Jackal-97-Nether-Blue^}"
INDEX="hop"
OUT="$(mktemp -d)"

fail() {
  echo "FAILED: ${1}"
  for f in "${OUT}"/*.json; do
    [ -f "${f}" ] && echo "----- ${f} -----" && cat "${f}"
  done
  exit 1
}

# <name of the pipeline or workflow> <file>: the newest document of that execution
fetch() {
  local name="${1}" file="${2}"
  for attempt in $(seq 1 15); do
    curl -k -s -u "${USER_NAME}:${PASSWORD}" -H 'Content-Type: application/json' \
      "${URL}/${INDEX}/_search" \
      -d "{ \"query\": { \"match_phrase\": { \"execution.name\": \"${name}\" } }, \"size\": 10 }" \
      >"${file}"
    if python3 -I -c '
import json, sys
hits = json.load(open(sys.argv[1]))["hits"]["hits"]
sys.exit(0 if any(h["_source"].get("executionState", {}).get("failure") for h in hits) else 1)
' "${file}" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  fail "no document of ${name} with a failure in index ${INDEX}"
}

# <file> <python expression on the failure f> <what it means>. root(f) follows causedBy to the end.
check() {
  python3 -I -c '
import json, sys
hits = json.load(open(sys.argv[1]))["hits"]["hits"]
f = [h["_source"]["executionState"]["failure"] for h in hits if h["_source"].get("executionState", {}).get("failure")][0]
def root(f):
    while f.get("causedBy"):
        f = f["causedBy"]
    return f
sys.exit(0 if eval(sys.argv[2]) else 1)
' "${1}" "${2}" || fail "${1} does not say ${3}"
}

curl -k -s -u "${USER_NAME}:${PASSWORD}" -X POST "${URL}/${INDEX}/_refresh" >/dev/null

PIPELINE="${OUT}/pipeline.json"
fetch "0002-opensearch-failing-pipeline" "${PIPELINE}"
check "${PIPELINE}" 'f["phase"] == "RUN"' "that the pipeline failed while running"
check "${PIPELINE}" 'f["causedBy"]["component"]["kind"] == "TRANSFORM"' "that a transform failed"
check "${PIPELINE}" 'f["causedBy"]["component"]["pluginId"] == "Abort"' "which transform failed"
check "${PIPELINE}" '"Failing on purpose" in f["causedBy"]["cause"]["message"]' "the message the transform logged"

WORKFLOW="${OUT}/workflow.json"
fetch "0002-opensearch-failing-workflow" "${WORKFLOW}"
check "${WORKFLOW}" 'f["causedBy"]["component"]["name"] == "Abort the workflow"' "that the workflow ended at the Abort action"
check "${WORKFLOW}" 'f["causedBy"]["causedBy"]["component"]["name"] == "Run the failing pipeline"' "that the Abort action points at the Pipeline action"
check "${WORKFLOW}" 'root(f)["component"]["pluginId"] == "Abort"' "that the failure started in the Abort transform"

# The index had no mapping for the failure, as one created by an older Hop: the location adds it
# before its first write, so the failure is stored without adding fields against the limit of 500.
curl -k -s -u "${USER_NAME}:${PASSWORD}" "${URL}/${INDEX}/_mapping" >"${OUT}/mapping.json"
python3 -I -c '
import json, sys
mapping = next(iter(json.load(open(sys.argv[1])).values()))["mappings"]
def count(properties):
    n = 0
    for field in properties.values():
        n += 1 + len(field.get("fields", {}))
        n += count(field.get("properties", {}))
    return n
state = mapping.get("properties", {}).get("executionState", {})
failure = state.get("properties", {}).get("failure", {})
print("Fields in index: %d (limit 500), mapping of executionState.failure: %s" % (
    count(mapping.get("properties", {})), json.dumps(failure)[:200]))
sys.exit(0 if failure.get("enabled") is False and not failure.get("properties") else 1)
' "${OUT}/mapping.json" || fail "index ${INDEX} maps every field of the failure instead of storing it unindexed"

rm -rf "${OUT}"
echo "The failed pipeline and workflow are stored at the OpenSearch location with their failure"

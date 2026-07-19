#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Configure local Artifactory so Hop marketplace can download plugins without
# admin credentials:
#   1. Ensure local Maven repo hop-plugins-local exists
#   2. Enable global "Allow Anonymous Access"
#   3. Grant anonymous *read only* on hop-plugins-local (not deploy/delete)
#
# Deploy/publish still requires admin (or another user with write).
#
# Usage:
#   export ARTIFACTORY_PASSWORD='…'   # admin password from first login
#   ./docker/marketplace-artifactory/configure-anonymous-read.sh
#
set -euo pipefail

BASE_URL="${ARTIFACTORY_URL_BASE:-http://localhost:8082}"
USER="${ARTIFACTORY_USER:-admin}"
PASS="${ARTIFACTORY_PASSWORD:-}"
REPO_KEY="${ARTIFACTORY_REPO_KEY:-hop-plugins-local}"
PERM_NAME="${ARTIFACTORY_ANON_PERM:-hop-plugins-anonymous-read}"

if [[ -z "${PASS}" ]]; then
  echo "Set ARTIFACTORY_PASSWORD to the Artifactory admin password." >&2
  exit 1
fi

AUTH=(-u "${USER}:${PASS}")
API="${BASE_URL}/artifactory/api"

echo "==> Checking Artifactory at ${BASE_URL}"
if ! curl -sf "${BASE_URL}/artifactory/api/system/ping" >/dev/null; then
  echo "ERROR: Artifactory not reachable (ping failed)." >&2
  exit 1
fi

# --- 1) Local Maven repository ------------------------------------------------
echo "==> Ensuring local Maven repository '${REPO_KEY}'"
REPO_CODE=$(curl -s -o /tmp/hop-af-repo.json -w '%{http_code}' "${AUTH[@]}" \
  "${API}/repositories/${REPO_KEY}" || true)
if [[ "${REPO_CODE}" == "200" ]]; then
  echo "    Repository already exists"
else
  HTTP=$(curl -s -o /tmp/hop-af-create-repo.json -w '%{http_code}' "${AUTH[@]}" \
    -X PUT "${API}/repositories/${REPO_KEY}" \
    -H 'Content-Type: application/json' \
    -d "{
      \"key\": \"${REPO_KEY}\",
      \"rclass\": \"local\",
      \"packageType\": \"maven\",
      \"repoLayoutRef\": \"maven-2-default\",
      \"description\": \"Hop marketplace plugin zips (local / IT)\"
    }")
  if [[ "${HTTP}" != "200" && "${HTTP}" != "201" ]]; then
    echo "ERROR: create repository failed (HTTP ${HTTP}):" >&2
    cat /tmp/hop-af-create-repo.json >&2 || true
    exit 1
  fi
  echo "    Created ${REPO_KEY}"
fi

# --- 2) Enable anonymous access (global) --------------------------------------
# Artifactory security configuration (classic REST). We GET, patch anonAccessEnabled, PUT back.
echo "==> Enabling global anonymous access"
if curl -sf "${AUTH[@]}" "${API}/security/configuration" -o /tmp/hop-af-sec.json; then
  # configuration is XML for some versions, JSON for others — handle both lightly
  if head -c 1 /tmp/hop-af-sec.json | grep -q '<'; then
    # XML: set <anonAccessEnabled>true</anonAccessEnabled>
    if grep -q '<anonAccessEnabled>' /tmp/hop-af-sec.json; then
      sed -i 's#<anonAccessEnabled>[^<]*</anonAccessEnabled>#<anonAccessEnabled>true</anonAccessEnabled>#' /tmp/hop-af-sec.json
    else
      # insert before closing security tag if present
      sed -i 's#</security>#  <anonAccessEnabled>true</anonAccessEnabled>\n</security>#' /tmp/hop-af-sec.json
    fi
    HTTP=$(curl -s -o /tmp/hop-af-sec-put.json -w '%{http_code}' "${AUTH[@]}" \
      -X POST "${API}/security/configuration" \
      -H 'Content-Type: application/xml' \
      --data-binary @/tmp/hop-af-sec.json)
  else
    # JSON (Access / newer): set anonAccessEnabled
    if command -v python3 >/dev/null 2>&1; then
      python3 - <<'PY'
import json
from pathlib import Path
p = Path("/tmp/hop-af-sec.json")
data = json.loads(p.read_text())
# common shapes
if isinstance(data, dict):
    data["anonAccessEnabled"] = True
    if "security" in data and isinstance(data["security"], dict):
        data["security"]["anonAccessEnabled"] = True
p.write_text(json.dumps(data))
PY
    fi
    HTTP=$(curl -s -o /tmp/hop-af-sec-put.json -w '%{http_code}' "${AUTH[@]}" \
      -X POST "${API}/security/configuration" \
      -H 'Content-Type: application/json' \
      --data-binary @/tmp/hop-af-sec.json)
  fi
  if [[ "${HTTP}" == "200" || "${HTTP}" == "201" || "${HTTP}" == "204" ]]; then
    echo "    Anonymous access enabled (API)"
  else
    echo "    WARNING: could not set anonymous via API (HTTP ${HTTP})."
    echo "    Do this in the UI: Administration → User Management → Settings → Allow Anonymous Access"
    cat /tmp/hop-af-sec-put.json 2>/dev/null || true
  fi
else
  echo "    WARNING: GET /security/configuration failed."
  echo "    Enable in UI: Administration → User Management → Settings → Allow Anonymous Access"
fi

# --- 3) Permission: anonymous READ only on hop-plugins-local --------------------
# Artifactory 7.x removed anonymous from the default "Anything" target — grant explicitly.
echo "==> Creating permission '${PERM_NAME}' (anonymous → read on ${REPO_KEY} only)"
HTTP=$(curl -s -o /tmp/hop-af-perm.json -w '%{http_code}' "${AUTH[@]}" \
  -X PUT "${API}/v2/security/permissions/${PERM_NAME}" \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\": \"${PERM_NAME}\",
    \"repo\": {
      \"include-patterns\": [\"**\"],
      \"exclude-patterns\": [],
      \"repositories\": [\"${REPO_KEY}\"],
      \"actions\": {
        \"users\": {
          \"anonymous\": [\"read\"]
        }
      }
    }
  }")

if [[ "${HTTP}" != "200" && "${HTTP}" != "201" ]]; then
  # Fallback: classic permissions API
  HTTP2=$(curl -s -o /tmp/hop-af-perm2.json -w '%{http_code}' "${AUTH[@]}" \
    -X PUT "${API}/security/permissions/${PERM_NAME}" \
    -H 'Content-Type: application/json' \
    -d "{
      \"name\": \"${PERM_NAME}\",
      \"repositories\": [\"${REPO_KEY}\"],
      \"principals\": {
        \"users\": {
          \"anonymous\": [\"r\"]
        }
      }
    }")
  if [[ "${HTTP2}" != "200" && "${HTTP2}" != "201" ]]; then
    echo "ERROR: could not create permission (v2 HTTP ${HTTP}, classic HTTP ${HTTP2}):" >&2
    cat /tmp/hop-af-perm.json >&2 || true
    cat /tmp/hop-af-perm2.json >&2 || true
    echo >&2
    echo "UI fallback:" >&2
    echo "  1) Administration → User Management → Settings → Allow Anonymous Access = ON" >&2
    echo "  2) Administration → User Management → Permissions → New Permission" >&2
    echo "       Repositories: ${REPO_KEY}" >&2
    echo "       Users: anonymous → Read only (no Deploy/Delete/Manage)" >&2
    exit 1
  fi
  echo "    Permission created (classic API)"
else
  echo "    Permission created (v2 API)"
fi

# --- 4) Smoke test anonymous download path ------------------------------------
echo "==> Smoke test: anonymous GET on repo root (expect 200 or 404 empty, not 401)"
ANON_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
  "${BASE_URL}/artifactory/${REPO_KEY}/" || true)
case "${ANON_CODE}" in
  200|404)
    echo "    OK (HTTP ${ANON_CODE}) — anonymous can reach the repository"
    ;;
  401|403)
    echo "    STILL DENIED (HTTP ${ANON_CODE}). Check UI anonymous + permission steps above." >&2
    exit 1
    ;;
  *)
    echo "    Unexpected HTTP ${ANON_CODE} (repo may be empty; try install after publish)"
    ;;
esac

echo
echo "Done. Deploy still needs admin; downloads do not:"
echo "  # publish (authenticated)"
echo "  export ARTIFACTORY_PASSWORD=…"
echo "  ./docker/marketplace-artifactory/publish-wave1-plugins.sh"
echo
echo "  # install (no credentials)"
echo "  # hop-config marketplace.repositories[0].url ="
echo "  #   ${BASE_URL}/artifactory/${REPO_KEY}/"
echo "  ./hop marketplace install hop-tech-parquet"

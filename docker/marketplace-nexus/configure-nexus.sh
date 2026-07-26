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
# Idempotent Nexus OSS bootstrap for Hop marketplace:
#   - change admin password from first-boot random password if needed
#   - enable anonymous access
#   - create hosted Maven repo hop-plugins (MIXED / ALLOW for SNAPSHOT zips)
#
# Env:
#   NEXUS_URL              default http://localhost:8081
#   NEXUS_ADMIN_USER       default admin
#   NEXUS_ADMIN_PASSWORD   desired admin password (default hop-nexus-dev)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_URL="${NEXUS_URL:-http://localhost:8081}"
BASE_URL="${BASE_URL%/}"
ADMIN_USER="${NEXUS_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${NEXUS_ADMIN_PASSWORD:-hop-nexus-dev}"
REPO_NAME="${NEXUS_REPO_NAME:-hop-plugins}"
CONTAINER="${NEXUS_CONTAINER:-hop-marketplace-nexus}"

api() {
  # api METHOD PATH [curl args...]
  local method="$1"
  local path="$2"
  shift 2
  curl -sS -u "${ADMIN_USER}:${CURRENT_PASSWORD}" \
    -X "${method}" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json' \
    "${BASE_URL}${path}" \
    "$@"
}

http_code() {
  local method="$1"
  local path="$2"
  shift 2
  curl -sS -o /tmp/nexus-http.body -w '%{http_code}' \
    -u "${ADMIN_USER}:${CURRENT_PASSWORD}" \
    -X "${method}" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json' \
    "${BASE_URL}${path}" \
    "$@"
}

echo "==> Resolving admin password"
CURRENT_PASSWORD="${ADMIN_PASSWORD}"

# If desired password already works, skip change
code=$(curl -sS -o /dev/null -w '%{http_code}' -u "${ADMIN_USER}:${CURRENT_PASSWORD}" \
  "${BASE_URL}/service/rest/v1/status" || echo 000)
if [[ "${code}" != "200" ]]; then
  # First-boot password is in the data volume
  if docker exec "${CONTAINER}" test -f /nexus-data/admin.password 2>/dev/null; then
    BOOT_PASSWORD=$(docker exec "${CONTAINER}" cat /nexus-data/admin.password | tr -d '\r\n')
    echo "    Using first-boot password from container volume"
    CURRENT_PASSWORD="${BOOT_PASSWORD}"
    code=$(curl -sS -o /dev/null -w '%{http_code}' -u "${ADMIN_USER}:${CURRENT_PASSWORD}" \
      "${BASE_URL}/service/rest/v1/status" || echo 000)
  fi
fi

if [[ "${code}" != "200" ]]; then
  echo "ERROR: cannot authenticate as ${ADMIN_USER} (HTTP ${code})." >&2
  echo "  If you set a custom password earlier, export NEXUS_ADMIN_PASSWORD=…" >&2
  echo "  Or reset: ./start.sh --reset" >&2
  exit 1
fi
echo "    Authenticated as ${ADMIN_USER}"

# Change password if still on first-boot password
if [[ "${CURRENT_PASSWORD}" != "${ADMIN_PASSWORD}" ]]; then
  echo "==> Setting admin password"
  chg=$(curl -sS -o /tmp/nexus-chg.body -w '%{http_code}' \
    -u "${ADMIN_USER}:${CURRENT_PASSWORD}" \
    -X PUT \
    -H 'Content-Type: text/plain' \
    --data-binary "${ADMIN_PASSWORD}" \
    "${BASE_URL}/service/rest/v1/security/users/${ADMIN_USER}/change-password" || echo 000)
  # Some versions expect JSON body:
  if [[ "${chg}" != "204" && "${chg}" != "200" ]]; then
    chg=$(curl -sS -o /tmp/nexus-chg.body -w '%{http_code}' \
      -u "${ADMIN_USER}:${CURRENT_PASSWORD}" \
      -X PUT \
      -H 'Content-Type: application/json' \
      -d "{\"password\":\"${ADMIN_PASSWORD}\"}" \
      "${BASE_URL}/service/rest/v1/security/users/${ADMIN_USER}/change-password" || echo 000)
  fi
  if [[ "${chg}" == "204" || "${chg}" == "200" ]]; then
    echo "    Password updated"
    CURRENT_PASSWORD="${ADMIN_PASSWORD}"
  else
    echo "    WARNING: change-password returned HTTP ${chg} (body: $(cat /tmp/nexus-chg.body 2>/dev/null || true))"
    echo "    Continuing with current credentials"
  fi
fi

echo "==> Enabling anonymous access"
anon=$(http_code PUT /service/rest/v1/security/anonymous \
  -d '{"enabled":true,"userId":"anonymous","realmName":"NexusAuthorizingRealm"}')
if [[ "${anon}" == "200" || "${anon}" == "204" ]]; then
  echo "    Anonymous enabled (HTTP ${anon})"
else
  # GET then PUT full object if needed
  echo "    WARNING: anonymous PUT HTTP ${anon}; check UI Security → Anonymous Access"
  cat /tmp/nexus-http.body 2>/dev/null || true
fi

echo "==> Ensuring hosted Maven repository '${REPO_NAME}'"
exists=$(curl -sS -o /dev/null -w '%{http_code}' -u "${ADMIN_USER}:${CURRENT_PASSWORD}" \
  "${BASE_URL}/service/rest/v1/repositories/${REPO_NAME}" || echo 000)
if [[ "${exists}" == "200" ]]; then
  echo "    Repository already exists"
else
  create=$(http_code POST /service/rest/v1/repositories/maven/hosted -d "{
    \"name\": \"${REPO_NAME}\",
    \"online\": true,
    \"storage\": {
      \"blobStoreName\": \"default\",
      \"strictContentTypeValidation\": false,
      \"writePolicy\": \"ALLOW\"
    },
    \"cleanup\": { \"policyNames\": [] },
    \"component\": { \"proprietaryComponents\": false },
    \"maven\": {
      \"versionPolicy\": \"MIXED\",
      \"layoutPolicy\": \"PERMISSIVE\",
      \"contentDisposition\": \"ATTACHMENT\"
    }
  }")
  if [[ "${create}" == "201" || "${create}" == "200" ]]; then
    echo "    Created hosted Maven repo '${REPO_NAME}'"
  else
    echo "ERROR: create repository failed HTTP ${create}" >&2
    cat /tmp/nexus-http.body >&2 || true
    exit 1
  fi
fi

echo "==> Smoke test anonymous GET"
REPO_URL="${BASE_URL}/repository/${REPO_NAME}/"
anon_get=$(curl -sS -o /dev/null -w '%{http_code}' "${REPO_URL}" || echo 000)
case "${anon_get}" in
  200|404)
    echo "    OK — anonymous can reach ${REPO_URL} (HTTP ${anon_get})"
    ;;
  401|403)
    echo "    WARNING: anonymous still denied (HTTP ${anon_get})."
    echo "    UI: Settings → Security → Anonymous Access → enable"
    ;;
  *)
    echo "    Unexpected HTTP ${anon_get} for ${REPO_URL}"
    ;;
esac

# Persist for publish scripts
ENV_FILE="${SCRIPT_DIR}/.env"
cat > "${ENV_FILE}" <<EOF
# Generated by configure-nexus.sh — do not commit
NEXUS_URL=${BASE_URL}
NEXUS_ADMIN_USER=${ADMIN_USER}
NEXUS_ADMIN_PASSWORD=${ADMIN_PASSWORD}
NEXUS_REPO_NAME=${REPO_NAME}
NEXUS_REPO_URL=${BASE_URL}/repository/${REPO_NAME}
EOF
chmod 600 "${ENV_FILE}" 2>/dev/null || true
echo "    Wrote ${ENV_FILE}"

echo "Configure complete."

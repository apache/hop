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
# End-to-end smoke test: local Nexus + hop marketplace CLI (anonymous).
#
# Prerequisites:
#   - ./docker/marketplace-nexus/start.sh
#   - ./docker/marketplace-nexus/publish-wave1-plugins.sh
#   - Unzipped hop client at assemblies/client/target/hop (or HOP_DIR)
#
# Usage (from repo root):
#   ./docker/marketplace-nexus/smoke-test.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOP_DIR="${HOP_DIR:-${ROOT}/assemblies/client/target/hop}"
REPO_URL="${NEXUS_REPO_URL:-http://127.0.0.1:8081/repository/hop-plugins/}"
REPO_URL="${REPO_URL%/}/"
VERSION="${HOP_VERSION:-2.19.0-SNAPSHOT}"

# Small plugins for a fast round-trip (publish-wave1 must have deployed them)
INSTALL_A="${SMOKE_PLUGIN_A:-hop-tech-dropbox}"
INSTALL_B="${SMOKE_PLUGIN_B:-hop-transform-edi2xml}"

if [[ ! -x "${HOP_DIR}/hop" ]]; then
  echo "Hop launcher not found: ${HOP_DIR}/hop" >&2
  echo "Build/unzip hop-client first (assemblies/client/target/hop)." >&2
  exit 1
fi

echo "==> Checking Nexus anonymous read: ${REPO_URL}"
code=$(curl -sS -o /dev/null -w '%{http_code}' "${REPO_URL}" || true)
if [[ "${code}" != "200" && "${code}" != "404" ]]; then
  echo "Nexus not reachable or not anonymous (HTTP ${code}). Run start.sh first." >&2
  exit 1
fi

# Isolated config so we do not touch the developer's HOP_CONFIG_FOLDER
CFG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hop-marketplace-smoke.XXXXXX")"
AUDIT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hop-marketplace-audit.XXXXXX")"
trap 'rm -rf "${CFG_DIR}" "${AUDIT_DIR}"' EXIT

cat > "${CFG_DIR}/hop-config.json" <<EOF
{
  "marketplace": {
    "enabled": true,
    "groupId": "org.apache.hop",
    "defaultVersion": "${VERSION}",
    "repositories": [
      {
        "id": "local-nexus",
        "url": "${REPO_URL}"
      }
    ]
  }
}
EOF

ENV_FILE="${CFG_DIR}/hop-env-smoke.yaml"
cat > "${ENV_FILE}" <<EOF
version: "1.0"
hopVersion: "${VERSION}"
enforceOnRun: false
repositories:
  - id: local-nexus
    url: "${REPO_URL}"
plugins:
  - artifactId: ${INSTALL_A}
    version: "${VERSION}"
EOF

export HOP_CONFIG_FOLDER="${CFG_DIR}"
export HOP_AUDIT_FOLDER="${AUDIT_DIR}"
# Ensure no leftover marketplace auth poisons anonymous Nexus
unset HOP_MARKETPLACE_USERNAME HOP_MARKETPLACE_USER HOP_MARKETPLACE_PASSWORD || true

run_hop() {
  (cd "${HOP_DIR}" && ./hop "$@")
}

echo "==> marketplace list (baseline)"
run_hop marketplace list || true

echo "==> marketplace install ${INSTALL_A}"
run_hop marketplace install "${INSTALL_A}"

echo "==> marketplace install ${INSTALL_B}"
run_hop marketplace install "${INSTALL_B}"

echo "==> marketplace list (expect both)"
list_out=$(run_hop marketplace list)
echo "${list_out}"
echo "${list_out}" | grep -q "${INSTALL_A}"
echo "${list_out}" | grep -q "${INSTALL_B}"

echo "==> marketplace validate -f hop-env-smoke.yaml"
run_hop marketplace validate -f "${ENV_FILE}"

echo "==> marketplace uninstall ${INSTALL_B}"
run_hop marketplace uninstall "${INSTALL_B}"

echo "==> marketplace apply -f hop-env-smoke.yaml (idempotent ${INSTALL_A})"
run_hop marketplace apply -f "${ENV_FILE}"

echo "==> marketplace list (expect ${INSTALL_A} only from smoke installs; others may remain)"
run_hop marketplace list | grep -q "${INSTALL_A}"

if [[ ! -d "${HOP_DIR}/plugins" ]]; then
  echo "FAIL: plugins/ missing under ${HOP_DIR}" >&2
  exit 1
fi

echo
echo "OK — marketplace smoke test passed against ${REPO_URL}"
echo "    hop install: ${HOP_DIR}"
echo "    config:      ${HOP_CONFIG_FOLDER}/hop-config.json"

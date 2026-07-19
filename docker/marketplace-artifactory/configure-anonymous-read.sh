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
# Configure *Artifactory OSS* so Hop can download plugins without admin credentials.
#
# IMPORTANT: Artifactory OSS does NOT allow creating repositories (or many admin
# security resources) via REST — those APIs return HTTP 400 "Artifactory Pro only".
# Repository + anonymous + read permission are done in the UI; this script prints
# the exact clicks and verifies anonymous access afterward.
#
# Usage:
#   # 1) Create hop-plugins-local in the UI (see printed steps)
#   # 2) Enable anonymous + read permission (see printed steps)
#   # 3) Optional verify:
#   export ARTIFACTORY_PASSWORD='…'
#   ./docker/marketplace-artifactory/configure-anonymous-read.sh
#
set -euo pipefail

BASE_URL="${ARTIFACTORY_URL_BASE:-http://localhost:8082}"
USER="${ARTIFACTORY_USER:-admin}"
PASS="${ARTIFACTORY_PASSWORD:-}"
REPO_KEY="${ARTIFACTORY_REPO_KEY:-hop-plugins-local}"

REPO_URL="${BASE_URL}/artifactory/${REPO_KEY}/"

echo "============================================================"
echo " Artifactory OSS — anonymous read for Hop marketplace"
echo "============================================================"
echo
echo "Artifactory OSS license: repository create/manage REST APIs are Pro-only."
echo "Do the following once in the UI at ${BASE_URL}/"
echo

cat <<EOF
┌─────────────────────────────────────────────────────────────────┐
│  A) Create local Maven repository (if you have not already)     │
├─────────────────────────────────────────────────────────────────┤
│  Administration → Repositories → Create a Repository → Local    │
│                                                                 │
│  Package type:     Maven                                        │
│  Repository Key:   ${REPO_KEY}                                  │
│  Repository Layout: maven-2-default                             │
│  (Save / Create)                                                │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  B) Allow anonymous access (global)                             │
├─────────────────────────────────────────────────────────────────┤
│  Administration → User Management → Settings                    │
│    ☑ Allow Anonymous Access                                     │
│  Save                                                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  C) Anonymous READ only on ${REPO_KEY} (not deploy)               │
├─────────────────────────────────────────────────────────────────┤
│  Administration → User Management → Permissions → + Add         │
│                                                                 │
│  Permission name:  hop-plugins-anonymous-read                   │
│  Resources → Repositories:  select only "${REPO_KEY}"           │
│  (do not use "Any Local" / "Anything" if you want least priv) │
│                                                                 │
│  Users:  anonymous                                              │
│  Actions:  ☑ Read   ☐ Annotate ☐ Deploy ☐ Delete ☐ Manage       │
│  Save                                                           │
└─────────────────────────────────────────────────────────────────┘

Security model:
  • Hop marketplace install  → anonymous HTTP GET (no password)
  • publish-wave1-plugins.sh → admin (or a deploy user) only

EOF

if [[ -z "${PASS}" ]]; then
  echo "Set ARTIFACTORY_PASSWORD and re-run this script to verify anonymous access:"
  echo "  export ARTIFACTORY_PASSWORD='…'"
  echo "  $0"
  exit 0
fi

AUTH=(-u "${USER}:${PASS}")

echo "==> Checking Artifactory"
if ! curl -sf "${BASE_URL}/artifactory/api/system/ping" >/dev/null; then
  echo "ERROR: ping failed — is the stack up?" >&2
  exit 1
fi
echo "    ping OK"

# Confirm we are on OSS (informational)
LICENSE=$(curl -sf "${AUTH[@]}" "${BASE_URL}/artifactory/api/system/version" \
  | sed -n 's/.*"license"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1 || true)
echo "    license: ${LICENSE:-unknown}"

echo "==> Checking repository '${REPO_KEY}' exists (authenticated)"
# OSS: list/get repository REST may also be limited; probe storage path instead.
HTTP_AUTH=$(curl -s -o /dev/null -w '%{http_code}' "${AUTH[@]}" "${REPO_URL}" || true)
case "${HTTP_AUTH}" in
  200|404)
    echo "    Repository path reachable as admin (HTTP ${HTTP_AUTH})"
    ;;
  401|403)
    echo "ERROR: admin auth failed (HTTP ${HTTP_AUTH}). Check ARTIFACTORY_PASSWORD." >&2
    exit 1
    ;;
  *)
    echo "    WARNING: unexpected HTTP ${HTTP_AUTH} for ${REPO_URL}"
    echo "    If the repo is missing, create it in the UI (step A above)."
    ;;
esac

echo "==> Checking anonymous read on '${REPO_KEY}'"
HTTP_ANON=$(curl -s -o /dev/null -w '%{http_code}' "${REPO_URL}" || true)
case "${HTTP_ANON}" in
  200|404)
    echo "    OK — anonymous access works (HTTP ${HTTP_ANON})"
    echo
    echo "Hop config (no username/password):"
    echo "  \"url\": \"${REPO_URL}\""
    echo
    echo "Then:"
    echo "  unset HOP_MARKETPLACE_PASSWORD ARTIFACTORY_PASSWORD"
    echo "  ./hop marketplace install hop-tech-parquet"
    exit 0
    ;;
  401|403)
    echo "    NOT YET (HTTP ${HTTP_ANON}) — finish UI steps B and C above, then re-run:"
    echo "      $0"
    exit 1
    ;;
  *)
    echo "    Unexpected HTTP ${HTTP_ANON}. Finish UI steps A–C and re-run."
    exit 1
    ;;
esac

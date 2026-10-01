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
# Start Nexus, set admin password, create hop-plugins repo, enable anonymous read.
#
# Usage:
#   ./docker/marketplace-nexus/start.sh
#   ./docker/marketplace-nexus/start.sh --reset          # wipe volumes
#   NEXUS_ADMIN_PASSWORD=secret ./docker/marketplace-nexus/start.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

BASE_URL="${NEXUS_URL:-http://localhost:8081}"
ADMIN_USER="${NEXUS_ADMIN_USER:-admin}"
# Password we want after bootstrap (override with env)
ADMIN_PASSWORD="${NEXUS_ADMIN_PASSWORD:-hop-nexus-dev}"
RESET=false

for arg in "$@"; do
  case "${arg}" in
    --reset|-r) RESET=true ;;
    -h|--help)
      echo "Usage: $0 [--reset]"
      echo "  NEXUS_ADMIN_PASSWORD  admin password to set (default: hop-nexus-dev)"
      exit 0
      ;;
  esac
done

if [[ "${RESET}" == "true" ]]; then
  echo "Resetting Nexus volumes..."
  docker compose down -v
fi

echo "Starting Nexus Repository OSS..."
docker compose up -d

echo "Waiting for Nexus status API (first boot can take 1–3 minutes)..."
ready=false
for i in $(seq 1 80); do
  if curl -sf "${BASE_URL}/service/rest/v1/status" >/dev/null 2>&1; then
    ready=true
    break
  fi
  # Also accept 200/503 from status/writable during warmup
  code=$(curl -s -o /dev/null -w '%{http_code}' "${BASE_URL}/service/rest/v1/status" 2>/dev/null || echo 000)
  if [[ "${code}" == "200" ]]; then
    ready=true
    break
  fi
  printf '.'
  sleep 3
done
echo
if [[ "${ready}" != "true" ]]; then
  echo "ERROR: Nexus did not become ready. Logs:" >&2
  docker logs hop-marketplace-nexus 2>&1 | tail -40 >&2
  exit 1
fi
echo "Nexus is up: ${BASE_URL}"

export NEXUS_URL="${BASE_URL}"
export NEXUS_ADMIN_USER="${ADMIN_USER}"
export NEXUS_ADMIN_PASSWORD="${ADMIN_PASSWORD}"
./configure-nexus.sh

echo
echo "Done."
echo "  UI:           ${BASE_URL}/"
echo "  Admin user:   ${ADMIN_USER}"
echo "  Admin pass:   ${ADMIN_PASSWORD}"
echo "  Hosted repo:  ${BASE_URL}/repository/hop-plugins/"
echo
echo "Publish (authenticated):"
echo "  export NEXUS_URL=${BASE_URL}/repository/hop-plugins"
echo "  export NEXUS_USER=${ADMIN_USER}"
echo "  export NEXUS_PASSWORD=${ADMIN_PASSWORD}"
echo "  ./publish-marketplace-plugins.sh [--package]"
echo
echo "Hop marketplace (anonymous read):"
echo "  \"url\": \"${BASE_URL}/repository/hop-plugins/\""

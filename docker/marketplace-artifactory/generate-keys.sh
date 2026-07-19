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
# Generate the JFrog Artifactory master key required for a clean startup.
# Artifactory runs as UID/GID 1030:1030 inside the container; the key file
# must be owned by that user and mode 600 or the service hangs at boot.
#
# Usage:
#   ./docker/marketplace-artifactory/generate-keys.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEYS_DIR="${SCRIPT_DIR}/keys"
MASTER_KEY_FILE="${KEYS_DIR}/master.key"

# Artifactory process user inside the official OSS image
ARTIFACTORY_UID=1030
ARTIFACTORY_GID=1030

mkdir -p "${KEYS_DIR}"

if [[ -f "${MASTER_KEY_FILE}" ]]; then
  echo "Master key already exists: ${MASTER_KEY_FILE}"
else
  echo "Generating Artifactory master key (openssl rand -hex 32)..."
  openssl rand -hex 32 > "${MASTER_KEY_FILE}"
  echo "Wrote ${MASTER_KEY_FILE}"
fi

chmod 600 "${MASTER_KEY_FILE}"

# Ownership must be 1030:1030 so the container process can read the key.
if chown "${ARTIFACTORY_UID}:${ARTIFACTORY_GID}" "${MASTER_KEY_FILE}" 2>/dev/null; then
  echo "Set owner ${ARTIFACTORY_UID}:${ARTIFACTORY_GID} on master.key"
elif command -v docker >/dev/null 2>&1; then
  # No root on host: fix ownership via a throwaway container
  docker run --rm \
    -v "${KEYS_DIR}:/keys" \
    alpine:3.19 \
    chown "${ARTIFACTORY_UID}:${ARTIFACTORY_GID}" /keys/master.key
  echo "Set owner ${ARTIFACTORY_UID}:${ARTIFACTORY_GID} on master.key (via docker)"
else
  echo "WARNING: could not chown master.key to ${ARTIFACTORY_UID}:${ARTIFACTORY_GID}." >&2
  echo "         Run: sudo chown ${ARTIFACTORY_UID}:${ARTIFACTORY_GID} ${MASTER_KEY_FILE}" >&2
  exit 1
fi

ls -la "${MASTER_KEY_FILE}"
echo "Done. Start Artifactory with:"
echo "  docker compose -f docker/marketplace-artifactory/docker-compose.yml up -d"

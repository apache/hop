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
# Generate JFrog Artifactory security keys required for a clean startup:
#   - master.key  (shared master key)
#   - join.key    (cluster join key; required even for a single-node OSS stack)
#
# Both are openssl rand -hex 32 (64 hex chars, no trailing newline), mode 600,
# owned by UID/GID 1030:1030 (Artifactory process user in the official image).
# Without them, services hang ("Cluster join: Join key is missing").
#
# Usage:
#   ./docker/marketplace-artifactory/generate-keys.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEYS_DIR="${SCRIPT_DIR}/keys"

ARTIFACTORY_UID=1030
ARTIFACTORY_GID=1030

if ! command -v openssl >/dev/null 2>&1; then
  echo "ERROR: openssl is required on the host to generate keys" >&2
  exit 1
fi

mkdir -p "${KEYS_DIR}"

# Write key material to a host temp file (64 hex chars, no newline)
make_key_material() {
  local tmp
  tmp="$(mktemp)"
  openssl rand -hex 32 | tr -d '\n' > "${tmp}"
  # Verify length (32 bytes → 64 hex chars)
  local len
  len="$(wc -c < "${tmp}" | tr -d ' ')"
  if [[ "${len}" != "64" ]]; then
    rm -f "${tmp}"
    echo "ERROR: expected 64-char hex key, got length ${len}" >&2
    exit 1
  fi
  echo "${tmp}"
}

# Install a key file as 1030:1030 mode 600. Works even when an old key is owned by 1030.
install_key() {
  local name="$1"
  local dest="${KEYS_DIR}/${name}"
  local material
  material="$(make_key_material)"

  if [[ -f "${dest}" ]]; then
    # Readable empty/corrupt keys should be replaced
    local existing_len=0
    if [[ -r "${dest}" ]]; then
      existing_len="$(wc -c < "${dest}" | tr -d ' ')"
    else
      # Unreadable (owned by 1030): probe via docker
      if command -v docker >/dev/null 2>&1; then
        existing_len="$(docker run --rm -v "${KEYS_DIR}:/keys" alpine:3.19 \
          sh -c "wc -c < /keys/${name}" | tr -d ' ')"
      fi
    fi
    if [[ "${existing_len}" == "64" ]]; then
      rm -f "${material}"
      echo "Key already exists and looks valid: ${dest} (${existing_len} bytes)"
      return 0
    fi
    echo "Replacing invalid/empty ${dest} (length ${existing_len})"
  else
    echo "Generating ${name}..."
  fi

  # Try host write first
  if cat "${material}" > "${dest}" 2>/dev/null \
    && chmod 600 "${dest}" 2>/dev/null \
    && chown "${ARTIFACTORY_UID}:${ARTIFACTORY_GID}" "${dest}" 2>/dev/null; then
    rm -f "${material}"
    echo "Wrote ${dest}"
    return 0
  fi

  # Host cannot overwrite 1030-owned file: install via docker as root
  if command -v docker >/dev/null 2>&1; then
    docker run --rm \
      -v "${KEYS_DIR}:/keys" \
      -v "${material}:/tmp/key.material:ro" \
      alpine:3.19 \
      sh -c "cp /tmp/key.material /keys/${name} && chmod 600 /keys/${name} && chown ${ARTIFACTORY_UID}:${ARTIFACTORY_GID} /keys/${name}"
    rm -f "${material}"
    echo "Wrote ${dest} (via docker)"
    return 0
  fi

  rm -f "${material}"
  echo "ERROR: unable to write ${dest} (need docker or root for chown 1030:1030)" >&2
  exit 1
}

fix_perms() {
  local name="$1"
  if chmod 600 "${KEYS_DIR}/${name}" 2>/dev/null \
    && chown "${ARTIFACTORY_UID}:${ARTIFACTORY_GID}" "${KEYS_DIR}/${name}" 2>/dev/null; then
    return 0
  fi
  if command -v docker >/dev/null 2>&1; then
    docker run --rm -v "${KEYS_DIR}:/keys" alpine:3.19 \
      sh -c "chmod 600 /keys/${name} && chown ${ARTIFACTORY_UID}:${ARTIFACTORY_GID} /keys/${name}"
    return 0
  fi
  echo "WARNING: could not enforce 600 / ${ARTIFACTORY_UID}:${ARTIFACTORY_GID} on ${name}" >&2
  return 1
}

install_key "master.key"
install_key "join.key"
fix_perms "master.key"
fix_perms "join.key"

echo "Key files:"
if command -v docker >/dev/null 2>&1; then
  docker run --rm -v "${KEYS_DIR}:/keys" alpine:3.19 \
    sh -c 'ls -la /keys/master.key /keys/join.key && echo -n "lengths: " && wc -c /keys/master.key /keys/join.key'
else
  ls -la "${KEYS_DIR}"/*.key
  wc -c "${KEYS_DIR}"/*.key
fi

echo "Done. Restart Artifactory with:"
echo "  docker compose -f docker/marketplace-artifactory/docker-compose.yml down"
echo "  docker compose -f docker/marketplace-artifactory/docker-compose.yml up -d"

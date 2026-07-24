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
# Unpack marketplace-optional plugin zips into a Hop install (e.g. for IT images).
# Plugin list is read only from optional-plugins.yaml (via list-marketplace-plugins.sh).
#
# Usage:
#   ./tools/install-wave1-plugins.sh [HOP_INSTALL_DIR]
# Default install dir: assemblies/client/target/hop
#
# Requires plugin modules to have been packaged (*.zip under plugins/**/target/).
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIR="${1:-${ROOT}/assemblies/client/target/hop}"
VERSION="${HOP_VERSION:-2.19.0-SNAPSHOT}"
LIST_SCRIPT="${ROOT}/tools/list-marketplace-plugins.sh"

if [[ ! -d "${INSTALL_DIR}" ]]; then
  echo "Hop install not found: ${INSTALL_DIR}" >&2
  echo "Unzip hop-client first, or pass an existing install path." >&2
  exit 1
fi

if [[ ! -x "${LIST_SCRIPT}" ]]; then
  chmod +x "${LIST_SCRIPT}" 2>/dev/null || true
fi

mapfile -t PLUGINS < <(HOP_VERSION="${VERSION}" "${LIST_SCRIPT}" --zips)

installed=0
skipped=0
for entry in "${PLUGINS[@]}"; do
  id="${entry%%|*}"
  rel="${entry#*|}"
  zip="${ROOT}/${rel}"
  if [[ ! -f "${zip}" ]]; then
    echo "SKIP (not built): ${id} — ${rel}"
    skipped=$((skipped + 1))
    continue
  fi
  echo "Installing ${id} from ${rel}"
  unzip -o -q "${zip}" -d "${INSTALL_DIR}"
  installed=$((installed + 1))
done

echo "Marketplace plugins: installed=${installed} skipped=${skipped} into ${INSTALL_DIR}"
# Note: beam plugin zip includes lib-beam (Beam SDKs).

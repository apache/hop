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
# HOP_PLUGIN_ZIP_DIR: optional flat directory of plugin zips, searched by file name when the
# reactor layout is not available. Docker COPY flattens globs, so the "fast" image builder
# hands the zips over in one directory instead of plugins/<category>/<name>/target/.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIR="${1:-${ROOT}/assemblies/client/target/hop}"
VERSION="${HOP_VERSION:-2.19.0-SNAPSHOT}"
LIST_SCRIPT="${ROOT}/tools/list-marketplace-plugins.sh"
ZIP_DIR="${HOP_PLUGIN_ZIP_DIR:-}"

if [[ ! -d "${INSTALL_DIR}" ]]; then
  echo "Hop install not found: ${INSTALL_DIR}" >&2
  echo "Unzip hop-client first, or pass an existing install path." >&2
  exit 1
fi

if [[ ! -x "${LIST_SCRIPT}" ]]; then
  chmod +x "${LIST_SCRIPT}" 2>/dev/null || true
fi

# `mapfile` is a bash 4+ builtin and macOS still ships bash 3.2, where it aborts the whole
# script under `set -e` and no plugin ever gets installed. Read the list portably instead.
PLUGINS=()
while IFS= read -r entry; do
  [[ -n "${entry}" ]] && PLUGINS+=("${entry}")
done < <(HOP_VERSION="${VERSION}" "${LIST_SCRIPT}" --zips)

installed=0
skipped=0
# bash 3.2 treats an empty array as unbound under `set -u`; expand defensively.
for entry in ${PLUGINS[@]+"${PLUGINS[@]}"}; do
  id="${entry%%|*}"
  rel="${entry#*|}"
  zip="${ROOT}/${rel}"
  if [[ ! -f "${zip}" && -n "${ZIP_DIR}" && -f "${ZIP_DIR}/${rel##*/}" ]]; then
    zip="${ZIP_DIR}/${rel##*/}"
  fi
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
# Note: the Beam plugin zip unpacks SDKs into lib/core (and Spark client jars into lib/spark-client).

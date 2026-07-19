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
# Generate full-client-env.yaml from optional-plugins.yaml (marketplace registry).
#
# Usage:
#   ./tools/generate-full-client-env.sh [HOP_VERSION] [OUTPUT_PATH]
#
# Defaults:
#   HOP_VERSION  from ${project.version} when set by Maven, else 2.19.0-SNAPSHOT
#   OUTPUT_PATH  plugins/misc/marketplace/target/generated/full-client-env.yaml
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REGISTRY="${ROOT}/plugins/misc/marketplace/src/main/resources/org/apache/hop/marketplace/optional-plugins.yaml"
VERSION="${1:-${HOP_VERSION:-2.19.0-SNAPSHOT}}"
OUT="${2:-${ROOT}/plugins/misc/marketplace/target/generated/full-client-env.yaml}"

if [[ ! -f "${REGISTRY}" ]]; then
  echo "Registry not found: ${REGISTRY}" >&2
  exit 1
fi

mkdir -p "$(dirname "${OUT}")"

# Extract artifactIds (lines like "  - artifactId: hop-tech-parquet")
mapfile -t ARTIFACTS < <(sed -n 's/^[[:space:]]*- artifactId:[[:space:]]*\(.*\)$/\1/p' "${REGISTRY}")

if [[ ${#ARTIFACTS[@]} -eq 0 ]]; then
  echo "No plugins found in ${REGISTRY}" >&2
  exit 1
fi

{
  cat <<EOF
# Generated from optional-plugins.yaml — do not edit by hand.
# Rebuild marketplace / hop-client to refresh (tools/generate-full-client-env.sh).
#
# Restores marketplace-optional plugins that used to ship in the fat hop-client.
# From the Hop install directory (network required for ASF/Central or your mirror):
#
#   ./hop marketplace apply -f full-client-env.yaml
#
# Then restart Hop so the plugin registry reloads.
# Do not use --prune unless you intend to remove other marketplace installs.
#
version: "1.0"
hopVersion: "${VERSION}"
enforceOnRun: false
# repositories omitted: uses hop-config defaults (ASF primary + Maven Central)
plugins:
EOF
  for id in "${ARTIFACTS[@]}"; do
    echo "  - artifactId: ${id}"
  done
} >"${OUT}"

echo "Wrote ${OUT} (${#ARTIFACTS[@]} plugins, hopVersion=${VERSION})"

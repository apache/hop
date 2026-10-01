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
# List marketplace-optional plugins from optional-plugins.yaml (source of truth).
#
# Output (one line per unique artifactId, registry order):
#   artifactId|modulePath
#
# Usage:
#   ./tools/list-marketplace-plugins.sh
#   HOP_VERSION=2.19.0-SNAPSHOT ./tools/list-marketplace-plugins.sh --zips
#     → artifactId|modulePath/target/artifactId-${HOP_VERSION}.zip
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REGISTRY="${ROOT}/plugins/misc/marketplace/src/main/resources/org/apache/hop/marketplace/optional-plugins.yaml"
VERSION="${HOP_VERSION:-2.19.0-SNAPSHOT}"
MODE="pairs"

if [[ "${1:-}" == "--zips" ]]; then
  MODE="zips"
elif [[ "${1:-}" == "--modules" ]]; then
  MODE="modules"
elif [[ -n "${1:-}" ]]; then
  echo "Usage: $0 [--zips|--modules]" >&2
  exit 2
fi

if [[ ! -f "${REGISTRY}" ]]; then
  echo "Registry not found: ${REGISTRY}" >&2
  exit 1
fi

# Parse simple YAML entries: - artifactId: … then modulePath: …
# Deduplicate by artifactId (first wins). No yq required.
awk '
  /^[[:space:]]*-[[:space:]]*artifactId:[[:space:]]*/ {
    if (aid != "" && path != "" && !(aid in seen)) {
      seen[aid] = 1
      print aid "|" path
      order[++n] = aid
    }
    line = $0
    sub(/^[[:space:]]*-[[:space:]]*artifactId:[[:space:]]*/, "", line)
    gsub(/[[:space:]]+$/, "", line)
    gsub(/^["'\'']|["'\'']$/, "", line)
    aid = line
    path = ""
    next
  }
  /^[[:space:]]*modulePath:[[:space:]]*/ {
    line = $0
    sub(/^[[:space:]]*modulePath:[[:space:]]*/, "", line)
    gsub(/[[:space:]]+$/, "", line)
    gsub(/^["'\'']|["'\'']$/, "", line)
    path = line
    next
  }
  END {
    if (aid != "" && path != "" && !(aid in seen)) {
      print aid "|" path
    }
  }
' "${REGISTRY}" | while IFS='|' read -r artifactId modulePath; do
  [[ -n "${artifactId}" && -n "${modulePath}" ]] || continue
  case "${MODE}" in
    pairs)
      printf '%s|%s\n' "${artifactId}" "${modulePath}"
      ;;
    zips)
      printf '%s|%s/target/%s-%s.zip\n' \
        "${artifactId}" "${modulePath}" "${artifactId}" "${VERSION}"
      ;;
    modules)
      printf '%s\n' "${modulePath}"
      ;;
  esac
done

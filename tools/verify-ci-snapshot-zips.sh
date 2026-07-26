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
# After a Jenkins-like:
#   mvn … -DaltDeploymentRepository=…::file:./local-snapshots-dir clean deploy
# verify that marketplace-optional plugins have zip artifacts in that tree
# (what wagon:upload would send to ASF snapshots, given current deploy-snapshots
# excludes that keep plugin zips).
#
# Usage (repo root):
#   ./tools/verify-ci-snapshot-zips.sh
#   ./tools/verify-ci-snapshot-zips.sh ./local-snapshots-dir
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIR="${1:-${ROOT}/local-snapshots-dir}"
LIST="${ROOT}/tools/list-marketplace-plugins.sh"
GROUP_PATH="org/apache/hop"

if [[ ! -d "${DIR}" ]]; then
  echo "ERROR: directory not found: ${DIR}" >&2
  echo "Run a Jenkins-like clean deploy first, e.g.:" >&2
  echo "  rm -rf local-snapshots-dir && mkdir local-snapshots-dir" >&2
  echo "  ./mvnw -T 2 -B -DskipTests \\" >&2
  echo "    -DaltDeploymentRepository=snapshot-repo::default::file:\$(pwd)/local-snapshots-dir \\" >&2
  echo "    clean deploy" >&2
  exit 1
fi

if [[ ! -x "${LIST}" ]]; then
  chmod +x "${LIST}" 2>/dev/null || true
fi

mapfile -t IDS < <("${LIST}" | cut -d'|' -f1 | awk 'NF && !seen[$0]++')
if [[ ${#IDS[@]} -eq 0 ]]; then
  echo "ERROR: no plugins listed from optional-plugins.yaml" >&2
  exit 1
fi

echo "==> Checking marketplace plugin zips under ${DIR}"
echo "    (registry: ${#IDS[@]} artifactIds)"
echo

ok=0
missing=0
declare -a MISSING=()

for id in "${IDS[@]}"; do
  # Any zip under the GAV folder (SNAPSHOT unique names included)
  hits=$(find "${DIR}/${GROUP_PATH}/${id}" -name '*.zip' 2>/dev/null | wc -l | tr -d ' ')
  if [[ "${hits}" -gt 0 ]]; then
    sample=$(find "${DIR}/${GROUP_PATH}/${id}" -name '*.zip' 2>/dev/null | head -1)
    echo "  OK  ${id}  (${hits} zip(s), e.g. ${sample#${DIR}/})"
    ok=$((ok + 1))
  else
    echo "  MISSING  ${id}"
    missing=$((missing + 1))
    MISSING+=("${id}")
  fi
done

echo
echo "Summary: ok=${ok} missing=${missing} (of ${#IDS[@]})"

# Remind about wagon excludes (plugin zips are intentionally kept)
echo
echo "Jenkins Deploy stage uses -P deploy-snapshots wagon:upload from local-snapshots-dir."
echo "Current excludes drop hop-assemblies* and core/engine/ui zips — not marketplace plugins."
echo "After merge to main, confirm a zip on ASF snapshots, e.g.:"
echo "  https://repository.apache.org/content/repositories/snapshots/org/apache/hop/hop-tech-parquet/"

if [[ "${missing}" -gt 0 ]]; then
  echo >&2
  echo "FAILED — no zip in ${DIR} for:" >&2
  printf '  - %s\n' "${MISSING[@]}" >&2
  exit 1
fi

echo
echo "PASSED — all marketplace-optional plugins have zips in the CI deploy tree."

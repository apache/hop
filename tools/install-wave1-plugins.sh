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
# Unpack Wave 1 marketplace plugin zips into a Hop install (e.g. for IT images).
# Usage:
#   ./tools/install-wave1-plugins.sh [HOP_INSTALL_DIR]
# Default install dir: assemblies/client/target/hop
#
# Requires plugin modules to have been packaged (*.zip under plugins/**/target/).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIR="${1:-${ROOT}/assemblies/client/target/hop}"
VERSION="${HOP_VERSION:-2.19.0-SNAPSHOT}"

if [[ ! -d "${INSTALL_DIR}" ]]; then
  echo "Hop install not found: ${INSTALL_DIR}" >&2
  echo "Unzip hop-client first, or pass an existing install path." >&2
  exit 1
fi

# artifactId|relative path to zip under repo root
PLUGINS=(
  "hop-engines-spark|plugins/engines/spark/target/hop-engines-spark-${VERSION}.zip"
  "hop-engines-beam|plugins/engines/beam/target/hop-engines-beam-${VERSION}.zip"
  "hop-transform-script|plugins/transforms/script/target/hop-transform-script-${VERSION}.zip"
  "hop-tech-cassandra|plugins/tech/cassandra/target/hop-tech-cassandra-${VERSION}.zip"
  "hop-transform-tika|plugins/transforms/tika/target/hop-transform-tika-${VERSION}.zip"
  "hop-transform-drools|plugins/transforms/drools/target/hop-transform-drools-${VERSION}.zip"
  "hop-tech-parquet|plugins/tech/parquet/target/hop-tech-parquet-${VERSION}.zip"
  "hop-transform-stanfordnlp|plugins/transforms/stanfordnlp/target/hop-transform-stanfordnlp-${VERSION}.zip"
  "hop-tech-arrow|plugins/tech/arrow/target/hop-tech-arrow-${VERSION}.zip"
  "hop-tech-dropbox|plugins/tech/dropbox/target/hop-tech-dropbox-${VERSION}.zip"
  "hop-transform-edi2xml|plugins/transforms/edi2xml/target/hop-transform-edi2xml-${VERSION}.zip"
)

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

echo "Wave 1 plugins: installed=${installed} skipped=${skipped} into ${INSTALL_DIR}"
# Note: beam plugin zip does not include lib/beam (already in core client).

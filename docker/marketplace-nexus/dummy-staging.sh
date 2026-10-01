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
# Dummy marketplace staging: publish optional-plugin zips (from the registry) to
# a Maven repo, verify HTTP layout, and smoke-test hop marketplace install.
#
# Default target is local Sonatype Nexus (same as publish-marketplace-plugins.sh).
# This is the dry-run analogue of an ASF Nexus staging repository check.
#
# Prerequisites:
#   - ./docker/marketplace-nexus/start.sh   (for default local Nexus)
#   - Plugin zips built, or pass --package
#   - Optional: hop client at assemblies/client/target/hop (or HOP_DIR) for install smoke
#
# Usage (repo root):
#   ./docker/marketplace-nexus/dummy-staging.sh
#   ./docker/marketplace-nexus/dummy-staging.sh --package
#   ./docker/marketplace-nexus/dummy-staging.sh --skip-publish   # only verify + smoke
#   ./docker/marketplace-nexus/dummy-staging.sh --skip-smoke
#
# ASF staging (when you have apache.releases.https credentials):
#   After release:perform, set:
#     REPO_URL=https://repository.apache.org/content/repositories/orgapachehop-XXXX/
#     ./docker/marketplace-nexus/dummy-staging.sh --skip-publish
#   Then drop the staging repo if it was only a dry-run.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIST_SCRIPT="${ROOT}/tools/list-marketplace-plugins.sh"
PUBLISH_SCRIPT="${SCRIPT_DIR}/publish-marketplace-plugins.sh"

if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  # shellcheck disable=SC1091
  set -a
  source "${SCRIPT_DIR}/.env"
  set +a
fi

VERSION="${HOP_VERSION:-2.19.0-SNAPSHOT}"
GROUP_ID="${GROUP_ID:-org.apache.hop}"
# NEXUS_REPO_URL / ARTIFACTORY_URL / REPO_URL (ASF staging) all accepted
REPO_URL="${NEXUS_REPO_URL:-${ARTIFACTORY_URL:-${REPO_URL:-http://127.0.0.1:8081/repository/hop-plugins}}}"
REPO_URL="${REPO_URL%/}/"
HOP_DIR="${HOP_DIR:-${ROOT}/assemblies/client/target/hop}"

PACKAGE=0
SKIP_PUBLISH=0
SKIP_SMOKE=0
SMOKE_A="${SMOKE_PLUGIN_A:-hop-tech-dropbox}"
SMOKE_B="${SMOKE_PLUGIN_B:-hop-transform-fake}"

for arg in "$@"; do
  case "${arg}" in
    --package) PACKAGE=1 ;;
    --skip-publish) SKIP_PUBLISH=1 ;;
    --skip-smoke) SKIP_SMOKE=1 ;;
    -h | --help)
      sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      exit 2
      ;;
  esac
done

echo "==> Dummy marketplace staging"
echo "    version:  ${VERSION}"
echo "    repo:     ${REPO_URL}"
echo "    hop dir:  ${HOP_DIR}"
echo

if [[ ! -x "${LIST_SCRIPT}" ]]; then
  chmod +x "${LIST_SCRIPT}" 2>/dev/null || true
fi

mapfile -t ENTRIES < <(HOP_VERSION="${VERSION}" "${LIST_SCRIPT}" --zips)
if [[ ${#ENTRIES[@]} -eq 0 ]]; then
  echo "No plugins in optional-plugins.yaml registry" >&2
  exit 1
fi
echo "    registry: ${#ENTRIES[@]} plugin(s)"

# --- Publish (local Nexus / Artifactory) ---
if [[ "${SKIP_PUBLISH}" -eq 0 ]]; then
  echo
  echo "==> Publish marketplace plugin zips"
  if [[ ! -f "${PUBLISH_SCRIPT}" ]]; then
    echo "Missing ${PUBLISH_SCRIPT}" >&2
    exit 1
  fi
  publish_args=()
  if [[ "${PACKAGE}" -eq 1 ]]; then
    publish_args+=(--package)
  fi
  # Prefer 127.0.0.1 for deploy URL consistency with Hop client
  export NEXUS_REPO_URL="${REPO_URL%/}"
  export HOP_VERSION="${VERSION}"
  "${PUBLISH_SCRIPT}" "${publish_args[@]+"${publish_args[@]}"}"
else
  echo
  echo "==> Skip publish (--skip-publish)"
fi

# --- Verify HTTP Maven layout for every registry plugin ---
echo
echo "==> Verify zip URLs (Maven layout)"
ok=0
fail=0
declare -a FAILED=()
for entry in "${ENTRIES[@]}"; do
  artifactId="${entry%%|*}"
  # SNAPSHOT path uses version folder; unique timestamp filename resolved via maven-metadata later.
  # For releases: {base}{g}/{a}/{v}/{a}-{v}.zip
  # For SNAPSHOT: try literal SNAPSHOT zip first, then accept 200 on metadata + any zip listing.
  gpath="${GROUP_ID//.//}"
  base="${REPO_URL}${gpath}/${artifactId}/${VERSION}/"
  literal_zip="${base}${artifactId}-${VERSION}.zip"
  code=$(curl -sS -o /dev/null -w '%{http_code}' -L "${literal_zip}" || true)
  if [[ "${code}" == "200" ]]; then
    ok=$((ok + 1))
    echo "  OK  ${artifactId} (HTTP ${code})"
    continue
  fi
  # SNAPSHOT unique versions: maven-metadata.xml lists <extension>zip</extension><value>…</value>
  meta="${base}maven-metadata.xml"
  mcode=$(curl -sS -o /tmp/hop-staging-meta.xml -w '%{http_code}' -L "${meta}" || true)
  if [[ "${mcode}" == "200" ]]; then
    # Prefer value that appears with extension zip (snapshotVersion block)
    value=$(
      tr '\n' ' ' </tmp/hop-staging-meta.xml \
        | sed 's/<snapshotVersion>/\n<snapshotVersion>/g' \
        | grep -F '<extension>zip</extension>' \
        | sed -n 's/.*<value>\([^<]*\)<\/value>.*/\1/p' \
        | head -1 || true
    )
    if [[ -z "${value}" ]]; then
      value=$(sed -n 's/.*<value>\([^<]*\)<\/value>.*/\1/p' /tmp/hop-staging-meta.xml | head -1 || true)
    fi
    if [[ -n "${value}" ]]; then
      snap_zip="${base}${artifactId}-${value}.zip"
      scode=$(curl -sS -o /dev/null -w '%{http_code}' -L "${snap_zip}" || true)
      if [[ "${scode}" == "200" ]]; then
        ok=$((ok + 1))
        echo "  OK  ${artifactId} (unique SNAPSHOT ${value}.zip)"
        continue
      fi
      FAILED+=("${artifactId} snap_zip=${scode} value=${value}")
      echo "  FAIL ${artifactId} (SNAPSHOT zip HTTP ${scode} for ${value})"
      fail=$((fail + 1))
      continue
    fi
  fi
  fail=$((fail + 1))
  FAILED+=("${artifactId} literal=${code} meta=${mcode:-n/a}")
  echo "  FAIL ${artifactId} (zip HTTP ${code}, meta HTTP ${mcode:-n/a})"
done
rm -f /tmp/hop-staging-meta.xml

echo
echo "    verified OK=${ok} FAIL=${fail} (of ${#ENTRIES[@]})"
if [[ "${fail}" -gt 0 ]]; then
  echo "Missing or unreachable zips:" >&2
  printf '  - %s\n' "${FAILED[@]}" >&2
  exit 1
fi

# --- Hop marketplace smoke install ---
if [[ "${SKIP_SMOKE}" -eq 1 ]]; then
  echo
  echo "==> Skip install smoke (--skip-smoke)"
  echo "OK — dummy staging verify passed against ${REPO_URL}"
  exit 0
fi

if [[ ! -x "${HOP_DIR}/hop" ]]; then
  echo
  echo "WARN: Hop launcher not found at ${HOP_DIR}/hop — skip install smoke."
  echo "      Build/unzip hop-client or set HOP_DIR."
  echo "OK — dummy staging publish+verify passed against ${REPO_URL}"
  exit 0
fi

# Ensure marketplace CLI plugin is present in the hop install (bundled plugin).
MKT_ZIP="${ROOT}/plugins/misc/marketplace/target/hop-misc-marketplace-${VERSION}.zip"
if [[ ! -f "${HOP_DIR}/plugins/misc/marketplace/version.xml" ]]; then
  if [[ -f "${MKT_ZIP}" ]]; then
    echo "==> Installing marketplace plugin into ${HOP_DIR}"
    unzip -o -q "${MKT_ZIP}" -d "${HOP_DIR}"
  else
    echo "WARN: marketplace plugin not in hop install and zip missing: ${MKT_ZIP}" >&2
    echo "      Smoke may fail with 'Unmatched arguments: marketplace'." >&2
  fi
fi

echo
echo "==> Hop marketplace install smoke (${SMOKE_A}, ${SMOKE_B})"
export NEXUS_REPO_URL="${REPO_URL}"
export HOP_DIR
export HOP_VERSION="${VERSION}"
export SMOKE_PLUGIN_A="${SMOKE_A}"
export SMOKE_PLUGIN_B="${SMOKE_B}"
# Prefer 127.0.0.1 for Hop client (IPv6 localhost issues with Docker)
export NEXUS_REPO_URL="${REPO_URL/localhost/127.0.0.1}"
"${SCRIPT_DIR}/smoke-test.sh"

echo
echo "================================================================"
echo "Dummy staging complete."
echo "  Repository: ${REPO_URL}"
echo "  Registry plugins verified: ${ok}"
echo
echo "ASF release staging (RM) — after release:perform:"
echo "  1. Open https://repository.apache.org/#stagingRepositories"
echo "  2. Open the orgapachehop-* staging repo content"
echo "  3. Confirm e.g. org/apache/hop/hop-tech-parquet/<ver>/…zip"
echo "  4. REPO_URL=<staging-url>/ ./docker/marketplace-nexus/dummy-staging.sh --skip-publish"
echo "  5. Drop the staging repository if this was only a dry-run"
echo "================================================================"

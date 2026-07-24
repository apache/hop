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
# Template: release a third-party Hop plugin zip to a Maven repository (Nexus).
# Copy into your plugin repo as scripts/release-to-nexus.sh and adjust defaults.
#
# Usage:
#   export NEXUS_USER=ci NEXUS_PASSWORD=secret
#   export NEXUS_URL=https://nexus.example/repository/hop-plugins/
#   ./release-plugin-to-nexus.sh 1.2.0 --hop-version 2.19.0
#   ./release-plugin-to-nexus.sh 1.2.0 --dry-run
#
set -euo pipefail

GROUP_ID="${GROUP_ID:-com.example}"
ARTIFACT_ID="${ARTIFACT_ID:-hop-data-vault}"
# Path to the packaged zip after mvn package (override if your layout differs)
ZIP_GLOB="${ZIP_GLOB:-target/${ARTIFACT_ID}-*.zip}"

NEXUS_URL="${NEXUS_URL:-http://127.0.0.1:8081/repository/hop-plugins/}"
NEXUS_REPO_ID="${NEXUS_REPO_ID:-sandbox-hop-plugins}"
HOP_VERSION="${HOP_VERSION:-2.19.0}"
DRY_RUN=false
DO_TAG=false
PLUGIN_VERSION=""

usage() {
  cat <<EOF
Usage: $0 <plugin-version> [options]

  <plugin-version>     Plugin release version (e.g. 1.2.0 or 1.2.0-SNAPSHOT)

Options:
  --hop-version VER    Hop APIs to compile/test against (default: ${HOP_VERSION})
  --nexus-url URL      Maven repo URL (default: ${NEXUS_URL})
  --repo-id ID         settings.xml server id (default: ${NEXUS_REPO_ID})
  --group-id G         Maven groupId (default: ${GROUP_ID})
  --artifact-id A      Maven artifactId (default: ${ARTIFACT_ID})
  --tag                Create git tag plugin-<version> after successful deploy
  --dry-run            Print actions only
  -h, --help           Show this help

Environment:
  NEXUS_USER / NEXUS_PASSWORD   Deploy credentials (or configure settings.xml)
  MVN                          Maven command (default: mvn)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hop-version) HOP_VERSION="$2"; shift 2 ;;
    --nexus-url) NEXUS_URL="$2"; shift 2 ;;
    --repo-id) NEXUS_REPO_ID="$2"; shift 2 ;;
    --group-id) GROUP_ID="$2"; shift 2 ;;
    --artifact-id) ARTIFACT_ID="$2"; shift 2 ;;
    --tag) DO_TAG=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      if [[ -z "${PLUGIN_VERSION}" ]]; then
        PLUGIN_VERSION="$1"
        shift
      else
        echo "Unexpected argument: $1" >&2
        exit 1
      fi
      ;;
  esac
done

if [[ -z "${PLUGIN_VERSION}" ]]; then
  echo "ERROR: plugin version is required" >&2
  usage >&2
  exit 1
fi

MVN="${MVN:-mvn}"
NEXUS_URL="${NEXUS_URL%/}/"

run() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "DRY-RUN: $*"
  else
    "$@"
  fi
}

echo "==> Plugin ${GROUP_ID}:${ARTIFACT_ID}:${PLUGIN_VERSION}"
echo "    Hop compile version: ${HOP_VERSION}"
echo "    Nexus: ${NEXUS_URL} (repo id ${NEXUS_REPO_ID})"

if [[ "${DRY_RUN}" != "true" ]]; then
  if [[ -z "${NEXUS_USER:-}" || -z "${NEXUS_PASSWORD:-}" ]]; then
    echo "NOTE: NEXUS_USER/NEXUS_PASSWORD not set; relying on Maven settings.xml server '${NEXUS_REPO_ID}'"
  fi
fi

# Optional: align Maven project version (requires versions-maven-plugin or CI-friendly versions)
# Uncomment if your pom version should match the release argument:
# run ${MVN} -B versions:set -DnewVersion="${PLUGIN_VERSION}" -DgenerateBackupPoms=false

echo "==> verify (hop.version=${HOP_VERSION})"
run ${MVN} -B clean verify -Dhop.version="${HOP_VERSION}"

echo "==> package zip"
run ${MVN} -B package -DskipTests -Dhop.version="${HOP_VERSION}"

ZIP_FILE=""
# shellcheck disable=SC2086
for f in ${ZIP_GLOB}; do
  if [[ -f "$f" && "$f" == *"${PLUGIN_VERSION}.zip" ]]; then
    ZIP_FILE="$f"
    break
  fi
done
if [[ -z "${ZIP_FILE}" ]]; then
  # fallback: first matching zip
  # shellcheck disable=SC2086
  for f in ${ZIP_GLOB}; do
    if [[ -f "$f" ]]; then
      ZIP_FILE="$f"
      break
    fi
  done
fi

if [[ -z "${ZIP_FILE}" || ! -f "${ZIP_FILE}" ]]; then
  echo "ERROR: zip not found (looked for ${ZIP_GLOB}). Check assembly appendAssemblyId=false." >&2
  exit 1
fi

echo "==> deploy ${ZIP_FILE}"
DEPLOY=(
  ${MVN} -B org.apache.maven.plugins:maven-deploy-plugin:3.1.3:deploy-file
  -DgroupId="${GROUP_ID}"
  -DartifactId="${ARTIFACT_ID}"
  -Dversion="${PLUGIN_VERSION}"
  -Dpackaging=zip
  -Dfile="${ZIP_FILE}"
  -DrepositoryId="${NEXUS_REPO_ID}"
  -Durl="${NEXUS_URL}"
  -DgeneratePom=true
)
if [[ -n "${NEXUS_USER:-}" && -n "${NEXUS_PASSWORD:-}" ]]; then
  # Temporary settings so CI need not write ~/.m2/settings.xml
  SETTINGS="$(mktemp)"
  trap 'rm -f "${SETTINGS}"' EXIT
  cat >"${SETTINGS}" <<EOF
<settings>
  <servers>
    <server>
      <id>${NEXUS_REPO_ID}</id>
      <username>${NEXUS_USER}</username>
      <password>${NEXUS_PASSWORD}</password>
    </server>
  </servers>
</settings>
EOF
  DEPLOY+=(-s "${SETTINGS}")
fi
run "${DEPLOY[@]}"

if [[ "${DO_TAG}" == "true" ]]; then
  TAG="plugin-${PLUGIN_VERSION}"
  echo "==> git tag ${TAG}"
  run git tag -a "${TAG}" -m "Release ${GROUP_ID}:${ARTIFACT_ID}:${PLUGIN_VERSION} (Hop ${HOP_VERSION})"
fi

cat <<EOF

OK — published ${GROUP_ID}:${ARTIFACT_ID}:${PLUGIN_VERSION}

Users can install with:

  ./hop marketplace repo add --id sandbox --url ${NEXUS_URL}
  ./hop marketplace install ${GROUP_ID}:${ARTIFACT_ID}:${PLUGIN_VERSION}
  # restart Hop

Built against Hop ${HOP_VERSION}. Document runtime compatibility in your README.
EOF

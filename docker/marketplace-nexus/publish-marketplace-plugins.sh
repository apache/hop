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
# Deploy marketplace-optional plugin zips to local Nexus, Artifactory, or any
# Maven 2 repository. Plugin list and module paths come only from:
#
#   plugins/misc/marketplace/src/main/resources/org/apache/hop/marketplace/optional-plugins.yaml
#
# Env:
#   NEXUS_REPO_URL or ARTIFACTORY_URL   e.g. http://127.0.0.1:8081/repository/hop-plugins
#   NEXUS_USER / NEXUS_PASSWORD         (or ARTIFACTORY_USER / ARTIFACTORY_PASSWORD)
#   NEXUS_REPO_ID                       Maven server id in settings (default hop-plugins)
#   HOP_VERSION                         default 2.19.0-SNAPSHOT
#   GROUP_ID                            default org.apache.hop (or groupId: in registry)
#
# Flags:
#   --package   run mvn package for all registry modules before deploy
#   --dry-run   print what would be deployed without calling Maven
#
# Usage (from repo root):
#   export NEXUS_PASSWORD=hop-nexus-dev
#   ./docker/marketplace-nexus/publish-marketplace-plugins.sh
#   ./docker/marketplace-nexus/publish-marketplace-plugins.sh --package
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIST_SCRIPT="${ROOT}/tools/list-marketplace-plugins.sh"
REGISTRY="${ROOT}/plugins/misc/marketplace/src/main/resources/org/apache/hop/marketplace/optional-plugins.yaml"

PACKAGE=0
DRY_RUN=0
for arg in "$@"; do
  case "${arg}" in
    --package) PACKAGE=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h | --help)
      sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      echo "Usage: $0 [--package] [--dry-run]" >&2
      exit 2
      ;;
  esac
done

# Load .env from start/configure if present
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  # shellcheck disable=SC1091
  set -a
  source "${SCRIPT_DIR}/.env"
  set +a
fi

REPO_URL="${NEXUS_REPO_URL:-${ARTIFACTORY_URL:-http://127.0.0.1:8081/repository/hop-plugins}}"
REPO_URL="${REPO_URL%/}"
REPO_ID="${NEXUS_REPO_ID:-hop-plugins}"
USER="${NEXUS_USER:-${NEXUS_ADMIN_USER:-${ARTIFACTORY_USER:-admin}}}"
PASS="${NEXUS_PASSWORD:-${NEXUS_ADMIN_PASSWORD:-${ARTIFACTORY_PASSWORD:-}}}"
VERSION="${HOP_VERSION:-2.19.0-SNAPSHOT}"

# Prefer groupId from registry when set
GROUP_ID="${GROUP_ID:-}"
if [[ -z "${GROUP_ID}" && -f "${REGISTRY}" ]]; then
  GROUP_ID="$(sed -n 's/^groupId:[[:space:]]*//p' "${REGISTRY}" | head -1 | tr -d '[:space:]"'"'"'')"
fi
GROUP_ID="${GROUP_ID:-org.apache.hop}"

if [[ ! -x "${LIST_SCRIPT}" ]]; then
  chmod +x "${LIST_SCRIPT}" 2>/dev/null || true
fi
if [[ ! -f "${LIST_SCRIPT}" ]]; then
  echo "Missing ${LIST_SCRIPT}" >&2
  exit 1
fi

mapfile -t ENTRIES < <(HOP_VERSION="${VERSION}" "${LIST_SCRIPT}" --zips)
if [[ ${#ENTRIES[@]} -eq 0 ]]; then
  echo "No plugins found in ${REGISTRY}" >&2
  exit 1
fi

MVN="${ROOT}/mvnw"
if [[ ! -x "${MVN}" ]]; then
  MVN=mvn
fi

if [[ "${PACKAGE}" -eq 1 ]]; then
  mapfile -t MODULES < <("${LIST_SCRIPT}" --modules | awk 'NF && !seen[$0]++')
  if [[ ${#MODULES[@]} -eq 0 ]]; then
    echo "No modulePath entries to package" >&2
    exit 1
  fi
  PL_LIST="$(IFS=,; echo "${MODULES[*]}")"
  echo "Packaging ${#MODULES[@]} module(s) (HOP_VERSION=${VERSION})…"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "DRY-RUN: ${MVN} -pl ${PL_LIST} -am package -DskipTests"
  else
    "${MVN}" -pl "${PL_LIST}" -am package -DskipTests
  fi
fi

if [[ "${DRY_RUN}" -eq 0 && -z "${PASS}" ]]; then
  echo "Set NEXUS_PASSWORD (or NEXUS_ADMIN_PASSWORD / ARTIFACTORY_PASSWORD)." >&2
  echo "After ./docker/marketplace-nexus/start.sh the default is hop-nexus-dev." >&2
  exit 1
fi

SETTINGS=""
if [[ "${DRY_RUN}" -eq 0 ]]; then
  SETTINGS="$(mktemp)"
  trap 'rm -f "${SETTINGS}"' EXIT
  cat >"${SETTINGS}" <<EOF
<settings>
  <servers>
    <server>
      <id>${REPO_ID}</id>
      <username>${USER}</username>
      <password>${PASS}</password>
    </server>
  </servers>
</settings>
EOF
fi

deploy_one() {
  local artifactId="$1"
  local rel="$2"
  local file="${ROOT}/${rel}"
  if [[ ! -f "${file}" ]]; then
    echo "SKIP (not built): ${artifactId} — missing ${rel}"
    return 0
  fi
  echo "Deploying ${GROUP_ID}:${artifactId}:${VERSION} → ${REPO_URL}"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "DRY-RUN: deploy-file ${file}"
    return 0
  fi
  "${MVN}" -q -s "${SETTINGS}" \
    org.apache.maven.plugins:maven-deploy-plugin:3.1.3:deploy-file \
    -DgroupId="${GROUP_ID}" \
    -DartifactId="${artifactId}" \
    -Dversion="${VERSION}" \
    -Dpackaging=zip \
    -Dfile="${file}" \
    -DrepositoryId="${REPO_ID}" \
    -Durl="${REPO_URL}" \
    -DgeneratePom=true \
    -DretryFailedDeploymentCount=2
}

deployed=0
skipped=0
for entry in "${ENTRIES[@]}"; do
  artifactId="${entry%%|*}"
  rel="${entry#*|}"
  if [[ ! -f "${ROOT}/${rel}" ]]; then
    echo "SKIP (not built): ${artifactId} — missing ${rel}"
    skipped=$((skipped + 1))
    continue
  fi
  deploy_one "${artifactId}" "${rel}"
  deployed=$((deployed + 1))
done

echo "Done. deployed=${deployed} skipped=${skipped}  base URL: ${REPO_URL}/"
echo "Example: hop marketplace install hop-tech-parquet"
echo "Registry: ${REGISTRY}"

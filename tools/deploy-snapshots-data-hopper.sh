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
# Deploy Hop SNAPSHOT artifacts to a private Maven repo (e.g. data-hopper),
# similar in spirit to Jenkins local deploy + upload — without changing the
# committed apache/hop distributionManagement.
#
# Modes:
#   marketplace (default)  — package + deploy-file all optional-plugins.yaml zips
#   reactor                — full mvn clean deploy (-P=-assemblies) to the repo
#
# Credentials (never commit these):
#   marketplace mode: HOP_DEPLOY_USER + HOP_DEPLOY_PASSWORD (or NEXUS_* / ARTIFACTORY_*)
#   reactor mode:     ~/.m2/settings.xml server id matching DATA_HOPPER_SERVER_ID
#
# Usage (from Hop repo root):
#   export HOP_DEPLOY_PASSWORD='…'
#   ./tools/deploy-snapshots-data-hopper.sh
#   ./tools/deploy-snapshots-data-hopper.sh marketplace
#   ./tools/deploy-snapshots-data-hopper.sh reactor
#
# Env overrides:
#   DATA_HOPPER_URL          default https://repository.data-hopper.com/repository/apache-hop-plugins/
#   DATA_HOPPER_SERVER_ID    default apache-hop-plugins
#   HOP_DEPLOY_USER          default hop_build
#   HOP_DEPLOY_PASSWORD      required for marketplace mode
#   HOP_VERSION              default 2.19.0-SNAPSHOT
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
URL="${DATA_HOPPER_URL:-https://repository.data-hopper.com/repository/apache-hop-plugins/}"
URL="${URL%/}"
ID="${DATA_HOPPER_SERVER_ID:-apache-hop-plugins}"
MODE="${1:-marketplace}"
VERSION="${HOP_VERSION:-2.19.0-SNAPSHOT}"
USER="${HOP_DEPLOY_USER:-${NEXUS_USER:-${ARTIFACTORY_USER:-hop_build}}}"
PASS="${HOP_DEPLOY_PASSWORD:-${NEXUS_PASSWORD:-${ARTIFACTORY_PASSWORD:-}}}"

MVN="${ROOT}/mvnw"
if [[ ! -x "${MVN}" ]]; then
  MVN=mvn
fi

case "${MODE}" in
  marketplace | plugins)
    if [[ -z "${PASS}" ]]; then
      echo "Set HOP_DEPLOY_PASSWORD (or NEXUS_PASSWORD / ARTIFACTORY_PASSWORD) for marketplace deploy." >&2
      exit 1
    fi
    echo "==> Marketplace plugin SNAPSHOTs → ${URL}"
    echo "    server id: ${ID}  user: ${USER}  version: ${VERSION}"
    # Force data-hopper (or override) — must win over docker/marketplace-nexus/.env
    export ARTIFACTORY_URL="${URL}"
    export NEXUS_REPO_URL="${URL}"
    export NEXUS_URL="${URL}"
    export NEXUS_REPO_ID="${ID}"
    export ARTIFACTORY_USER="${USER}"
    export NEXUS_USER="${USER}"
    # Do not let local .env admin password replace hop_build
    export ARTIFACTORY_PASSWORD="${PASS}"
    export NEXUS_PASSWORD="${PASS}"
    unset NEXUS_ADMIN_PASSWORD 2>/dev/null || true
    export HOP_VERSION="${VERSION}"
    exec "${ROOT}/docker/marketplace-nexus/publish-marketplace-plugins.sh" --package
    ;;

  reactor | full)
    echo "==> Reactor SNAPSHOT deploy (-P=-assemblies) → ${URL}"
    echo "    server id: ${ID}  (credentials from ~/.m2/settings.xml)"
    echo "    Requires <server><id>${ID}</id>… in settings.xml"
    # shellcheck disable=SC2086
    exec "${MVN}" -f "${ROOT}/pom.xml" clean deploy \
      -DskipTests \
      -P=-assemblies \
      -DaltDeploymentRepository="${ID}::default::${URL}/" \
      ${MVN_EXTRA_ARGS:-}
    ;;

  -h | --help | help)
    sed -n '2,45p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;

  *)
    echo "Usage: $0 [marketplace|reactor]" >&2
    exit 2
    ;;
esac

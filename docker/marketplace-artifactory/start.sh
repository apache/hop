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
# Start Artifactory OSS, create the hop-plugins-local Maven repo, enable anonymous read.
#
# Usage:
#   ./docker/marketplace-artifactory/start.sh
#   ./docker/marketplace-artifactory/start.sh --reset          # wipe volumes
#   ARTIFACTORY_ADMIN_PASSWORD=secret ./docker/marketplace-artifactory/start.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

BASE_URL="${ARTIFACTORY_URL:-http://localhost:8082}"
ADMIN_USER="${ARTIFACTORY_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${ARTIFACTORY_ADMIN_PASSWORD:-password}"
REPO_KEY="${ARTIFACTORY_REPO:-example-repo-local}"
RESET=false

for arg in "$@"; do
  case "${arg}" in
    --reset|-r) RESET=true ;;
    -h|--help)
      echo "Usage: $0 [--reset]"
      echo "  ARTIFACTORY_ADMIN_PASSWORD  admin password (default: password)"
      echo "  ARTIFACTORY_REPO            existing repo to publish into (default: example-repo-local)"
      exit 0
      ;;
  esac
done

if [[ "${RESET}" == "true" ]]; then
  echo "Wiping Artifactory data..."
  docker compose down -v
fi

docker compose up -d

echo "Waiting for Artifactory (first start takes several minutes)..."
for _ in $(seq 1 120); do
  if curl -sf "${BASE_URL}/artifactory/api/system/ping" >/dev/null 2>&1; then
    echo "Artifactory is up."
    break
  fi
  sleep 5
done

if ! curl -sf "${BASE_URL}/artifactory/api/system/ping" >/dev/null 2>&1; then
  echo "Artifactory did not become ready." >&2
  echo "  docker compose -f ${SCRIPT_DIR}/docker-compose.yml logs -f artifactory" >&2
  # The router logs a generic retry loop whatever the cause; the real reason is in the service log.
  echo "  docker exec hop-marketplace-artifactory tail -50 /opt/jfrog/artifactory/var/log/access-service.log" >&2
  exit 1
fi

auth=(-u "${ADMIN_USER}:${ADMIN_PASSWORD}")

# Creating a repository over REST is an Artifactory Pro feature; on OSS it answers 400 with
# "This REST API is available only in Artifactory Pro". So use a repository that already exists
# rather than trying to make one. The OSS image ships example-repo-local, which is fine here:
# Hop matches paths and .zip names and never asks what package type a repository declares.
echo "Checking repository '${REPO_KEY}'..."
repositories=$(curl -sf "${auth[@]}" "${BASE_URL}/artifactory/api/repositories" || echo '[]')
if printf '%s' "${repositories}" | grep -q "\"${REPO_KEY}\""; then
  echo "  present."
else
  echo "  '${REPO_KEY}' does not exist." >&2
  echo "  Artifactory OSS cannot create repositories over REST. Either set" >&2
  echo "  ARTIFACTORY_REPO to one that exists, or create it in the UI at ${BASE_URL}/ui/." >&2
  echo "  Repositories present: $(printf '%s' "${repositories}" | tr -d ' \n')" >&2
  exit 1
fi

# Turn anonymous access on. Note POST answers 415 here — the config descriptor takes a PATCH.
# This is only half of anonymous read: the anonymous user also needs read permission on the
# repository, which is a permission target, and those are Pro-only. On OSS the requests stop
# failing with 401 and start failing with 403, so grant it in the UI if you want that path.
echo "Enabling anonymous access..."
if curl -sf "${auth[@]}" -X PATCH "${BASE_URL}/artifactory/api/system/configuration" \
  -H 'Content-Type: application/yaml' \
  --data-binary 'security:
  anonAccessEnabled: true
' >/dev/null; then
  echo "  anonymous access enabled."
else
  echo "  could not enable anonymous access; use credentials." >&2
fi

cat <<EOF

Artifactory is ready.

  UI            ${BASE_URL}/ui/
  Admin         ${ADMIN_USER} / ${ADMIN_PASSWORD}
  Maven repo    ${BASE_URL}/artifactory/${REPO_KEY}/

Publish the marketplace plugin zips:

  export ARTIFACTORY_URL='${BASE_URL}/artifactory/${REPO_KEY}'
  export ARTIFACTORY_USER='${ADMIN_USER}'
  export ARTIFACTORY_PASSWORD='${ADMIN_PASSWORD}'
  export NEXUS_REPO_ID=artifactory
  ../marketplace-nexus/publish-marketplace-plugins.sh --package

Point Hop at it (from the unzipped hop-client directory):

  ./hop marketplace repo add --id artifactory \\
    --url ${BASE_URL}/artifactory/${REPO_KEY}/ --primary --browse \\
    --username ${ADMIN_USER} --password ${ADMIN_PASSWORD}
  ./hop marketplace query

Credentials are used above because granting the anonymous user read access needs a
permission target, and those are Pro-only. Grant it in the UI to try the anonymous
path, which is the one that browses by walking /api/storage instead of using AQL.

EOF

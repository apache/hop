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
# Generate keys (if needed) and start Artifactory + PostgreSQL.
#
# Usage:
#   ./docker/marketplace-artifactory/start.sh
#   ./docker/marketplace-artifactory/start.sh --reset   # wipe volumes (clean DB)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

RESET=false
for arg in "$@"; do
  case "${arg}" in
    --reset|-r) RESET=true ;;
    -h|--help)
      echo "Usage: $0 [--reset]"
      exit 0
      ;;
  esac
done

./generate-keys.sh

if [[ ! -f .env ]]; then
  echo "ERROR: .env missing after generate-keys.sh" >&2
  exit 1
fi

if [[ "${RESET}" == "true" ]]; then
  echo "Resetting volumes (removes all Artifactory/Postgres data)..."
  docker compose --env-file .env down -v
fi

echo "Starting Artifactory + PostgreSQL..."
docker compose --env-file .env up -d

echo
echo "Waiting for health (can take 2–4 minutes on first boot)..."
echo "  Logs: docker logs -f hop-marketplace-artifactory"
echo "  UI:   http://localhost:8082"
echo
echo "Create a local Maven repo key 'hop-plugins-local', then:"
echo "  export ARTIFACTORY_URL=http://localhost:8082/artifactory/hop-plugins-local"
echo "  export ARTIFACTORY_USER=admin"
echo "  export ARTIFACTORY_PASSWORD=…   # set on first login"
echo "  ./publish-wave1-plugins.sh"

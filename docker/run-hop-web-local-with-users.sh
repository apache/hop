#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Build and run a local Hop Web Docker image with multi-role BASIC auth.
# Mounts docker/local-auth-config as /config (tomcat-users.xml + web.xml).
#
# Typical workflow:
#   ./docker/run-hop-web-local-with-users.sh              # build, image, start with auth
#   ./docker/run-hop-web-local-with-users.sh --quick      # faster rebuild after rap/ui changes
#   ./docker/run-hop-web-local-with-users.sh --run-only   # restart existing hop-web:local image
#   ./docker/run-hop-web-local-with-users.sh --stop       # stop and remove the container
#
# Hop Web UI (BASIC auth):  http://localhost:8080/ui
# Test users: see docker/local-auth-config/README.md

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME="hop-web:local"
CONTAINER_NAME="hop-web-local-auth"
HOST_PORT="8080"
HOP_UI_PATH="/ui"

# Mounted into the container as /config (picked up by docker/resources/run-web.sh)
AUTH_CONFIG_DIR="${SCRIPT_DIR}/local-auth-config"

# Used for readiness probe (must match local-auth-config/tomcat-users.xml)
PROBE_USER="${HOP_WEB_PROBE_USER:-admin}"
PROBE_PASSWORD="${HOP_WEB_PROBE_PASSWORD:-admin}"

SKIP_MAVEN=false
SKIP_DOCKER_BUILD=false
RUN_ONLY=false
STOP_ONLY=false
QUICK_BUILD=false
DOCKER_NO_CACHE=false
FOLLOW_LOGS=false
BUILD_ONLY=false

log() {
  echo "$(date '+%H:%M:%S') $*"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

usage() {
  # Print the comment block under the license header (lines that introduce this script)
  sed -n '18,28p' "$0" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Options:
  --skip-maven       Skip the Maven build (use existing target/ artifacts)
  --quick            Faster Maven build (rap/ui/engine/core + web assembly only)
  --build-only       Build the Docker image but do not start a container
  --run-only         Start the container without rebuilding (image must exist)
  --stop             Stop and remove the local auth container
  --port PORT        Host port to map to container 8080 (default: 8080)
  --config DIR       Host directory to mount as /config (default: docker/local-auth-config)
  --no-cache         Build the Docker image without layer cache
  --logs             Follow container logs after start
  -h, --help         Show this help

Environment:
  MAVEN_ARGS              Extra arguments passed to ./mvnw (e.g. '-T 2C')
  HOP_WEB_PROBE_USER      Username for readiness curl (default: admin)
  HOP_WEB_PROBE_PASSWORD  Password for readiness curl (default: admin)

Test users (local-auth-config/tomcat-users.xml):
  admin / admin           hop-admin     → Admin (full)
  developer / developer   hop-user      → User (CRUD + execute)
  operator / operator     hop-operator  → Operator (view + execute)
  viewer / viewer         hop-readonly  → Read-only (view only)
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --skip-maven)
        SKIP_MAVEN=true
        shift
        ;;
      --quick)
        QUICK_BUILD=true
        shift
        ;;
      --build-only)
        BUILD_ONLY=true
        shift
        ;;
      --run-only)
        RUN_ONLY=true
        SKIP_MAVEN=true
        SKIP_DOCKER_BUILD=true
        shift
        ;;
      --stop)
        STOP_ONLY=true
        shift
        ;;
      --port)
        HOST_PORT="${2:?--port requires a value}"
        shift 2
        ;;
      --config)
        AUTH_CONFIG_DIR="${2:?--config requires a directory}"
        shift 2
        ;;
      --no-cache)
        DOCKER_NO_CACHE=true
        shift
        ;;
      --logs)
        FOLLOW_LOGS=true
        shift
        ;;
      -h | --help)
        usage
        exit 0
        ;;
      *)
        die "Unknown option: $1 (use --help)"
        ;;
    esac
  done
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed"
}

stop_container() {
  if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    log "Stopping container ${CONTAINER_NAME}"
    docker rm -f "${CONTAINER_NAME}" >/dev/null
  fi
}

# Reuse the same image build pipeline as run-hop-web-local.sh
run_maven() {
  require_command "${PROJECT_ROOT}/mvnw"
  cd "${PROJECT_ROOT}"

  local mvn_profile=(install)
  local mvn_modules=(-pl assemblies/web,assemblies/client -am)
  if [[ "${QUICK_BUILD}" == "true" ]]; then
    log "Quick Maven build: ui, rap, engine, core and web assembly"
    mvn_modules=(-pl ui,rap,assemblies/web -am)
  else
    log "Full Hop Web Maven build (client + web assemblies)"
  fi

  # shellcheck disable=SC2206
  local extra_args=(${MAVEN_ARGS:-})

  log "Running: ./mvnw ${mvn_profile[*]} ${mvn_modules[*]} -DskipTests ..."
  ./mvnw "${mvn_profile[@]}" "${mvn_modules[@]}" \
    -DskipTests \
    -Dspotless.check.skip=true \
    -Drat.skip=true \
    -Dcheckstyle.skip=true \
    "${extra_args[@]}"
}

ensure_client_layout() {
  local client_zip
  client_zip="$(ls -1 "${PROJECT_ROOT}"/assemblies/client/target/hop-client-*.zip 2>/dev/null | head -1 || true)"
  [[ -n "${client_zip}" ]] || die "Client assembly not found. Run without --skip-maven first."

  if [[ ! -d "${PROJECT_ROOT}/assemblies/client/target/hop/config" ]]; then
    log "Unpacking client assembly"
    rm -rf "${PROJECT_ROOT}/assemblies/client/target/hop"
    unzip -q "${client_zip}" -d "${PROJECT_ROOT}/assemblies/client/target"
  fi
}

overlay_module_jars() {
  local dest_dir="$1"
  mkdir -p "${dest_dir}"

  for module in core engine ui; do
    local jar
    jar="$(ls -1 "${PROJECT_ROOT}/${module}"/target/hop-"${module}"-*-SNAPSHOT.jar 2>/dev/null | grep -v tests | head -1 || true)"
    if [[ -n "${jar}" ]]; then
      log "  ${module}: $(basename "${jar}") -> ${dest_dir}"
      cp -f "${jar}" "${dest_dir}/"
    else
      die "Missing ${module} SNAPSHOT jar in ${PROJECT_ROOT}/${module}/target/"
    fi
  done

  local rap_jar
  rap_jar="$(ls -1 "${PROJECT_ROOT}"/rap/target/hop-ui-rap-*-SNAPSHOT.jar 2>/dev/null | head -1 || true)"
  [[ -n "${rap_jar}" ]] || die "Missing rap SNAPSHOT jar in ${PROJECT_ROOT}/rap/target/"
  log "  rap: $(basename "${rap_jar}") -> ${dest_dir}"
  cp -f "${rap_jar}" "${dest_dir}/"
}

prepare_webapp() {
  local war="${PROJECT_ROOT}/assemblies/web/target/hop.war"
  local webapp="${PROJECT_ROOT}/assemblies/web/target/webapp"
  local client_lib="${PROJECT_ROOT}/assemblies/client/target/hop/lib/core"

  [[ -f "${war}" ]] || die "hop.war not found at ${war}. Run Maven build first."

  log "Preparing exploded web application"
  rm -rf "${webapp}"
  unzip -q "${war}" -d "${webapp}"

  log "Overlaying latest Hop module JARs (core, engine, ui, rap)"
  overlay_module_jars "${webapp}/WEB-INF/lib"
  overlay_module_jars "${client_lib}"

  rm -f "${webapp}/WEB-INF/lib"/hop-ui-rcp-*.jar
  log "Removed hop-ui-rcp from staged webapp WEB-INF/lib (Hop Web must use hop-ui-rap only)"
}

build_image() {
  require_command docker
  ensure_client_layout
  prepare_webapp

  local cache_flag=()
  if [[ "${DOCKER_NO_CACHE}" == "true" ]]; then
    cache_flag=(--no-cache)
  fi

  log "Building Docker image ${IMAGE_NAME}"
  docker build "${PROJECT_ROOT}" \
    -f "${SCRIPT_DIR}/web.Dockerfile" \
    -t "${IMAGE_NAME}" \
    "${cache_flag[@]}"

  log "Cleaning up exploded webapp staging directory"
  rm -rf "${PROJECT_ROOT}/assemblies/web/target/webapp"
}

validate_auth_config() {
  [[ -d "${AUTH_CONFIG_DIR}" ]] || die "Auth config directory not found: ${AUTH_CONFIG_DIR}"
  [[ -f "${AUTH_CONFIG_DIR}/tomcat-users.xml" ]] || die "Missing ${AUTH_CONFIG_DIR}/tomcat-users.xml"
  [[ -f "${AUTH_CONFIG_DIR}/web.xml" ]] || die "Missing ${AUTH_CONFIG_DIR}/web.xml"
  log "Auth config: ${AUTH_CONFIG_DIR}"
}

wait_for_hop_web() {
  local url="http://localhost:${HOST_PORT}${HOP_UI_PATH}"
  local attempts=60
  log "Waiting for Hop Web at ${url} (BASIC auth as ${PROBE_USER})"
  for ((i = 1; i <= attempts; i++)); do
    # 200 with credentials = ready; 401 without would also mean Tomcat is up, but we
    # require successful auth so misconfigured tomcat-users/web.xml fail the probe.
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' \
      -u "${PROBE_USER}:${PROBE_PASSWORD}" \
      "${url}" || true)"
    if [[ "${code}" == "200" ]]; then
      log "Hop Web is ready (HTTP ${code})"
      return 0
    fi
    if [[ "${i}" -eq 1 || $((i % 10)) -eq 0 ]]; then
      log "  still waiting... (HTTP ${code:-none}, attempt ${i}/${attempts})"
    fi
    sleep 2
  done
  die "Hop Web did not become ready within $((attempts * 2)) seconds. Check: docker logs ${CONTAINER_NAME}"
}

ensure_audit_volume() {
  local audit_dir="$1"
  local hop_uid="${HOP_UID:-501}"
  local hop_gid="${HOP_GID:-501}"

  mkdir -p "${audit_dir}"

  # Nested per-user dirs may be 0750 owned by the host user; open so hop (501) can write.
  if chown -R "${hop_uid}:${hop_gid}" "${audit_dir}" 2>/dev/null; then
    log "Audit volume ownership: ${hop_uid}:${hop_gid} (recursive)"
  fi
  if chmod -R a+rwX "${audit_dir}" 2>/dev/null; then
    log "Audit volume permissions: world-writable recursive (container user uid ${hop_uid})"
    return 0
  fi

  die "Cannot make ${audit_dir} writable by Hop Web (uid ${hop_uid})"
}

run_container() {
  require_command docker
  validate_auth_config
  stop_container

  local data_dir="${SCRIPT_DIR}/local-data"
  ensure_audit_volume "${data_dir}/audit"

  # Absolute path required for Docker bind mounts
  local auth_config_abs
  auth_config_abs="$(cd "${AUTH_CONFIG_DIR}" && pwd)"

  log "Starting ${CONTAINER_NAME} on port ${HOST_PORT}"
  log "Mounting auth config: ${auth_config_abs} -> /config"
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -p "${HOST_PORT}:8080" \
    -e HOP_LOG_LEVEL=Basic \
    -e HOP_GUI_ZOOM_FACTOR=1.0 \
    -e HOP_AUDIT_FOLDER=/tmp/hop-web-audit \
    -v "${data_dir}/audit:/tmp/hop-web-audit" \
    -v "${auth_config_abs}:/config:ro" \
    "${IMAGE_NAME}" >/dev/null

  wait_for_hop_web

  echo ""
  echo "Hop Web is running with multi-role BASIC authentication:"
  echo "  Light UI:  http://localhost:${HOST_PORT}/ui"
  echo "  Dark UI:   http://localhost:${HOST_PORT}/ui-dark"
  echo ""
  echo "Test users (local only — see docker/local-auth-config/README.md):"
  echo "  admin      / admin       → Admin (full access)"
  echo "  developer  / developer   → User (CRUD + execute)"
  echo "  operator   / operator    → Operator (view + execute, no save)"
  echo "  viewer     / viewer      → Read-only (view only)"
  echo ""
  echo "What to check:"
  echo "  - Browser login prompt (BASIC)"
  echo "  - Window title includes [username]"
  echo "  - operator: Save disabled; Run still enabled"
  echo "  - viewer:   Save and Run disabled"
  echo ""
  echo "Container:   ${CONTAINER_NAME}"
  echo "Image:       ${IMAGE_NAME}"
  echo "Auth config: ${auth_config_abs}"
  echo "Audit data:  ${data_dir}/audit"
  echo ""
  echo "Logs:        docker logs -f ${CONTAINER_NAME}"
  echo "Stop:        ${SCRIPT_DIR}/run-hop-web-local-with-users.sh --stop"

  if [[ "${FOLLOW_LOGS}" == "true" ]]; then
    docker logs -f "${CONTAINER_NAME}"
  fi
}

main() {
  parse_args "$@"

  if [[ "${STOP_ONLY}" == "true" ]]; then
    stop_container
    log "Done"
    exit 0
  fi

  require_command docker
  if ! docker info >/dev/null 2>&1; then
    die "Docker daemon is not running"
  fi

  if [[ "${RUN_ONLY}" != "true" ]]; then
    if [[ "${SKIP_MAVEN}" != "true" ]]; then
      run_maven
    fi
    if [[ "${SKIP_DOCKER_BUILD}" != "true" ]]; then
      build_image
    fi
  else
    docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1 \
      || die "Image ${IMAGE_NAME} not found. Build first without --run-only."
  fi

  if [[ "${BUILD_ONLY}" == "true" ]]; then
    log "Build complete (container not started)"
    exit 0
  fi

  run_container
}

main "$@"

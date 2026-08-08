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
# Build and run Hop Web with Hop-managed BASIC authentication (no Tomcat realm).
# Seeds demo users admin/developer/operator/viewer (password = username).
#
#   ./docker/run-hop-web-local-with-basic.sh
#   ./docker/run-hop-web-local-with-basic.sh --quick
#   ./docker/run-hop-web-local-with-basic.sh --run-only
#   ./docker/run-hop-web-local-with-basic.sh --stop
#
# UI: http://localhost:8080/ui  (browser BASIC login)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME="hop-web:local"
CONTAINER_NAME="hop-web-local-basic"
HOST_PORT="8080"
HOP_UI_PATH="/ui"

# Persist generated users.json across restarts (under container HOP_CONFIG_FOLDER/security)
SECURITY_DATA_DIR="${SCRIPT_DIR}/local-data/basic-security"

SKIP_MAVEN=false
SKIP_DOCKER_BUILD=false
RUN_ONLY=false
STOP_ONLY=false
QUICK_BUILD=false
DOCKER_NO_CACHE=false
FOLLOW_LOGS=false
BUILD_ONLY=false

# Demo probe credentials (match seeded users)
PROBE_USER="${HOP_WEB_PROBE_USER:-admin}"
PROBE_PASSWORD="${HOP_WEB_PROBE_PASSWORD:-admin}"

log() {
  echo "$(date '+%H:%M:%S') $*"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

usage() {
  sed -n '18,28p' "$0" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Options:
  --skip-maven       Skip the Maven build
  --quick            Faster Maven build (ui/rap + web assembly)
  --build-only       Build image only
  --run-only         Start without rebuilding
  --stop             Stop and remove the basic-auth container
  --port PORT        Host port (default: 8080)
  --no-cache         Docker build --no-cache
  --logs             Follow container logs
  -h, --help         Show this help

Environment (passed into the container):
  HOP_WEB_SECURITY_MODE=BASIC          (forced by this script)
  HOP_WEB_SEED_DEMO_USERS=true         (forced — seeds 4 demo users when store empty)
  HOP_WEB_PROBE_USER / PASSWORD        readiness check credentials

Demo users (password = username):
  admin / admin           Admin
  developer / developer   User
  operator / operator     Operator
  viewer / viewer         Read-only
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --skip-maven) SKIP_MAVEN=true; shift ;;
      --quick) QUICK_BUILD=true; shift ;;
      --build-only) BUILD_ONLY=true; shift ;;
      --run-only) RUN_ONLY=true; SKIP_MAVEN=true; SKIP_DOCKER_BUILD=true; shift ;;
      --stop) STOP_ONLY=true; shift ;;
      --port) HOST_PORT="${2:?}"; shift 2 ;;
      --no-cache) DOCKER_NO_CACHE=true; shift ;;
      --logs) FOLLOW_LOGS=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "Unknown option: $1" ;;
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

run_maven() {
  require_command "${PROJECT_ROOT}/mvnw"
  cd "${PROJECT_ROOT}"
  local mvn_modules=(-pl assemblies/web,assemblies/client -am)
  if [[ "${QUICK_BUILD}" == "true" ]]; then
    log "Quick Maven build: ui, rap, assemblies/web"
    mvn_modules=(-pl ui,rap,assemblies/web -am)
  else
    log "Full Hop Web Maven build"
  fi
  # shellcheck disable=SC2206
  local extra_args=(${MAVEN_ARGS:-})
  ./mvnw install "${mvn_modules[@]}" \
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
    [[ -n "${jar}" ]] || die "Missing ${module} SNAPSHOT jar"
    log "  ${module}: $(basename "${jar}")"
    cp -f "${jar}" "${dest_dir}/"
  done
  local rap_jar
  rap_jar="$(ls -1 "${PROJECT_ROOT}"/rap/target/hop-ui-rap-*-SNAPSHOT.jar 2>/dev/null | head -1 || true)"
  [[ -n "${rap_jar}" ]] || die "Missing rap SNAPSHOT jar"
  log "  rap: $(basename "${rap_jar}")"
  cp -f "${rap_jar}" "${dest_dir}/"
}

prepare_webapp() {
  local war="${PROJECT_ROOT}/assemblies/web/target/hop.war"
  local webapp="${PROJECT_ROOT}/assemblies/web/target/webapp"
  local client_lib="${PROJECT_ROOT}/assemblies/client/target/hop/lib/core"
  [[ -f "${war}" ]] || die "hop.war not found"
  log "Preparing exploded web application"
  rm -rf "${webapp}"
  unzip -q "${war}" -d "${webapp}"
  log "Overlaying latest Hop module JARs"
  overlay_module_jars "${webapp}/WEB-INF/lib"
  overlay_module_jars "${client_lib}"
  rm -f "${webapp}/WEB-INF/lib"/hop-ui-rcp-*.jar
}

build_image() {
  require_command docker
  ensure_client_layout
  prepare_webapp
  local cache_flag=()
  [[ "${DOCKER_NO_CACHE}" == "true" ]] && cache_flag=(--no-cache)
  log "Building Docker image ${IMAGE_NAME}"
  docker build "${PROJECT_ROOT}" -f "${SCRIPT_DIR}/web.Dockerfile" -t "${IMAGE_NAME}" "${cache_flag[@]}"
  rm -rf "${PROJECT_ROOT}/assemblies/web/target/webapp"
}

wait_for_hop_web() {
  local login_url="http://localhost:${HOST_PORT}/login"
  local ui_url="http://localhost:${HOST_PORT}${HOP_UI_PATH}"
  local attempts=90
  local cookie_jar
  cookie_jar="$(mktemp)"
  log "Waiting for Hop Web login page at ${login_url}"
  for ((i = 1; i <= attempts; i++)); do
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' "${login_url}" || true)"
    if [[ "${code}" == "200" ]]; then
      # Form login then open UI with session cookie
      curl -s -o /dev/null -c "${cookie_jar}" -b "${cookie_jar}" \
        -X POST "${login_url}" \
        -d "username=${PROBE_USER}&password=${PROBE_PASSWORD}&redirect=/ui" || true
      local ui_code
      ui_code="$(curl -s -o /dev/null -w '%{http_code}' -b "${cookie_jar}" "${ui_url}" || true)"
      if [[ "${ui_code}" == "200" ]]; then
        rm -f "${cookie_jar}"
        log "Hop Web is ready (login + UI HTTP ${ui_code})"
        return 0
      fi
      if [[ "${i}" -eq 1 || $((i % 10)) -eq 0 ]]; then
        log "  login page up; UI not ready yet (HTTP ${ui_code:-none}, attempt ${i}/${attempts})"
      fi
    elif [[ "${i}" -eq 1 || $((i % 10)) -eq 0 ]]; then
      log "  still waiting... (login HTTP ${code:-none}, attempt ${i}/${attempts})"
    fi
    sleep 2
  done
  rm -f "${cookie_jar}"
  die "Hop Web did not become ready. Check: docker logs ${CONTAINER_NAME}"
}

ensure_dirs() {
  local hop_uid="${HOP_UID:-501}"
  local hop_gid="${HOP_GID:-501}"
  mkdir -p "${SCRIPT_DIR}/local-data/audit" "${SECURITY_DATA_DIR}"
  # Nested users/<name>/ dirs may be 0750 from Tomcat umask + host ownership; hop is UID 501.
  chown -R "${hop_uid}:${hop_gid}" "${SCRIPT_DIR}/local-data/audit" "${SECURITY_DATA_DIR}" 2>/dev/null || true
  chmod -R a+rwX "${SCRIPT_DIR}/local-data/audit" "${SECURITY_DATA_DIR}" 2>/dev/null || true
}

run_container() {
  require_command docker
  stop_container
  ensure_dirs

  local security_abs
  security_abs="$(cd "${SECURITY_DATA_DIR}" && pwd)"

  log "Starting ${CONTAINER_NAME} with Hop-managed BASIC auth"
  log "Security data (users.json): ${security_abs}"
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -p "${HOST_PORT}:8080" \
    -e HOP_LOG_LEVEL=Basic \
    -e HOP_GUI_ZOOM_FACTOR=1.0 \
    -e HOP_WEB_SECURITY_MODE=BASIC \
    -e HOP_WEB_SEED_DEMO_USERS=true \
    -e HOP_AUDIT_FOLDER=/tmp/hop-web-audit \
    -v "${SCRIPT_DIR}/local-data/audit:/tmp/hop-web-audit" \
    -v "${security_abs}:/usr/local/tomcat/webapps/ROOT/config/security" \
    "${IMAGE_NAME}" >/dev/null

  wait_for_hop_web

  echo ""
  echo "Hop Web is running with Hop-managed BASIC authentication:"
  echo "  Light UI:  http://localhost:${HOST_PORT}/ui"
  echo "  Dark UI:   http://localhost:${HOST_PORT}/ui-dark"
  echo ""
  echo "Demo users (password = username):"
  echo "  admin      / admin       → Admin"
  echo "  developer  / developer   → User"
  echo "  operator   / operator    → Operator (no save)"
  echo "  viewer     / viewer      → Read-only"
  echo ""
  echo "Users file:  ${security_abs}/users.json"
  echo "Container:   ${CONTAINER_NAME}"
  echo "Stop:        ${SCRIPT_DIR}/run-hop-web-local-with-basic.sh --stop"
  echo ""

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
  docker info >/dev/null 2>&1 || die "Docker daemon is not running"

  if [[ "${RUN_ONLY}" != "true" ]]; then
    [[ "${SKIP_MAVEN}" != "true" ]] && run_maven
    [[ "${SKIP_DOCKER_BUILD}" != "true" ]] && build_image
  else
    docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1 \
      || die "Image ${IMAGE_NAME} not found. Build first."
  fi

  if [[ "${BUILD_ONLY}" == "true" ]]; then
    log "Build complete"
    exit 0
  fi
  run_container
}

main "$@"

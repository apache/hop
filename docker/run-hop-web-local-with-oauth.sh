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
# Build and run Hop Web with OAuth2 / OpenID Connect (authorization code + PKCE).
# Requires an OIDC IdP (Keycloak, Entra ID, Okta, Google, …).
#
# Typical usage:
#   export HOP_WEB_OAUTH_ISSUER=https://keycloak.example/realms/hop
#   export HOP_WEB_OAUTH_CLIENT_ID=hop-web
#   export HOP_WEB_OAUTH_CLIENT_SECRET=...   # optional for public + PKCE clients
#   ./docker/run-hop-web-local-with-oauth.sh
#
#   ./docker/run-hop-web-local-with-oauth.sh --quick
#   ./docker/run-hop-web-local-with-oauth.sh --run-only
#   ./docker/run-hop-web-local-with-oauth.sh --stop
#
# Optional: source a dotenv file before running:
#   set -a && source docker/local-data/oauth.env && set +a
#   ./docker/run-hop-web-local-with-oauth.sh --run-only
#
# UI:     http://localhost:8080/ui   → redirects to /login → IdP
# Login:  http://localhost:8080/login
# Callback (register with IdP): http://localhost:8080/oauth/callback

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME="hop-web:local"
CONTAINER_NAME="hop-web-local-oauth"
HOST_PORT="8080"
HOP_UI_PATH="/ui"

# Persist security-config.json (role mappings, etc.) across restarts
SECURITY_DATA_DIR="${SCRIPT_DIR}/local-data/oauth-security"

# ---------------------------------------------------------------------------
# OAuth / OIDC environment (exported into the container)
#
# Required for a working SSO login:
#
#   HOP_WEB_OAUTH_ISSUER
#       OIDC issuer URL (no trailing slash required).
#       Examples:
#         https://keycloak.example/realms/hop
#         https://login.microsoftonline.com/<tenant-id>/v2.0
#         https://accounts.google.com
#
#   HOP_WEB_OAUTH_CLIENT_ID
#       Client ID registered at the IdP for this Hop Web app.
#
# Recommended / optional:
#
#   HOP_WEB_OAUTH_CLIENT_SECRET
#       Client secret for confidential clients. Prefer env over committing secrets
#       to security-config.json. Leave empty for public clients that use PKCE only.
#
#   HOP_WEB_OAUTH_REDIRECT_URI
#       Exact redirect URI registered with the IdP.
#       Default (auto): http://localhost:<port>/oauth/callback
#       Must match the IdP client configuration character-for-character.
#
#   HOP_WEB_OAUTH_SCOPES
#       Space-separated scopes. Default: openid profile email
#
#   HOP_WEB_OAUTH_ROLE_CLAIM / HOP_WEB_OAUTH_USERNAME_CLAIM
#       Optional. Only set these if you want env to override security-config.json.
#       Prefer the config file (or Configuration → Security). If exported here, they
#       overwrite the JSON claim fields at bootstrap.
#       Keycloak examples: realm_access.roles | groups
#       Google (personal): set in JSON — oauthRoleClaim "email", map "you@gmail.com" → admin
#
# Forced by this script:
#
#   HOP_WEB_SECURITY_MODE=OAUTH2
#
# Also useful after first login (in Configuration → Security → External):
#   Map IdP groups or email addresses to Hop roles, e.g. hop-admin → admin,
#   you@gmail.com → admin
# ---------------------------------------------------------------------------

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
  # Header comment block only (avoid printing set -euo pipefail)
  sed -n '18,39p' "$0" | sed 's/^# \{0,1\}//' | grep -v '^set '
  cat <<'EOF'

Options:
  --skip-maven       Skip the Maven build
  --quick            Faster Maven build (ui/rap + web assembly)
  --build-only       Build image only
  --run-only         Start without rebuilding
  --stop             Stop and remove the oauth container
  --port PORT        Host port mapped to container 8080 (default: 8080)
  --no-cache         Docker build --no-cache
  --logs             Follow container logs after start
  -h, --help         Show this help

Required environment (export before running, or source a .env file):
  HOP_WEB_OAUTH_ISSUER       OIDC issuer URL
  HOP_WEB_OAUTH_CLIENT_ID    OAuth2 / OIDC client id

Optional environment:
  HOP_WEB_OAUTH_CLIENT_SECRET      Client secret (confidential clients)
  HOP_WEB_OAUTH_REDIRECT_URI       Default: http://localhost:<port>/oauth/callback
  HOP_WEB_OAUTH_SCOPES             Default: openid profile email
  HOP_WEB_OAUTH_ROLE_CLAIM         Optional override of security-config.json (avoid for Google)
  HOP_WEB_OAUTH_USERNAME_CLAIM     Optional override of security-config.json

IdP checklist:
  1. Create a client (confidential or public+PKCE)
  2. Valid redirect URI:  http://localhost:<port>/oauth/callback
  3. Post-logout redirect (if supported): http://localhost:<port>/login
  4. Configure role/username claims in security-config.json (not via env unless intentional)
  5. Map IdP groups or email → Hop roles (External tab / roleMappings in JSON)
  6. Google: oauthRoleClaim "email" + roleMappings "you@gmail.com":"admin"; unset claim env vars

Example (Keycloak):
  export HOP_WEB_OAUTH_ISSUER=https://keycloak.example/realms/hop
  export HOP_WEB_OAUTH_CLIENT_ID=hop-web
  export HOP_WEB_OAUTH_CLIENT_SECRET=change-me
  # Prefer putting realm_access.roles in security-config.json; env is optional:
  # export HOP_WEB_OAUTH_ROLE_CLAIM=realm_access.roles
  ./docker/run-hop-web-local-with-oauth.sh --quick

Example (Google):
  export HOP_WEB_OAUTH_ISSUER=https://accounts.google.com
  export HOP_WEB_OAUTH_CLIENT_ID=….apps.googleusercontent.com
  export HOP_WEB_OAUTH_CLIENT_SECRET=…
  unset HOP_WEB_OAUTH_ROLE_CLAIM HOP_WEB_OAUTH_USERNAME_CLAIM
  # Edit docker/local-data/oauth-security/security-config.json:
  #   oauthRoleClaim/email username claim + roleMappings you@gmail.com → admin
  ./docker/run-hop-web-local-with-oauth.sh --quick
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
      --port) HOST_PORT="${2:?--port requires a value}"; shift 2 ;;
      --no-cache) DOCKER_NO_CACHE=true; shift ;;
      --logs) FOLLOW_LOGS=true; shift ;;
      -h | --help) usage; exit 0 ;;
      *) die "Unknown option: $1 (use --help)" ;;
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

# Defaults that depend on --port
apply_oauth_defaults() {
  # Auto redirect URI unless the user already set one
  if [[ -z "${HOP_WEB_OAUTH_REDIRECT_URI:-}" ]]; then
    HOP_WEB_OAUTH_REDIRECT_URI="http://localhost:${HOST_PORT}/oauth/callback"
  fi
  if [[ -z "${HOP_WEB_OAUTH_SCOPES:-}" ]]; then
    HOP_WEB_OAUTH_SCOPES="openid profile email"
  fi
  # Do NOT default HOP_WEB_OAUTH_ROLE_CLAIM / USERNAME_CLAIM here: injecting them into the
  # container overwrites security-config.json at bootstrap (env wins). Leave claims in the
  # config file (or export them only when you intentionally want env to override).
  # Google: use oauthRoleClaim "email" + roleMappings "you@gmail.com" → "admin".
  # Keycloak: oauthRoleClaim "realm_access.roles" (or groups) in the config file.
}

validate_oauth_env() {
  apply_oauth_defaults

  local missing=()
  [[ -n "${HOP_WEB_OAUTH_ISSUER:-}" ]] || missing+=("HOP_WEB_OAUTH_ISSUER")
  [[ -n "${HOP_WEB_OAUTH_CLIENT_ID:-}" ]] || missing+=("HOP_WEB_OAUTH_CLIENT_ID")

  if ((${#missing[@]} > 0)); then
    echo "Missing required OAuth environment variable(s): ${missing[*]}" >&2
    echo "" >&2
    echo "Export them first, for example:" >&2
    echo "  export HOP_WEB_OAUTH_ISSUER=https://keycloak.example/realms/hop" >&2
    echo "  export HOP_WEB_OAUTH_CLIENT_ID=hop-web" >&2
    echo "  export HOP_WEB_OAUTH_CLIENT_SECRET=...   # if confidential client" >&2
    echo "" >&2
    echo "Run with --help for the full list." >&2
    exit 1
  fi

  log "OAuth issuer:     ${HOP_WEB_OAUTH_ISSUER}"
  log "OAuth client id:  ${HOP_WEB_OAUTH_CLIENT_ID}"
  log "OAuth redirect:   ${HOP_WEB_OAUTH_REDIRECT_URI}"
  log "OAuth scopes:     ${HOP_WEB_OAUTH_SCOPES}"
  log "OAuth role claim: ${HOP_WEB_OAUTH_ROLE_CLAIM:-(from security-config.json)}"
  log "OAuth user claim: ${HOP_WEB_OAUTH_USERNAME_CLAIM:-(from security-config.json)}"
  if [[ -n "${HOP_WEB_OAUTH_CLIENT_SECRET:-}" ]]; then
    log "OAuth secret:     (set, ${#HOP_WEB_OAUTH_CLIENT_SECRET} chars)"
  else
    log "OAuth secret:     (empty — PKCE-only / public client)"
  fi
}

wait_for_hop_web() {
  # Full SSO cannot be automated without the IdP; wait until the login page is up.
  local login_url="http://localhost:${HOST_PORT}/login"
  local attempts=90
  log "Waiting for Hop Web OAuth login page at ${login_url}"
  for ((i = 1; i <= attempts; i++)); do
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' "${login_url}" || true)"
    if [[ "${code}" == "200" ]]; then
      log "Hop Web login page is ready (HTTP ${code})"
      return 0
    fi
    if [[ "${i}" -eq 1 || $((i % 10)) -eq 0 ]]; then
      log "  still waiting... (login HTTP ${code:-none}, attempt ${i}/${attempts})"
    fi
    sleep 2
  done
  die "Hop Web did not become ready. Check: docker logs ${CONTAINER_NAME}"
}

ensure_dirs() {
  local hop_uid="${HOP_UID:-501}"
  local hop_gid="${HOP_GID:-501}"
  mkdir -p "${SCRIPT_DIR}/local-data/audit" "${SECURITY_DATA_DIR}"
  chown "${hop_uid}:${hop_gid}" "${SCRIPT_DIR}/local-data/audit" "${SECURITY_DATA_DIR}" 2>/dev/null \
    || chmod 777 "${SCRIPT_DIR}/local-data/audit" "${SECURITY_DATA_DIR}" 2>/dev/null \
    || true
}

run_container() {
  require_command docker
  validate_oauth_env
  stop_container
  ensure_dirs

  local security_abs
  security_abs="$(cd "${SECURITY_DATA_DIR}" && pwd)"

  # Docker -e list: only pass optional vars when set so empty secret is intentional.
  # Do not pass empty ROLE_CLAIM / USERNAME_CLAIM — that would override security-config.json.
  local docker_env=(
    -e HOP_LOG_LEVEL=Basic
    -e HOP_GUI_ZOOM_FACTOR=1.0
    -e HOP_WEB_SECURITY_MODE=OAUTH2
    -e "HOP_WEB_OAUTH_ISSUER=${HOP_WEB_OAUTH_ISSUER}"
    -e "HOP_WEB_OAUTH_CLIENT_ID=${HOP_WEB_OAUTH_CLIENT_ID}"
    -e "HOP_WEB_OAUTH_REDIRECT_URI=${HOP_WEB_OAUTH_REDIRECT_URI}"
    -e "HOP_WEB_OAUTH_SCOPES=${HOP_WEB_OAUTH_SCOPES}"
  )
  if [[ -n "${HOP_WEB_OAUTH_CLIENT_SECRET:-}" ]]; then
    docker_env+=(-e "HOP_WEB_OAUTH_CLIENT_SECRET=${HOP_WEB_OAUTH_CLIENT_SECRET}")
  fi
  if [[ -n "${HOP_WEB_OAUTH_ROLE_CLAIM:-}" ]]; then
    docker_env+=(-e "HOP_WEB_OAUTH_ROLE_CLAIM=${HOP_WEB_OAUTH_ROLE_CLAIM}")
  fi
  if [[ -n "${HOP_WEB_OAUTH_USERNAME_CLAIM:-}" ]]; then
    docker_env+=(-e "HOP_WEB_OAUTH_USERNAME_CLAIM=${HOP_WEB_OAUTH_USERNAME_CLAIM}")
  fi

  log "Starting ${CONTAINER_NAME} with OAuth2 / OIDC"
  log "Security data dir: ${security_abs}"
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -p "${HOST_PORT}:8080" \
    "${docker_env[@]}" \
    -e HOP_AUDIT_FOLDER=/tmp/hop-web-audit \
    -v "${SCRIPT_DIR}/local-data/audit:/tmp/hop-web-audit" \
    -v "${security_abs}:/usr/local/tomcat/webapps/ROOT/config/security" \
    "${IMAGE_NAME}" >/dev/null

  wait_for_hop_web

  echo ""
  echo "Hop Web is running with OAuth2 / OpenID Connect:"
  echo "  Light UI:   http://localhost:${HOST_PORT}/ui"
  echo "  Dark UI:    http://localhost:${HOST_PORT}/ui-dark"
  echo "  Login page: http://localhost:${HOST_PORT}/login"
  echo ""
  echo "Register this redirect URI with your IdP:"
  echo "  ${HOP_WEB_OAUTH_REDIRECT_URI}"
  echo "Post-logout redirect (if supported):"
  echo "  http://localhost:${HOST_PORT}/login"
  echo ""
  echo "Issuer:       ${HOP_WEB_OAUTH_ISSUER}"
  echo "Client ID:    ${HOP_WEB_OAUTH_CLIENT_ID}"
  echo "Role claim:   ${HOP_WEB_OAUTH_ROLE_CLAIM:-(from security-config.json)}"
  echo "Security dir: ${security_abs}"
  echo "Audit data:   ${SCRIPT_DIR}/local-data/audit  →  /tmp/hop-web-audit"
  echo "Container:    ${CONTAINER_NAME}"
  echo "Stop:         ${SCRIPT_DIR}/run-hop-web-local-with-oauth.sh --stop"
  echo ""
  echo "Map IdP groups or email → Hop roles in security-config.json (External tab)"
  echo "or Configuration → Security after admin login. Unset HOP_WEB_OAUTH_*_CLAIM"
  echo "in your shell unless you want env to override the config file."
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

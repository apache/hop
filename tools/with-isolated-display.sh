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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Run a command (typically ./mvnw) against a virtual X display so SWT UI tests
# do not steal the interactive session. Prefers host xvfb-run (what Jenkins
# uses). Falls back to the docker/ui-tests Xvfb sidecar when xvfb is not
# installed. Never points Maven at the current seat unless
# HOP_ALLOW_INTERACTIVE_DISPLAY=1 is set as a last resort.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT}/docker/ui-tests/compose.yaml"
SCREEN_ARGS="${HOP_XVFB_SCREEN:-1280x1024x24}"

usage() {
  cat <<'EOF'
Usage: tools/with-isolated-display.sh COMMAND [ARGS...]

Run COMMAND with SWT/GTK talking to a virtual framebuffer instead of the
interactive X/Wayland session. Example:

  tools/with-isolated-display.sh ./mvnw clean install

Host xvfb-run is used when installed (same as Jenkins). Otherwise a tiny
Xvfb Docker sidecar is started and DISPLAY is pointed at its unix socket.

Environment:
  HOP_ALLOW_INTERACTIVE_DISPLAY=1  run on the current DISPLAY if no Xvfb
                                   or Docker sidecar can be started
  HOP_XVFB_SCREEN=1280x1024x24     Xvfb screen geometry
EOF
}

die() {
  echo "with-isolated-display: $*" >&2
  exit 1
}

warn() {
  echo "with-isolated-display: $*" >&2
}

if [[ $# -eq 0 ]]; then
  usage
  exit 2
fi

# Force GTK/SWT onto X11 so a Wayland session does not keep sending windows
# to the seat we are trying to leave alone.
isolate_env() {
  export GDK_BACKEND=x11
  unset WAYLAND_DISPLAY || true
}

run_on_interactive_display() {
  warn "running on the interactive display; SWT shells will steal focus"
  exec "$@"
}

if command -v xvfb-run >/dev/null 2>&1; then
  isolate_env
  exec env -u WAYLAND_DISPLAY GDK_BACKEND=x11 \
    xvfb-run -a --server-args="-screen 0 ${SCREEN_ARGS}" "$@"
fi

# xvfb-run is a Linux/X11 tool. A Cocoa or Win32 SWT JVM cannot use this
# sidecar; say so and run the command as-is rather than pretending.
os="$(uname -s || echo unknown)"
if [[ "${os}" != "Linux" ]]; then
  warn "isolated X11 display is Linux-only (${os}); SWT will use the interactive session"
  exec "$@"
fi

have_docker=0
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  have_docker=1
fi

if [[ "${have_docker}" -ne 1 ]]; then
  if [[ "${HOP_ALLOW_INTERACTIVE_DISPLAY:-}" == "1" ]]; then
    run_on_interactive_display "$@"
  fi
  die "neither xvfb-run nor Docker is available. Install xvfb (preferred) or Docker, or set HOP_ALLOW_INTERACTIVE_DISPLAY=1"
fi

display_busy() {
  local n="$1"
  [[ -e "/tmp/.X11-unix/X${n}" || -e "/tmp/.X${n}-lock" ]]
}

pick_display() {
  local n
  for n in $(seq 99 199); do
    if ! display_busy "${n}"; then
      echo "${n}"
      return 0
    fi
  done
  return 1
}

DISPLAY_NUM="$(pick_display)" || die "no free X display in :99-:199"

SIDECAR_COMPOSE=0
SIDECAR_CID=""
COMPOSE_PROJECT="hop-xvfb-$$-${DISPLAY_NUM}"

compose_bin() {
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    echo "docker-compose"
    return 0
  fi
  return 1
}

stop_sidecar() {
  if [[ "${SIDECAR_COMPOSE}" -eq 1 ]]; then
    # shellcheck disable=SC2086
    DISPLAY_NUM="${DISPLAY_NUM}" ${COMPOSE} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" \
      down --remove-orphans >/dev/null 2>&1 || true
  elif [[ -n "${SIDECAR_CID}" ]]; then
    docker stop "${SIDECAR_CID}" >/dev/null 2>&1 || true
  fi
}

start_sidecar() {
  local compose_cmd
  if compose_cmd="$(compose_bin)"; then
    COMPOSE="${compose_cmd}"
    # shellcheck disable=SC2086
    DISPLAY_NUM="${DISPLAY_NUM}" ${COMPOSE} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" \
      up -d --build
    SIDECAR_COMPOSE=1
    return 0
  fi

  docker build -t hop-ui-test-xvfb:local "${ROOT}/docker/ui-tests"
  SIDECAR_CID="$(
    docker run -d --rm --network none --init \
      -e "DISPLAY_NUM=${DISPLAY_NUM}" \
      -v /tmp/.X11-unix:/tmp/.X11-unix \
      --name "${COMPOSE_PROJECT}" \
      hop-ui-test-xvfb:local
  )"
}

sidecar_logs() {
  if [[ "${SIDECAR_COMPOSE}" -eq 1 ]]; then
    # shellcheck disable=SC2086
    DISPLAY_NUM="${DISPLAY_NUM}" ${COMPOSE} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" \
      logs >&2 || true
  elif [[ -n "${SIDECAR_CID}" ]]; then
    docker logs "${SIDECAR_CID}" >&2 || true
  fi
}

wait_for_display() {
  local n="$1"
  local i
  for i in $(seq 1 100); do
    if [[ -S "/tmp/.X11-unix/X${n}" ]]; then
      return 0
    fi
    sleep 0.1
  done
  sidecar_logs
  return 1
}

trap stop_sidecar EXIT INT TERM

warn "xvfb-run not found; starting Docker Xvfb sidecar on :${DISPLAY_NUM}"
start_sidecar
wait_for_display "${DISPLAY_NUM}" || die "Xvfb sidecar did not create /tmp/.X11-unix/X${DISPLAY_NUM}"

isolate_env
export DISPLAY=":${DISPLAY_NUM}"
"$@"

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
# Check every plugin of a local build on its own: a client without plugins plus that
# one plugin, plus the plugins its dependencies.xml requires, and nothing else.
#
# Two baselines, both built by the command below:
#
#   bare      hop-core, hop-engine, hop-ui and hop-ui-rcp without SWT: a headless install
#             that runs pipelines and workflows (#9501). The default. References from a
#             plugin's dialogs into SWT are reported, not failed: there is nothing to link
#             them to.
#   bare-gui  bare plus SWT (--gui). Proves the dialogs link too.
#
# The integration tests and tools/check-plugin-classpath.sh in its default mode run a
# plugin next to all the others. A plugin whose classes only resolve because a
# different plugin put a jar in lib/core passes there, and fails in an install that
# only has the plugins a project uses. This finds those.
#
# Build first, from the repository root:
#
#   ./mvnw clean install -DskipTests
#   ./mvnw -pl assemblies/client package -Dhop.bare.skip=false
#
# Usage:
#   tools/check-plugins-isolated.sh [--gui] [--bare <dir>] [check-plugin-classpath.sh options]
#
#   --gui          Use the baseline with SWT (bare-gui) instead of the headless one.
#   --bare <dir>   The client without plugins to use. Default: the one the second
#                  command above leaves in assemblies/client/target.
#
# Every other option goes to tools/check-plugin-classpath.sh, for example
# --runtime, --plugin <artifactId>, --junit <file>, --report <file>, --self-test.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BARE=""
TIER=bare
PASS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
  --bare)
    shift
    BARE="${1:-}"
    [[ -n "${BARE}" ]] || { echo "ERROR: --bare needs a value" >&2; exit 2; }
    ;;
  --gui) TIER=bare-gui ;;
  -h | --help) sed -n '17,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
  *) PASS+=("$1") ;;
  esac
  shift
done

if [[ -z "${BARE}" ]]; then
  BARE="$(find "${ROOT}/assemblies/client/target" -maxdepth 2 -type d -path "*-${TIER}/hop" 2>/dev/null | sort | tail -1)"
  [[ -n "${BARE}" ]] || {
    echo "ERROR: no client without plugins in assemblies/client/target." >&2
    echo "Build it with: ./mvnw -pl assemblies/client package -Dhop.bare.skip=false" >&2
    exit 2
  }
fi
[[ -d "${BARE}/lib" ]] || { echo "ERROR: ${BARE} is not a Hop install (no lib/)" >&2; exit 2; }

# The version is in the directory's name: hop-client-<version>-bare[-gui]/hop. Only
# zips of that version are checked; plugin target/ folders keep zips from older builds.
VERSION="$(basename "$(dirname "${BARE}")")"
VERSION="${VERSION#hop-client-}"
VERSION="${VERSION%-gui}"
VERSION="${VERSION%-bare}"

LIST="$(mktemp "${TMPDIR:-/tmp}/plugins-isolated.XXXXXX")"
trap 'rm -f "${LIST}"' EXIT

find "${ROOT}/plugins" -path "*/target/*-${VERSION}.zip" -not -path '*/target/*/*' 2>/dev/null |
  sort | while IFS= read -r zip; do
  name="$(basename "${zip}")"
  printf '%s %s\n' "${name%-"${VERSION}".zip}" "${zip}"
done >"${LIST}"

[[ -s "${LIST}" ]] || { echo "ERROR: no plugin zips of version ${VERSION} under plugins/*/target" >&2; exit 2; }

"${ROOT}/tools/check-plugin-classpath.sh" \
  --client "${BARE}" \
  --plugins "${LIST}" \
  --label "isolated-${TIER}" \
  --isolated \
  ${PASS[@]+"${PASS[@]}"}

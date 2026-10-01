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
# Pair each marketplace plugin with the integration-test project that covers it, for
# tools/check-plugin-classpath.sh --it-suites.
#
# Most marketplace plugins cannot be smoke-tested from a GitHub runner: they need a
# database, a cloud account or a cluster. The integration tests cover those, so the
# report links to the matching suite instead of leaving the row looking untested.
#
# Both sides of the pairing are already in the repository, so this is derived rather
# than maintained by hand: the plugin list and its modulePath come from
# optional-plugins.yaml (via list-marketplace-plugins.sh) and the projects are the
# directories under integration-tests/. A plugin is paired when the last segment of
# its modulePath matches a project name. Only the handful of cases where the two
# trees picked different names need an entry in marketplace-it-aliases.txt, so a new
# plugin that follows the convention is picked up with no edit here.
#
# The project directory name is also the suite name Jenkins publishes, which is what
# makes the report link work.
#
# Usage:
#   tools/marketplace-it-suites.sh            # "<artifactId> <suite>" per line
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIST="${ROOT}/tools/list-marketplace-plugins.sh"
ALIASES="${ROOT}/tools/marketplace-it-aliases.txt"
IT_DIR="${ROOT}/integration-tests"

[[ -x "${LIST}" ]] || chmod +x "${LIST}" 2>/dev/null || true

alias_for() {
  [[ -f "${ALIASES}" ]] || return 1
  awk -v n="$1" '/^[[:space:]]*#/ { next } NF < 2 { next } $1 == n { print $2; found = 1; exit }
                 END { exit found ? 0 : 1 }' "${ALIASES}"
}

while IFS='|' read -r artifact module; do
  [[ -n "${artifact}" && -n "${module}" ]] || continue
  name="${module##*/}"
  suite="$(alias_for "${name}" || true)"
  [[ -n "${suite}" ]] || suite="${name}"
  [[ -d "${IT_DIR}/${suite}" ]] || continue
  printf '%s %s\n' "${artifact}" "${suite}"
done < <("${LIST}")

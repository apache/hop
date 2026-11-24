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
# Fail when a marketplace plugin zip cannot work once it is installed.
#
# Two failure modes, both silent in every other part of the build:
#
#   1. The zip ships no plugin jar at all. Its dependencies are there, its
#      version.xml is there, and installing it adds nothing that Hop can load.
#      An assembly <include> that matches no artifact produces exactly this and
#      maven-assembly-plugin does not warn.
#
#   2. The plugin's own classes reference a package that is on no classpath once
#      the plugin is installed, because a dependency was mis-scoped or dropped by
#      a wildcard <exclusion> and so landed neither in the plugin's lib/ nor in the
#      client's lib/core. The plugin then dies with NoClassDefFoundError on first
#      use, in the user's install, not in CI.
#
# Each plugin is checked against a baseline client that represents what its users
# actually have. That baseline matters: with every plugin present, one plugin's
# lib/core contribution covers another's gap, which is how these bugs survive.
# For a marketplace plugin the baseline is the plain client. For a plugin that
# ships inside a client, install its companions with --install first.
#
# jdeps is run over the plugin's own jars only, never over the third-party jars it
# bundles: those are full of optional dependencies that are on no classpath by
# design, and several are modular, which makes jdeps abort with a module resolution
# error rather than report. Restricting the input to first-party classes keeps the
# check silent when healthy and loud when a plugin is genuinely broken.
#
# Usage:
#   tools/check-plugin-classpath.sh --client <zip|dir> --plugins <list> [options]
#
#   --client <zip|dir>   Baseline client to install onto. Required.
#   --plugins <file>     Plugin list, one per line: "<artifactId> <path-to-zip>".
#                        A '|' separator is accepted too. '#' comments and blank
#                        lines are ignored. Required.
#   --install <zip>      Install this zip into the baseline before checking
#                        anything. Repeatable. Use for the plugins that ship
#                        inside the client the marketplace plugins install onto.
#   --allowlist <file>   Default: tools/plugin-classpath-allowlist.txt next to this
#                        script.
#   --label <name>       Name for the run in output and JUnit. Default "plugins".
#   --junit <file>       Also write a JUnit report, one testcase per plugin.
#   --report <file>      Also write a Markdown status table, one row per plugin.
#   --it-suites <file>   "<artifactId> <suite>" pairs naming the integration-test
#                        project that covers a plugin needing a live service. Those
#                        plugins report as needs-service rather than as a gap.
#   --it-base <url>      Base URL for the test report, to turn the suite name into a
#                        link. Without it the suite is named but not linked.
#   --plugin <id>        Check only this artifactId.
#   --allow-missing      Do not fail on plugins whose zip was not built.
#   --isolated           Check each plugin on its own. The baseline must be a client
#                        without plugins (see tools/check-plugins-isolated.sh); only
#                        its lib/ is on the classpath. The plugin gets the folders its
#                        dependencies.xml names, installed from the other zips in the
#                        plugin list, and nothing else. A required folder that no zip
#                        provides is a failure. A plugin the pom of the plugin depends on
#                        (an org.apache.hop dependency that is a zip in the list, outside
#                        test scope) is installed too: that is how a plugin that ships in
#                        lib/core, such as static-schema, is required.
#   --runtime            With --isolated: also start Hop on each isolated install and
#                        prove the plugin registers and every class in its own jars
#                        links (PluginIsolationCheck in hop-engine). Slower than jdeps,
#                        and finds what jdeps cannot: a missing class in a package that
#                        exists, a plugin that registers nothing.
#   --self-test          Prove the check can still fail, then exit.
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLIENT=""
PLUGIN_LIST=""
ALLOWLIST="${HERE}/plugin-classpath-allowlist.txt"
LABEL="plugins"
JUNIT=""
REPORT=""
IT_SUITES=""
IT_BASE=""
ONLY_PLUGIN=""
ALLOW_MISSING=false
ISOLATED=false
RUNTIME=false
SELF_TEST=false
INSTALL_ZIPS=()

die() {
  echo "ERROR: $*" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --client) shift; CLIENT="${1:-}"; [[ -n "${CLIENT}" ]] || die "--client needs a value" ;;
  --plugins) shift; PLUGIN_LIST="${1:-}"; [[ -n "${PLUGIN_LIST}" ]] || die "--plugins needs a value" ;;
  --install) shift; [[ -n "${1:-}" ]] || die "--install needs a value"; INSTALL_ZIPS+=("$1") ;;
  --allowlist) shift; ALLOWLIST="${1:-}"; [[ -n "${ALLOWLIST}" ]] || die "--allowlist needs a value" ;;
  --label) shift; LABEL="${1:-}"; [[ -n "${LABEL}" ]] || die "--label needs a value" ;;
  --junit) shift; JUNIT="${1:-}"; [[ -n "${JUNIT}" ]] || die "--junit needs a value" ;;
  --report) shift; REPORT="${1:-}"; [[ -n "${REPORT}" ]] || die "--report needs a value" ;;
  --it-suites) shift; IT_SUITES="${1:-}"; [[ -n "${IT_SUITES}" ]] || die "--it-suites needs a value" ;;
  --it-base) shift; IT_BASE="${1:-}"; [[ -n "${IT_BASE}" ]] || die "--it-base needs a value" ;;
  --plugin) shift; ONLY_PLUGIN="${1:-}"; [[ -n "${ONLY_PLUGIN}" ]] || die "--plugin needs an artifactId" ;;
  --allow-missing) ALLOW_MISSING=true ;;
  --isolated) ISOLATED=true ;;
  --runtime) RUNTIME=true ;;
  --self-test) SELF_TEST=true ;;
  -h | --help) sed -n '17,76p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
  shift
done

[[ -n "${CLIENT}" ]] || die "--client is required"
[[ -n "${PLUGIN_LIST}" ]] || die "--plugins is required"
[[ -f "${PLUGIN_LIST}" ]] || die "plugin list not found: ${PLUGIN_LIST}"
command -v jdeps >/dev/null 2>&1 || die "jdeps not on PATH — this needs a JDK, not a JRE"
[[ "${RUNTIME}" == false || "${ISOLATED}" == true ]] || die "--runtime needs --isolated"
command -v unzip >/dev/null 2>&1 || die "unzip not on PATH"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/plugin-classpath.XXXXXX")"
# Physical path without a doubled slash: the Java check prints normalised paths.
WORK="$(cd "${WORK}" && pwd -P)"
trap 'rm -rf "${WORK}"' EXIT

# ---------------------------------------------------------------- baseline

BASE="${WORK}/baseline"
if [[ -d "${CLIENT}" ]]; then
  mkdir -p "${BASE}"
  # Copied rather than used in place: --install writes into the baseline, and a
  # check must never mutate the caller's client.
  cp -R "${CLIENT}/." "${BASE}/"
elif [[ -f "${CLIENT}" ]]; then
  echo "==> Unpacking $(basename "${CLIENT}")"
  unzip -q -o "${CLIENT}" -d "${BASE}"
else
  die "--client is neither a file nor a directory: ${CLIENT}"
fi

# Client zips unpack to a single top directory; extracted container roots do not.
if [[ ! -d "${BASE}/lib" ]]; then
  inner="$(find "${BASE}" -maxdepth 2 -type d -name lib -print -quit 2>/dev/null || true)"
  [[ -n "${inner}" ]] || die "no lib/ directory found under ${CLIENT}"
  BASE="$(dirname "${inner}")"
fi

for zip in ${INSTALL_ZIPS[@]+"${INSTALL_ZIPS[@]}"}; do
  [[ -f "${zip}" ]] || die "--install zip not found: ${zip}"
  echo "    installing into baseline: $(basename "${zip}")"
  unzip -q -o "${zip}" -d "${BASE}"
done

# In isolated mode another plugin's jars must never be on the classpath, so a baseline
# that has plugins is refused rather than half used: its lib/core already carries what
# those plugins put there.
BASE_DIRS=("${BASE}/lib" "${BASE}/plugins")
if [[ "${ISOLATED}" == true ]]; then
  if [[ -n "$(find "${BASE}/plugins" -name '*.jar' 2>/dev/null | head -1)" ]]; then
    die "--isolated needs a client without plugins, but ${CLIENT} has a plugins/ tree with jars"
  fi
  BASE_DIRS=("${BASE}/lib")
fi

# A headless baseline (no SWT, #9501) cannot link a plugin's dialogs. References into SWT,
# and the hop-ui classes that need it, are then counted and reported, not failed; the
# baseline with SWT checks them.
HEADLESS=false
if [[ "${ISOLATED}" == true && -z "$(find "${BASE}/lib/swt" -name '*.jar' 2>/dev/null | head -1)" ]]; then
  HEADLESS=true
fi
GUI_CLASSES='^((org\.apache\.hop\.ui|org\.eclipse\.(swt|jface|rap|e4|core\.commands|core\.runtime|equinox|osgi|text|tm4e|ui))\.|org\.apache\.hop\.core\.(ProgressMonitorAdapter|Swt[A-Za-z]*)(\$.*)?$)'

BASE_CP=""
while IFS= read -r jar; do
  BASE_CP="${BASE_CP}${jar}:"
done < <(find "${BASE_DIRS[@]}" -name '*.jar' 2>/dev/null | sort)
[[ -n "${BASE_CP}" ]] || die "no jars found in the baseline client at ${BASE}"

# ---------------------------------------------------------------- helpers

# Jars that are the plugin's own code: directly under plugins/<category>/<name>/,
# as opposed to its bundled dependencies, which the assembly puts in that
# directory's lib/.
own_jars() { find "$1/plugins" -name '*.jar' 2>/dev/null | grep -v '/lib/' | sort || true; }

# What a zip is, before deciding whether it is broken:
#   plugin  — ships first-party jars, check them
#   nojar   — ships dependencies under plugins/**/lib but no plugin jar. Broken:
#             installing it adds libraries and nothing that Hop can load.
#   library — ships no plugins/ tree at all, only lib/ jars. A shared library
#             published as a zip, with nothing of its own to check.
#   empty   — no jars anywhere
# Plugins that ship their own jar in lib/core instead of a plugin folder (misc/naming,
# for one) register as native plugins from the annotation index in that jar. In
# isolated mode those hop-* jars with an index are the plugin's own jars.
native_jars() {
  [[ "${ISOLATED}" == true ]] || return 0
  local jar
  while IFS= read -r jar; do
    unzip -Z1 "${jar}" META-INF/jandex.idx >/dev/null 2>&1 && printf '%s\n' "${jar}"
  done < <(find "$1/lib/core" -maxdepth 1 -name 'hop-*.jar' 2>/dev/null | sort)
  return 0
}

classify() {
  local dir="$1"
  if [[ -n "$(own_jars "${dir}")" ]]; then echo plugin; return; fi
  if [[ -n "$(native_jars "${dir}")" ]]; then echo native; return; fi
  if [[ -n "$(find "${dir}/plugins" -name '*.jar' 2>/dev/null | head -1)" ]]; then echo nojar; return; fi
  if [[ -n "$(find "${dir}" -name '*.jar' 2>/dev/null | head -1)" ]]; then echo library; return; fi
  echo empty
}

# $2, when set, is a jar basename to withhold from the classpath (--self-test).
plugin_cp() {
  local dir="$1" withhold="${2:-}" cp="" jar
  while IFS= read -r jar; do
    [[ -n "${withhold}" && "$(basename "${jar}")" == "${withhold}" ]] && continue
    cp="${cp}${jar}:"
  done < <(find "${dir}" -name '*.jar' 2>/dev/null | sort)
  if [[ -z "${withhold}" ]]; then
    cp="${cp}${BASE_CP}"
  else
    while IFS= read -r jar; do
      [[ "$(basename "${jar}")" == "${withhold}" ]] && continue
      cp="${cp}${jar}:"
    done < <(find "${BASE_DIRS[@]}" -name '*.jar' 2>/dev/null | sort)
  fi
  printf '%s\n' "${cp}"
}

# $3, when set, is the list of jars to analyse. In isolated mode those are the jars of
# the plugin under test only: the plugins it requires are on the classpath, not checked.
run_jdeps() {
  local dir="$1" withhold="${2:-}" jars="${3:-}" cp
  cp="$(plugin_cp "${dir}" "${withhold}")"
  [[ -n "${jars}" ]] || jars="$(own_jars "${dir}")"
  [[ -n "${jars}" ]] || return 3
  # shellcheck disable=SC2086
  # Isolated mode works per class: hop-ui puts classes in hop-core packages, so only the
  # class tells a dialog's reference into hop-ui from a real gap.
  local verbose=()
  [[ "${ISOLATED}" == true ]] && verbose=(-verbose:class)
  jdeps -q --multi-release 21 ${verbose[@]+"${verbose[@]}"} -cp "${cp}" ${jars} 2>&1 || true
}

# "sourcePackage missingPackage" per unresolved reference. jdeps also emits a bare
# "<jar> -> not found" summary; only the indented package lines name the source
# package, which is what makes a finding actionable.
parse_missing() { awk '/ not found$/ && /^[[:space:]]/ { print $1, $3 }' | sort -u; }

# Collapses "a/b/../c/./d" to "a/c/d". Pure bash: macOS realpath has no -m.
normalize_path() {
  local part out=()
  local IFS=/
  for part in $1; do
    case "${part}" in
    '' | .) ;;
    ..) [[ ${#out[@]} -gt 0 ]] && unset 'out[${#out[@]}-1]' ;;
    *) out+=("${part}") ;;
    esac
  done
  printf '%s\n' "${out[*]}"
}

# The plugin folders a staged install asks for through dependencies.xml, relative to
# the install root: one "plugins/<category>/<name>" per line. Both <folder> under
# <dependencies> and the folder of a <libs> entry count. A folder outside plugins/,
# such as ../../../lib/core, is part of the baseline and skipped.
required_folders() {
  local dir="$1" xml rel from folder
  while IFS= read -r xml; do
    from="${xml#"${dir}"/}"
    from="$(dirname "${from}")"
    while IFS= read -r folder; do
      [[ -n "${folder}" ]] || continue
      rel="$(normalize_path "${from}/${folder}")"
      case "${rel}" in
      plugins/*/*) printf '%s\n' "$(printf '%s' "${rel}" | cut -d/ -f1-3)" ;;
      esac
    done < <(tr -d '\r' <"${xml}" | sed -n 's:.*<folder>[[:space:]]*\([^<]*[^<[:space:]]\)[[:space:]]*</folder>.*:\1:p')
  done < <(find "${dir}/plugins" -name dependencies.xml 2>/dev/null | sort)
}

# The org.apache.hop artifactIds the pom of the module a zip was built in depends on,
# outside test scope. Empty when the zip was not built in this repository.
pom_plugin_deps() {
  local pom
  pom="$(printf '%s' "$1" | sed -n 's|^\(.*\)/target/[^/]*\.zip$|\1/pom.xml|p')"
  [[ -n "${pom}" && -f "${pom}" ]] || return 0
  python3 -I - "${pom}" <<'PY'
import sys
import xml.etree.ElementTree as ET
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
root = ET.parse(sys.argv[1]).getroot()
for dep in root.findall("m:dependencies/m:dependency", ns):
    if dep.findtext("m:groupId", "", ns).strip() != "org.apache.hop":
        continue
    if dep.findtext("m:scope", "", ns).strip() == "test":
        continue
    print(dep.findtext("m:artifactId", "", ns).strip())
PY
}

# Installs what required_folders asks for from the zips in FOLDER_INDEX, and the plugins
# the poms of the installed zips depend on from ARTIFACT_INDEX, until nothing new is
# asked for. Prints "missing <folder>" for a folder no zip provides and
# "libcore <folder>" for one whose plugin ships in lib/core: installed, but the
# dependencies.xml line itself does nothing in a real install.
install_requirements() {
  local dir="$1" pass folder line zip where changed done_list="" installed=" $2 " art required
  for pass in 1 2 3 4 5 6 7 8; do
    changed=false
    for zip in ${installed}; do
      while IFS= read -r art; do
        [[ -n "${art}" ]] || continue
        required="$(awk -v a="${art}" '$1 == a { print $2; exit }' "${ARTIFACT_INDEX}")"
        [[ -n "${required}" ]] || continue
        case "${installed}" in *" ${required} "*) continue ;; esac
        unzip -q -o "${required}" -d "${dir}"
        installed="${installed}${required} "
        changed=true
      done < <(pom_plugin_deps "${zip}")
    done
    while IFS= read -r folder; do
      [[ -d "${dir}/${folder}" ]] && continue
      case " ${done_list} " in *" ${folder} "*) continue ;; esac
      line="$(awk -v f="${folder}" '$1 == f { print $2, $3; exit }' "${FOLDER_INDEX}")"
      zip="${line%% *}"
      where="${line#* }"
      if [[ -z "${zip}" ]]; then
        printf 'missing %s\n' "${folder}"
      else
        case "${installed}" in *" ${zip} "*) ;; *) unzip -q -o "${zip}" -d "${dir}" ;; esac
        installed="${installed}${zip} "
        [[ "${where}" == lib/core ]] && printf 'libcore %s\n' "${folder}"
        changed=true
      fi
      done_list="${done_list} ${folder}"
    done < <(required_folders "${dir}" | sort -u)
    [[ "${changed}" == true ]] || return 0
  done
}

# The SWT jar of this machine, so GUI classes link. Loading SWT classes needs no display.
host_swt() {
  local arch
  arch="$(uname -m)"
  case "$(uname -s)" in
  Darwin) [[ "${arch}" == arm64 ]] && echo osx/arm64 || echo osx/x86_64 ;;
  Linux) [[ "${arch}" == aarch64 || "${arch}" == arm64 ]] && echo linux/arm64 || echo linux/x86_64 ;;
  *) echo win64 ;;
  esac
}

# Starts Hop on the staged install with the given own jars as targets and prints the
# ISOLATION result lines. Returns the check's exit code: 0 ok, 1 failures, 2 no start.
# One target per line for PluginIsolationCheck: the folder of a plugin folder's jars,
# a lib/core plugin jar as itself. A folder with two jars is one target.
runtime_targets() {
  local dir="$1" jar
  while IFS= read -r jar; do
    [[ -n "${jar}" ]] || continue
    if [[ "${jar}" == "${dir}"/plugins/* ]]; then dirname "${jar}"; else printf '%s\n' "${jar}"; fi
  done <<<"$2"
}

run_runtime() {
  local dir="$1" jars="$2" target args=() rc=0
  while IFS= read -r target; do
    [[ -n "${target}" ]] || continue
    if [[ "${target}" == "${dir}"/plugins/* ]]; then
      args+=(--folder "${target}")
    else
      args+=(--jar "${target}")
    fi
  done < <(runtime_targets "${dir}" "${jars}" | sort -u)
  [[ ${#args[@]} -gt 0 ]] || return 2

  local cp="${BASE}/lib/core/*:${dir}/lib/core/*:${BASE}/lib/swt/$(host_swt)/*"
  local log="${dir}/.isolation.log"
  mkdir -p "${dir}/.hop-config" "${dir}/.hop-audit"
  (
    cd "${dir}" &&
      java -Xmx1g -Djava.awt.headless=true \
        -DHOP_PLUGIN_BASE_FOLDERS="${dir}/plugins" \
        -DHOP_SHARED_JDBC_FOLDERS="${dir}/lib/jdbc,${BASE}/lib/jdbc" \
        -DHOP_CONFIG_FOLDER="${dir}/.hop-config" \
        -DHOP_AUDIT_FOLDER="${dir}/.hop-audit" \
        -DHOP_AUTO_CREATE_CONFIG=Y -DHOP_PLATFORM_RUNTIME=Run \
        --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED \
        --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED \
        -cp "${cp}" org.apache.hop.core.plugins.isolation.PluginIsolationCheck "${args[@]}"
  ) >"${log}" 2>&1 || rc=$?
  grep '^ISOLATION ' "${log}" | sed "s|${dir}/||g" || true
  return "${rc}"
}

allowed() {
  local id="$1" pkg="$2"
  [[ -f "${ALLOWLIST}" ]] || return 1
  awk -v id="${id}" -v pkg="${pkg}" '
    /^[[:space:]]*#/ { next }
    NF < 2 { next }
    $1 == id && (pkg == $2 || index(pkg, $2 ".") == 1) { found = 1; exit }
    END { exit found ? 0 : 1 }
  ' "${ALLOWLIST}"
}

# The integration-test project covering this plugin, if it needs a live service.
it_suite() {
  [[ -n "${IT_SUITES}" && -f "${IT_SUITES}" ]] || return 1
  awk -v id="$1" '/^[[:space:]]*#/ { next } NF < 2 { next } $1 == id { print $2; found = 1; exit }
                  END { exit found ? 0 : 1 }' "${IT_SUITES}"
}

xml_escape() { sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'; }

# ---------------------------------------------------------------- plugin list

IDS=()
ZIPS=()
ALL_IDS=()
ALL_ZIPS=()
while IFS= read -r line || [[ -n "${line}" ]]; do
  line="${line%%#*}"
  line="$(printf '%s' "${line}" | tr '|' ' ')"
  # shellcheck disable=SC2086
  set -- ${line}
  [[ $# -ge 2 ]] || continue
  ALL_IDS+=("$1")
  ALL_ZIPS+=("$2")
  [[ -z "${ONLY_PLUGIN}" || "$1" == "${ONLY_PLUGIN}" ]] || continue
  IDS+=("$1")
  ZIPS+=("$2")
done <"${PLUGIN_LIST}"

# "plugins/<category>/<name> <zip>" for every plugin folder that ships a jar of its
# own, over the whole list (not just --plugin), so a required folder can be installed.
FOLDER_INDEX="${WORK}/folders.txt"
: >"${FOLDER_INDEX}"
# "<artifactId> <zip>" for every zip in the list, to install what a pom requires.
ARTIFACT_INDEX="${WORK}/artifacts.txt"
: >"${ARTIFACT_INDEX}"
if [[ "${ISOLATED}" == true ]]; then
  i=0
  while [[ ${i} -lt ${#ALL_ZIPS[@]} ]]; do
    printf '%s %s\n' "${ALL_IDS[$i]}" "${ALL_ZIPS[$i]}" >>"${ARTIFACT_INDEX}"
    i=$((i + 1))
  done

  for zip in ${ALL_ZIPS[@]+"${ALL_ZIPS[@]}"}; do
    [[ -f "${zip}" ]] || continue
    unzip -Z1 "${zip}" 2>/dev/null | grep -E '^plugins/[^/]+/[^/]+/' | grep '\.jar$' | grep -v '/lib/' |
      cut -d/ -f1-3 | sort -u | sed "s|\$| ${zip}|" >>"${FOLDER_INDEX}" || true
  done
  # A plugin that ships its jar in lib/core has no folder in an install, but a dependencies.xml
  # can still name the folder of its source module. Those resolve through the module the zip
  # was built in (plugins/<category>/<name>/target/<zip>), marked "lib/core".
  for zip in ${ALL_ZIPS[@]+"${ALL_ZIPS[@]}"}; do
    [[ -f "${zip}" ]] || continue
    module="$(printf '%s' "${zip}" | sed -n 's|.*/\(plugins/[^/]*/[^/]*\)/target/[^/]*\.zip$|\1|p')"
    [[ -n "${module}" ]] || continue
    grep -q "^${module} " "${FOLDER_INDEX}" || printf '%s %s lib/core\n' "${module}" "${zip}" >>"${FOLDER_INDEX}"
  done
fi

[[ ${#IDS[@]} -gt 0 ]] || die "no plugins to check from ${PLUGIN_LIST}"

# ---------------------------------------------------------------- self test

# Picks a plugin whose own classes resolve against a jar the plugin itself ships,
# withholds that jar, and expects the missing package to be reported. Derived from
# the build rather than hard-coded, so it keeps working as plugins come and go.
if [[ "${SELF_TEST}" == true ]]; then
  echo "==> Self test (${LABEL})"
  i=0
  while [[ ${i} -lt ${#IDS[@]} ]]; do
    id="${IDS[$i]}"; zip="${ZIPS[$i]}"; i=$((i + 1))
    [[ -f "${zip}" ]] || continue
    dir="${WORK}/st"; rm -rf "${dir}"; mkdir -p "${dir}"
    unzip -q -o "${zip}" -d "${dir}"
    [[ "$(classify "${dir}")" == plugin ]] || continue
    out="$(run_jdeps "${dir}")" || continue
    echo "${out}" | grep -qE '^(Exception in thread|Error: )' && continue
    [[ -n "$(echo "${out}" | parse_missing)" ]] && continue

    # Dependency jars only. The plugin's own jars are jdeps *input*, so withholding
    # one from the classpath changes nothing and would fail the self test spuriously.
    own_jars "${dir}" | xargs -n1 basename 2>/dev/null | sort -u >"${WORK}/own.txt"
    find "${dir}" -name '*.jar' -exec basename {} \; | sort -u |
      grep -Fxv -f "${WORK}/own.txt" >"${WORK}/shipped.txt" || true
    # jdeps names JDK modules in the same column ("java.base"), so keep real jars only.
    canary="$(echo "${out}" |
      awk 'NF == 4 && $2 == "->" && $4 ~ /\.jar$/ { print $4 }' |
      sort -u | grep -Fx -f "${WORK}/shipped.txt" | head -1 || true)"
    [[ -n "${canary}" ]] || continue

    echo "    canary plugin: ${id}"
    echo "    withholding:   ${canary}"
    findings="$(run_jdeps "${dir}" "${canary}" | parse_missing || true)"
    if [[ -z "${findings}" ]]; then
      echo
      echo "SELF TEST FAILED: withholding ${canary} produced no finding."
      exit 1
    fi
    echo "    reported:"
    echo "${findings}" | sed 's/^/      /'
    echo
    echo "Self test passed."
    exit 0
  done
  die "no plugin with a usable canary jar; build the plugin zips first"
fi

# ---------------------------------------------------------------- main

echo "==> Checking ${#IDS[@]} ${LABEL} plugin(s)"
echo "    baseline: ${CLIENT}"
echo

ok=0; allowedcount=0; failed=0; missing=0; errored=0; libs=0
CASES="${WORK}/cases"
: >"${CASES}"

# One line per plugin. Detail can be multi-line, which would otherwise turn every
# continuation line into its own testcase, so newlines are folded onto a record
# separator here and unfolded when the JUnit report is written.
# Fields are separated by a unit separator, not a tab: tab is an IFS whitespace
# character, so `read` collapses runs of it and an empty detail field would silently
# shift every column after it. Embedded newlines fold onto a record separator so one
# plugin stays one line.
record() {
  printf '%s\037%s\037%s\037%s\n' \
    "$1" "$2" "$(printf '%s' "$3" | tr '\n' '\036')" "${4:-0}" >>"${CASES}"
}

# Sample pipelines and workflows the zip ships. Plugins put these under
# config/projects/samples via src/main/samples, and the client pre-registers that
# project, so they are what a per-plugin smoke test would run.
sample_count() {
  find "$1" \( -name '*.hpl' -o -name '*.hwf' \) 2>/dev/null | wc -l | tr -d ' '
}

i=0
while [[ ${i} -lt ${#IDS[@]} ]]; do
  id="${IDS[$i]}"; zip="${ZIPS[$i]}"; i=$((i + 1))

  if [[ ! -f "${zip}" ]]; then
    echo "  MISSING   ${id} — ${zip}"
    missing=$((missing + 1))
    record "${id}" skipped "zip not built: ${zip}"
    continue
  fi

  dir="${WORK}/stage"; rm -rf "${dir}"; mkdir -p "${dir}"
  unzip -q -o "${zip}" -d "${dir}"
  samples="$(sample_count "${dir}")"

  case "$(classify "${dir}")" in
  library)
    echo "  LIBRARY   ${id} (no plugin tree, nothing to check)"
    libs=$((libs + 1))
    record "${id}" skipped "shared library zip" "${samples}"
    continue
    ;;
  nojar)
    echo "  FAIL      ${id}"
    echo "              the zip ships dependencies but no plugin jar"
    echo "              installing it adds nothing Hop can load"
    echo "              check the assembly <include> matches the module's groupId:artifactId"
    failed=$((failed + 1))
    record "${id}" failure "zip ships dependencies under plugins/**/lib but no plugin jar" "${samples}"
    continue
    ;;
  empty)
    echo "  FAIL      ${id} — the zip contains no jars at all"
    failed=$((failed + 1))
    record "${id}" failure "zip contains no jars" "${samples}"
    continue
    ;;
  esac

  own="$(own_jars "${dir}")"
  [[ -n "${own}" ]] || own="$(native_jars "${dir}")"
  unmet=""
  libcore_reqs=""
  if [[ "${ISOLATED}" == true ]]; then
    reqs="$(install_requirements "${dir}" "${zip}")"
    unmet="$(printf '%s\n' "${reqs}" | sed -n 's/^missing //p')"
    libcore_reqs="$(printf '%s\n' "${reqs}" | sed -n 's/^libcore //p')"
  fi
  out="$(run_jdeps "${dir}" "" "${own}")"

  # jdeps refusing to run must never read as a pass: that is how a plugin quietly
  # stops being checked. Some errors are an exception, others a bare "Error: ..." line
  # (a multi-release jar with conflicting versions of a class) and no output at all.
  if echo "${out}" | grep -qE '^(Exception in thread|Error: )'; then
    detail="$(echo "${out}" | grep -E '^(Exception in thread|Error: )' | head -1)"
    echo "  ERROR     ${id} — jdeps could not analyse the plugin:"
    echo "              ${detail}"
    errored=$((errored + 1))
    record "${id}" error "${detail}" "${samples}"
    continue
  fi

  rt=""
  rt_fail=""
  runs_gui=""
  rt_warn=""
  if [[ "${RUNTIME}" == true ]]; then
    rt_rc=0
    rt="$(run_runtime "${dir}" "${own}")" || rt_rc=$?
    if [[ ${rt_rc} -ge 2 ]]; then
      detail="$(printf '%s\n' "${rt}" | sed -n 's/^ISOLATION ERROR //p' | head -1)"
      [[ -n "${detail}" ]] || detail="exit code ${rt_rc}"
      echo "  ERROR     ${id} — Hop did not start on the isolated install:"
      echo "              ${detail}"
      # The staged install is removed after each plugin: show where it broke now.
      grep -E '^(Caused by|[[:space:]]+at org\.apache\.hop)' "${dir}/.isolation.log" 2>/dev/null |
        head -12 | sed 's/^[[:space:]]*/                /' || true
      errored=$((errored + 1))
      record "${id}" error "${detail}" "${samples}"
      continue
    fi
    rt_fail="$(printf '%s\n' "${rt}" | sed -n 's/^ISOLATION FAIL //p')"
    rt_warn="$(printf '%s\n' "${rt}" | sed -n 's/^ISOLATION WARN //p')"
  fi

  hits="$(echo "${out}" | parse_missing)"
  gui_note=""
  if [[ "${ISOLATED}" == true && -n "${hits}" ]]; then
    # Class level ("sourceClass missingClass"): filter the GUI references on a headless
    # baseline, then report by package like the default mode does.
    if [[ "${HEADLESS}" == true ]]; then
      # With --runtime the check knows which classes need SWT to load at all (dialogs and the
      # like). A reference into SWT from any other class is code that loads fine and breaks if
      # it runs without SWT. Most such code only ever runs from the GUI (painters, GUI-only
      # extension points), which jdeps cannot tell apart, so these are reported as risks, not
      # failures.
      if [[ "${RUNTIME}" == true ]]; then
        printf '%s\n' "${rt}" | sed -n 's/^ISOLATION GUICLASS //p' | sed 's/\$.*//' | sort -u >"${WORK}/guiclasses.txt"
        runs_gui="$(printf '%s\n' "${hits}" | GUI_CLASSES="${GUI_CLASSES}" awk '$2 ~ ENVIRON["GUI_CLASSES"]' |
          while read -r src target; do
            outer="${src%%\$*}"
            grep -qxF "${outer}" "${WORK}/guiclasses.txt" || printf '%s %s\n' "${src}" "${target}"
          done | sort -u)"
      fi
      gui_count="$(printf '%s\n' "${hits}" | awk '{ print $2 }' | sort -u | grep -cE "${GUI_CLASSES}" || true)"
      # Through the environment: awk -v would process the backslashes in the pattern.
      hits="$(printf '%s\n' "${hits}" | GUI_CLASSES="${GUI_CLASSES}" awk '$2 !~ ENVIRON["GUI_CLASSES"]')"
      [[ "${gui_count}" -eq 0 ]] || gui_note="${gui_count} SWT class(es) referenced (checked with --gui)"
    fi
    hits="$(printf '%s\n' "${hits}" | awk 'NF == 2 { sub(/\.[^.]*$/, "", $1); sub(/\.[^.]*$/, "", $2); print $1, $2 }' | sort -u)"
  fi
  if [[ "${RUNTIME}" == true ]]; then
    rt_gui="$(printf '%s\n' "${rt}" | sed -n 's/^ISOLATION GUI //p')"
    [[ -z "${rt_gui}" ]] || gui_note="${gui_note:+${gui_note}$'\n'}${rt_gui}"
  fi
  dep_note=""
  while IFS= read -r folder; do
    [[ -n "${folder}" ]] || continue
    dep_note="${dep_note}dependencies.xml names ${folder}, but that plugin ships in lib/core: the line does nothing, the plugin still has to be installed"$'\n'
  done <<<"${libcore_reqs}"
  if [[ -n "${runs_gui}" ]]; then
    risk_count="$(printf '%s\n' "${runs_gui}" | awk '{ print $1 }' | sed 's/\$.*//' | sort -u | wc -l | tr -d ' ')"
    gui_note="${gui_note:+${gui_note}$'\n'}headless risk: ${risk_count} class(es) load without SWT but call it:"$'\n'"$(printf '%s\n' "${runs_gui}" | awk '{ print "  " $1 " -> " $2 }')"
  fi
  if [[ -z "${hits}" && -z "${unmet}" && -z "${rt_fail}" ]]; then
    echo "  OK        ${id}"
    [[ -z "${dep_note}" ]] || printf '%s' "${dep_note}" | sed 's/^/              note: /'
    [[ -z "${rt_warn}" ]] || printf '%s\n' "${rt_warn}" | sed 's/^/              warn: /'
    [[ -z "${gui_note}" ]] || printf '%s\n' "${gui_note}" | sed 's/^/              gui:  /'
    ok=$((ok + 1))
    record "${id}" pass "" "${samples}"
    continue
  fi

  plugin_failed=false
  shown=""
  detail=""
  # A folder named in dependencies.xml that no zip provides: Hop skips it without a
  # word, so the plugin runs without the classes it asked for. Allowlist entries use
  # "dependencies.xml:<folder>" as the package.
  while read -r folder; do
    [[ -n "${folder}" ]] || continue
    if allowed "${id}" "dependencies.xml:${folder}"; then
      shown="${shown}              allowed: dependencies.xml:${folder}"$'\n'
    else
      shown="${shown}              dependencies.xml requires ${folder}, which no plugin zip provides"$'\n'
      detail="${detail}dependencies.xml requires ${folder}, which no plugin zip provides"$'\n'
      plugin_failed=true
    fi
  done <<<"${unmet}"
  # Runtime findings. A missing class is allowlisted like a package: by its name.
  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    cls="$(printf '%s' "${line}" | sed -n 's/.*: missing \([^ ,]*\).*/\1/p')"
    if [[ -n "${cls}" ]] && allowed "${id}" "${cls}"; then
      shown="${shown}              allowed: ${cls}"$'\n'
    else
      shown="${shown}              runtime: ${line}"$'\n'
      detail="${detail}runtime: ${line}"$'\n'
      plugin_failed=true
    fi
  done <<<"${rt_fail}"
  [[ -z "${rt_warn}" ]] || shown="${shown}$(printf '%s\n' "${rt_warn}" | sed 's/^/              warn: /')"$'\n'
  [[ -z "${gui_note}" ]] || shown="${shown}$(printf '%s\n' "${gui_note}" | sed 's/^/              gui:  /')"$'\n'
  [[ -z "${dep_note}" ]] || shown="${shown}$(printf '%s' "${dep_note}" | sed 's/^/              note: /')"$'\n'
  while read -r src pkg; do
    [[ -n "${src}" ]] || continue
    if allowed "${id}" "${pkg}"; then
      shown="${shown}              allowed: ${pkg}"$'\n'
    else
      shown="${shown}              ${src} -> ${pkg}"$'\n'
      detail="${detail}${src} -> ${pkg}"$'\n'
      plugin_failed=true
    fi
  done <<<"${hits}"

  if [[ "${plugin_failed}" == true ]]; then
    echo "  FAIL      ${id}"
    printf '%s' "${shown}"
    failed=$((failed + 1))
    record "${id}" failure "${detail}" "${samples}"
  else
    echo "  ALLOWED   ${id}"
    printf '%s' "${shown}"
    allowedcount=$((allowedcount + 1))
    record "${id}" pass "" "${samples}"
  fi
done

echo
echo "Summary (${LABEL}): ok=${ok} allowed=${allowedcount} library=${libs} failed=${failed} errored=${errored} missing=${missing}"

# ---------------------------------------------------------------- junit

if [[ -n "${JUNIT}" ]]; then
  mkdir -p "$(dirname "${JUNIT}")"
  total=$(wc -l <"${CASES}" | tr -d ' ')
  {
    echo '<?xml version="1.0" encoding="UTF-8"?>'
    printf '<testsuite name="plugin-classpath-%s" tests="%s" failures="%s" errors="%s" skipped="%s">\n' \
      "${LABEL}" "${total}" "${failed}" "${errored}" "$((missing + libs))"
    while IFS=$'\037' read -r id status detail _samples; do
      printf '  <testcase classname="plugin-classpath.%s" name="%s">' "${LABEL}" "${id}"
      case "${status}" in
      pass) ;;
      failure) printf '<failure message="unresolved classpath references"><![CDATA[%s]]></failure>' "$(printf '%s' "${detail}" | tr '\036' '\n')" ;;
      error) printf '<error message="jdeps failed"><![CDATA[%s]]></error>' "$(printf '%s' "${detail}" | tr '\036' '\n')" ;;
      skipped) printf '<skipped message="%s"/>' "$(printf '%s' "${detail}" | tr '\036' ' ' | xml_escape)" ;;
      esac
      printf '</testcase>\n'
    done <"${CASES}"
    echo '</testsuite>'
  } >"${JUNIT}"
  echo "JUnit report: ${JUNIT}"
fi

# ---------------------------------------------------------------- report

if [[ -n "${REPORT}" ]]; then
  mkdir -p "$(dirname "${REPORT}")"
  {
    printf '### Marketplace plugins — %s\n\n' "${LABEL}"
    printf '| Plugin | Classpath | Samples |\n|---|---|---|\n'
    while IFS=$'\037' read -r id status detail samples; do
      case "${status}" in
      pass) cp_cell="pass" ;;
      failure) cp_cell="**fail**" ;;
      error) cp_cell="**error**" ;;
      skipped) cp_cell="not checked — $(printf '%s' "${detail}" | tr '\036' ' ')" ;;
      *) cp_cell="${status}" ;;
      esac

      # A plugin an integration test already covers points at that test rather than
      # reading as untested — most of them need a live service and cannot run here at
      # all. It is evidence, not a pass: those tests run against a full distribution,
      # where a plugin can borrow another plugin's jars — the very thing the classpath
      # column exists to rule out.
      if suite="$(it_suite "${id}")"; then
        if [[ -n "${IT_BASE}" ]]; then
          samples_cell="covered by IT — [\`${suite}\` ↗](${IT_BASE%/}/${suite}/)"
        else
          samples_cell="covered by IT — ${suite}"
        fi
      elif [[ "${samples:-0}" -gt 0 ]]; then
        samples_cell="not run (${samples})"
      else
        samples_cell="no sample"
      fi
      printf '| %s | %s | %s |\n' "${id}" "${cp_cell}" "${samples_cell}"
    done <"${CASES}"

    printf '\nClasspath: ok=%s allowed=%s library=%s failed=%s errored=%s missing=%s\n' \
      "${ok}" "${allowedcount}" "${libs}" "${failed}" "${errored}" "${missing}"
    printf '\n%s\n' '- **no sample** — ships no pipeline under `config/projects/samples`, so nothing can smoke-test it.'
    printf '%s\n' '- **not run** — ships samples; running them per plugin is not wired up yet.'
    printf '%s\n' '- **covered by IT** — not smoke-tested here (these mostly need a live service), but exercised by the nightly integration tests. Supporting evidence only: those run against a full distribution, where a plugin can borrow another plugin'"'"'s jars.'
  } >"${REPORT}"
  echo "Report: ${REPORT}"
fi

status=0
if [[ ${failed} -gt 0 || ${errored} -gt 0 ]]; then
  status=1
fi
if [[ ${missing} -gt 0 ]]; then
  if [[ "${ALLOW_MISSING}" == true ]]; then
    echo "Ignoring ${missing} plugin zip(s) that were not built (--allow-missing)."
  else
    echo "${missing} plugin zip(s) were not built, so they were not checked."
    status=1
  fi
fi

[[ ${status} -eq 0 ]] && echo "Plugin classpath check passed (${LABEL})."
exit ${status}

#!/usr/bin/env sh

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

# Applies the plugin file changes the Hop marketplace could not make while Hop was running
# (plugins/.marketplace/pending). The Hop launchers run this before they start Java: once Java runs,
# the jars are loaded and, on Windows, locked.
#
# Usage, from the Hop installation folder:
#   sh lib/apply-pending-changes.sh [HOP_PLUGIN_BASE_FOLDERS]
#
# The list is applied in this folder and in the parent of every plugins folder named in
# HOP_PLUGIN_BASE_FOLDERS, the places the marketplace installs into. Each line is one change:
#   copy|<source>|<target>
#   delete|<target>
# Paths are relative to that folder. A change that fails stays in the list for the next start.
# Messages go to stderr, so the output of the Hop command itself is unchanged.

hop_pending_log() {
  echo "Hop marketplace: $*" >&2
}

# Relative, no "..", no backslash or drive letter.
hop_pending_safe_path() {
  case "$1" in
  '' | /* | */ | ./* | */. | *\\* | *:* | *..* | *\** | *\?*) return 1 ;;
  esac
  case "/$1/" in
  *//* | */./*) return 1 ;;
  esac
  return 0
}

hop_pending_valid_target() {
  hop_pending_safe_path "$1" || return 1
  case "$1" in
  plugins/.marketplace/*) return 1 ;;
  plugins/* | lib/* | webapps/ROOT/WEB-INF/lib/*) return 0 ;;
  esac
  return 1
}

hop_pending_valid_source() {
  hop_pending_safe_path "$1" || return 1
  case "$1" in
  plugins/.marketplace/pending-files/*/*) return 1 ;;
  plugins/.marketplace/pending-files/*.pending) return 0 ;;
  esac
  return 1
}

# Applies one line. Returns 0 when the line is done with (applied, or invalid and dropped) and 1
# when it has to be tried again at the next start.
hop_pending_apply_line() {
  _hp_home="$1"
  _hp_line="$2"
  _hp_action="${_hp_line%%|*}"
  _hp_rest="${_hp_line#*|}"
  if [ "${_hp_rest}" = "${_hp_line}" ]; then
    hop_pending_log "ignoring invalid pending change: ${_hp_line}"
    return 0
  fi
  case "${_hp_action}" in
  delete)
    if ! hop_pending_valid_target "${_hp_rest}"; then
      hop_pending_log "ignoring invalid pending change: ${_hp_line}"
      return 0
    fi
    _hp_target="${_hp_home}/${_hp_rest}"
    if [ -d "${_hp_target}" ]; then
      hop_pending_log "ignoring invalid pending change: ${_hp_line}"
      return 0
    fi
    rm -f "${_hp_target}" 2>/dev/null
    if [ -e "${_hp_target}" ] || [ -L "${_hp_target}" ]; then
      hop_pending_log "could not remove ${_hp_rest}, trying again at the next start"
      return 1
    fi
    return 0
    ;;
  copy)
    _hp_source_rel="${_hp_rest%%|*}"
    _hp_target_rel="${_hp_rest#*|}"
    if [ "${_hp_target_rel}" = "${_hp_rest}" ] ||
      ! hop_pending_valid_source "${_hp_source_rel}" ||
      ! hop_pending_valid_target "${_hp_target_rel}"; then
      hop_pending_log "ignoring invalid pending change: ${_hp_line}"
      return 0
    fi
    _hp_source="${_hp_home}/${_hp_source_rel}"
    _hp_target="${_hp_home}/${_hp_target_rel}"
    if [ -d "${_hp_target}" ]; then
      hop_pending_log "ignoring invalid pending change: ${_hp_line}"
      return 0
    fi
    if [ ! -f "${_hp_source}" ]; then
      hop_pending_log "ignoring a pending change to ${_hp_target_rel}: ${_hp_source_rel} is gone"
      return 0
    fi
    # Copy next to the target and rename, so the target is never half written.
    _hp_temporary="${_hp_target}.hop-pending-$$"
    if mkdir -p "$(dirname "${_hp_target}")" 2>/dev/null &&
      cp "${_hp_source}" "${_hp_temporary}" 2>/dev/null &&
      mv -f "${_hp_temporary}" "${_hp_target}" 2>/dev/null; then
      rm -f "${_hp_source}" 2>/dev/null
      return 0
    fi
    rm -f "${_hp_temporary}" 2>/dev/null
    hop_pending_log "could not write ${_hp_target_rel}, trying again at the next start"
    return 1
    ;;
  esac
  hop_pending_log "ignoring invalid pending change: ${_hp_line}"
  return 0
}

hop_pending_apply_home() {
  _hp_list="$1/plugins/.marketplace/pending"
  [ -f "${_hp_list}" ] || return 0
  _hp_done="${_hp_list}.done-$$"
  # A read-only installation: nothing can change here.
  : >"${_hp_done}" 2>/dev/null || return 0
  _hp_applied=0
  _hp_failed=0
  _hp_cr="$(printf '\r')"
  while IFS= read -r _hp_entry || [ -n "${_hp_entry}" ]; do
    case "${_hp_entry}" in
    '' | '#'* | "${_hp_cr}") continue ;;
    esac
    # A list edited on Windows: the carriage return is not part of the path.
    if hop_pending_apply_line "$1" "${_hp_entry%"${_hp_cr}"}"; then
      printf '%s\n' "${_hp_entry}" >>"${_hp_done}"
      _hp_applied=$((_hp_applied + 1))
    else
      _hp_failed=$((_hp_failed + 1))
    fi
  done <"${_hp_list}"

  if [ -s "${_hp_done}" ]; then
    # Remove only the lines handled here: a running Hop may have added lines in the meantime.
    _hp_remaining="${_hp_list}.remaining-$$"
    grep -v -x -F -f "${_hp_done}" "${_hp_list}" >"${_hp_remaining}"
    if grep -q -v -e '^#' -e '^[[:space:]]*$' "${_hp_remaining}"; then
      mv -f "${_hp_remaining}" "${_hp_list}"
    else
      rm -f "${_hp_remaining}" "${_hp_list}"
    fi
  fi
  rm -f "${_hp_done}"

  if [ "${_hp_failed}" -gt 0 ]; then
    hop_pending_log "applied ${_hp_applied} pending plugin changes in $1, ${_hp_failed} still waiting. Is Hop still running from this folder?"
  elif [ "${_hp_applied}" -gt 0 ]; then
    hop_pending_log "applied ${_hp_applied} pending plugin changes in $1"
  fi
}

hop_pending_main() {
  # The folder list below is split on newlines only: no wildcard expansion.
  set -f
  _hp_homes="."
  _hp_folders="$1"
  while [ -n "${_hp_folders}" ]; do
    _hp_folder="${_hp_folders%%,*}"
    case "${_hp_folders}" in
    *,*) _hp_folders="${_hp_folders#*,}" ;;
    *) _hp_folders="" ;;
    esac
    # Trim spaces, as Hop does.
    _hp_folder="${_hp_folder#"${_hp_folder%%[![:space:]]*}"}"
    _hp_folder="${_hp_folder%"${_hp_folder##*[![:space:]]}"}"
    [ -z "${_hp_folder}" ] && continue
    if [ "$(basename "${_hp_folder}")" = "plugins" ]; then
      _hp_folder="$(dirname "${_hp_folder}")"
    fi
    _hp_homes="${_hp_homes}
${_hp_folder}"
  done

  _hp_seen=""
  _hp_old_ifs="${IFS}"
  IFS='
'
  for _hp_home in ${_hp_homes}; do
    IFS="${_hp_old_ifs}"
    _hp_real="$(cd "${_hp_home}" 2>/dev/null && pwd -P)" || continue
    case "
${_hp_seen}
" in
    *"
${_hp_real}
"*) continue ;;
    esac
    _hp_seen="${_hp_seen}
${_hp_real}"
    hop_pending_apply_home "${_hp_real}"
  done
  IFS="${_hp_old_ifs}"
}

hop_pending_main "${1:-}"
exit 0

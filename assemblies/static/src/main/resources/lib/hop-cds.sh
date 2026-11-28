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

# Class data sharing (AppCDS) for the Hop launchers, sourced by hop-*.sh from the
# installation folder.
#
# The JVM keeps the classes a tool loaded, already parsed and verified, in an archive,
# and maps that archive at the next start instead of loading those classes again.
# A tool then starts about 0.3 to 0.6 seconds faster.
#
# The archives are in the cds folder of the audit folder (HOP_AUDIT_FOLDER, ./audit by
# default), one per tool and class path: hop-<tool>-<checksum>.jsa. The checksum is of the
# java command and the class path, so setups with other plugin folders (whose lib/core
# goes on the class path) or another JDK each keep their own archive.
#
# The JVM decides whether an archive can be used. It uses an archive only when the JDK
# and the jars of the class path (lib/core, the Spark client, SWT, the lib/core beside an
# extra plugins folder) are the same as when it was written; otherwise the tool starts
# normally. Plugins are loaded by Hop's own class loaders: for those classes the JVM only
# uses an archived class when the class file is unchanged, so a changed, added or removed
# plugin is always loaded from its jar.
#
# The JVM writes a new archive by itself only when the old one was written by another
# JDK, not when a jar of the class path changed. So with HOP_CDS=auto an archive older
# than a class path folder or one of its jars is removed first, and the tool writes a
# new one: after an upgrade, or jars added to or removed from lib/core.
#
# HOP_CDS controls it:
#   auto (default) The first run of a tool writes its archive when it exits, a few
#                  seconds longer than a normal run. It is written again after the JDK or
#                  the jars of the class path changed.
#   use            Only use an archive that exists, never write one. For installations
#                  that are prepared once, such as Docker images: see
#                  lib/generate-cds-archives.sh.
#   off            No class data sharing.
#
# HOP_CDS_FOLDER puts the archives in another folder than <audit folder>/cds.
#
# The JVM prints nothing about the archive: its own messages are switched off.

# Prints the JVM options for class data sharing of one tool, or nothing.
#   $1  the tool: conf, run, gui, server, search, import, export, encrypt, command (hop)
# Uses CLASSPATH and _HOP_JAVA of the launcher, so call it after both are set.
hop_cds_options() {
  _hop_cds_mode="${HOP_CDS:-auto}"
  case "${_hop_cds_mode}" in
  off | OFF | n | N | no | false) return 0 ;;
  esac

  _hop_cds_folder="${HOP_CDS_FOLDER:-${HOP_AUDIT_FOLDER:-./audit}/cds}"
  # The launchers pass HOP_OPTIONS unquoted: a path with spaces can't be passed
  case "${_hop_cds_folder}" in
  *[[:space:]]*) return 0 ;;
  esac

  _hop_cds_key=$(printf '%s\n%s' "${_HOP_JAVA}" "${CLASSPATH}" | cksum | cut -d' ' -f1)
  _hop_cds_archive="${_hop_cds_folder}/hop-$1-${_hop_cds_key}.jsa"
  case "${_hop_cds_mode}" in
  use | USE)
    [ -f "${_hop_cds_archive}" ] || return 0
    printf '%s' "-XX:SharedArchiveFile=${_hop_cds_archive} -Xlog:cds=off -Xlog:cds+dynamic=off"
    ;;
  *)
    mkdir -p "${_hop_cds_folder}" 2>/dev/null || return 0
    [ -w "${_hop_cds_folder}" ] || return 0
    if [ -f "${_hop_cds_archive}" ] && hop_cds_class_path_changed "${_hop_cds_archive}"; then
      rm -f "${_hop_cds_archive}"
    fi
    printf '%s' "-XX:SharedArchiveFile=${_hop_cds_archive} -XX:+AutoCreateSharedArchive -Xlog:cds=off -Xlog:cds+dynamic=off"
    ;;
  esac
}

# Whether a folder or jar of the class path (CLASSPATH) is newer than the given file: a
# jar was replaced, added or removed after the archive was written.
hop_cds_class_path_changed() {
  _hop_cds_rest="${CLASSPATH}"
  while [ -n "${_hop_cds_rest}" ]; do
    _hop_cds_entry="${_hop_cds_rest%%:*}"
    case "${_hop_cds_rest}" in
    *:*) _hop_cds_rest="${_hop_cds_rest#*:}" ;;
    *) _hop_cds_rest="" ;;
    esac
    # lib/core/* means the folder and its jars
    _hop_cds_entry="${_hop_cds_entry%/\*}"
    [ -e "${_hop_cds_entry}" ] || continue
    if [ -n "$(find "${_hop_cds_entry}" -maxdepth 1 -newer "$1" 2>/dev/null | head -n 1)" ]; then
      return 0
    fi
  done
  return 1
}

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
# installation folder, the java binary (its path, size and date) and the class path. So
# installations that share an audit folder, setups with other plugin folders (whose
# lib/core goes on the class path) and another or updated JDK each keep their own archive.
#
# The JVM decides whether an archive can be used. It uses an archive only when the JDK
# and the jars of the class path (lib/core, the Spark client, SWT, the lib/core beside an
# extra plugins folder) are the same as when it was written; otherwise the tool starts
# normally. Plugins are loaded by Hop's own class loaders: for those classes the JVM only
# uses an archived class when the class file is unchanged, so a changed, added or removed
# plugin is always loaded from its jar.
#
# With HOP_CDS=auto a tool without an archive writes one when it exits. Each JVM writes a
# file of its own, <archive>.<host>.<pid>: the JVM writes an archive by removing the file
# and creating it again read-only, so two JVMs that write the same file at the same moment
# make one of them exit with 1 after a successful run. The next start of the tool moves a
# finished file into place. The JVM writes the first bytes of the file, its header, last,
# so the file of a JVM that was stopped while writing is never used: it is removed once
# that JVM is gone.
#
# The JVM never replaces an archive that no longer fits. Another or updated JDK gets
# another archive name. An archive older than a class path folder or one of its jars is
# removed first, and the tool writes a new one: after an upgrade, or jars added to or
# removed from lib/core.
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

  # The class path is relative to the installation folder, the launchers run from there
  _hop_cds_key=$( (
    pwd -P
    LC_ALL=C ls -lL "$(command -v "${_HOP_JAVA}")"
    printf '%s' "${CLASSPATH}"
  ) 2>/dev/null | cksum | cut -d' ' -f1)
  _hop_cds_archive="${_hop_cds_folder}/hop-$1-${_hop_cds_key}.jsa"
  case "${_hop_cds_mode}" in
  use | USE)
    [ -f "${_hop_cds_archive}" ] || return 0
    printf '%s' "-XX:SharedArchiveFile=${_hop_cds_archive} -Xlog:cds=off -Xlog:cds+dynamic=off"
    ;;
  *)
    mkdir -p "${_hop_cds_folder}" 2>/dev/null || return 0
    [ -w "${_hop_cds_folder}" ] || return 0
    if [ -f "${_hop_cds_archive}" ] && {
      ! hop_cds_complete "${_hop_cds_archive}" || hop_cds_class_path_changed "${_hop_cds_archive}"
    }; then
      rm -f "${_hop_cds_archive}"
    fi
    hop_cds_promote "${_hop_cds_archive}"
    if [ -f "${_hop_cds_archive}" ]; then
      printf '%s' "-XX:SharedArchiveFile=${_hop_cds_archive} -Xlog:cds=off -Xlog:cds+dynamic=off"
    else
      printf '%s' "-XX:ArchiveClassesAtExit=${_hop_cds_archive}.$(uname -n).$$ -Xlog:cds=off -Xlog:cds+dynamic=off"
    fi
    ;;
  esac
}

# Moves a finished archive of an earlier run into place, and removes the files of JVMs
# that stopped before they finished writing.
#   $1  the archive
# The launchers exec the JVM, so the process id in a file name is the writing JVM's.
hop_cds_promote() {
  for _hop_cds_tmp in "$1".*; do
    [ -f "${_hop_cds_tmp}" ] || continue
    if hop_cds_complete "${_hop_cds_tmp}"; then
      [ -f "$1" ] || mv -f "${_hop_cds_tmp}" "$1" 2>/dev/null
      rm -f "${_hop_cds_tmp}"
    elif ! kill -0 "${_hop_cds_tmp##*.}" 2>/dev/null; then
      rm -f "${_hop_cds_tmp}"
    fi
  done
}

# Whether the JVM finished writing an archive: it writes the header, the first bytes of
# the file, last.
hop_cds_complete() {
  [ -s "$1" ] && [ "$(od -An -tx1 -N4 "$1" 2>/dev/null | tr -d ' \n')" != 00000000 ]
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

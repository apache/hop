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

# Writes the class data sharing archives of hop-conf and hop-run (see lib/hop-cds.sh),
# so that the first real run already starts fast and nothing writes an archive later.
# Docker images run it while they are built and then set HOP_CDS=use.
#
# It runs each tool once on the samples project: hop-conf lists the projects, hop-run
# runs a workflow that runs pipelines. It uses a copy of the configuration folder and a
# scratch audit folder, so the installation's own configuration and audit stay as they
# are. The archives go where the tools will look for them: HOP_CDS_FOLDER, or the cds
# folder of HOP_AUDIT_FOLDER (./audit by default).
#
# Run it again after the installation changed: a new JDK, other jars in lib/core. Run it
# with the same HOP_OPTIONS, HOP_PLUGIN_BASE_FOLDERS and JDK the tools use: each class
# path has its own archive.
#
# Usage: lib/generate-cds-archives.sh

cd "$(dirname "$0")/.." || exit 1

archives="${HOP_CDS_FOLDER:-${HOP_AUDIT_FOLDER:-./audit}/cds}"
mkdir -p "${archives}" || exit 1
rm -f "${archives}"/hop-conf-*.jsa "${archives}"/hop-run-*.jsa \
  "${archives}"/hop-conf-*.jsa.* "${archives}"/hop-run-*.jsa.*

work=$(mktemp -d 2>/dev/null || mktemp -d -t hop-cds)
cp -R config "${work}/config" || exit 1

(
  export HOP_CDS=auto HOP_CDS_FOLDER="${archives}"
  export HOP_CONFIG_FOLDER="${work}/config" HOP_AUDIT_FOLDER="${work}/audit"
  ./hop-conf.sh -pl >"${work}/conf.log" 2>&1
  # One JVM writes the archive, when it exits: a workflow that runs pipelines loads the
  # workflow and pipeline engines and the plugins of both. A second run would only use it.
  ./hop-run.sh -j samples -r local \
    -f '${PROJECT_HOME}/loops/loops-process-rows-from-memory.hwf' >"${work}/run.log" 2>&1
)

# Each JVM wrote a file of its own, which a tool moves into place at its next start: do
# that now, so that HOP_CDS=use finds the archives.
. lib/hop-cds.sh
for written in "${archives}"/hop-conf-*.jsa.* "${archives}"/hop-run-*.jsa.*; do
  [ -f "${written}" ] && hop_cds_promote "${written%.jsa.*}.jsa"
done

status=0
for tool in conf run; do
  written=""
  for archive in "${archives}"/hop-${tool}-*.jsa; do
    [ -s "${archive}" ] && written="${archive}"
  done
  if [ -n "${written}" ]; then
    echo "Class data sharing archive written: ${written}"
  else
    echo "No class data sharing archive for hop-${tool}: the tool starts without one." >&2
    tail -n 20 "${work}/${tool}.log" >&2
    status=1
  fi
done
rm -rf "${work}"
exit ${status}

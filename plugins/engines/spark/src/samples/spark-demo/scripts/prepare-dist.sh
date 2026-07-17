#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Prepare artifacts for the Native Spark cluster walk-through (outside the source tree):
#   1) native-provided fat jar
#   2) Spark project package zip (definitions + metadata)
#   3) seed data copy (also copied into the Docker volume by the submit script)
#
# Usage (from Hop install or with HOP_HOME set; repository root optional via REPO_ROOT):
#   ./plugins/engines/spark/src/samples/spark-demo/scripts/prepare-dist.sh
#
# Environment:
#   HOP_HOME       Hop client install (default: current dir if hop-conf.sh is present)
#   REPO_ROOT      Hop git checkout (default: detected from this script location)
#   DIST_DIR       Output directory (default: /tmp/spark-demo-dist)
#   PROJECT_NAME   Hop project name for hop-conf (default: spark-demo)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMPLE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# scripts → spark-demo → samples → src → spark → engines → plugins → repo root
REPO_ROOT="${REPO_ROOT:-$(cd "${SCRIPT_DIR}/../../../../../../.." && pwd)}"
DIST_DIR="${DIST_DIR:-/tmp/spark-demo-dist}"
PROJECT_NAME="${PROJECT_NAME:-spark-demo}"
FAT_JAR_NAME="${FAT_JAR_NAME:-hop-native-spark4-submit.jar}"
PACKAGE_NAME="${PACKAGE_NAME:-spark-demo.zip}"

if [[ -n "${HOP_HOME:-}" ]]; then
  HOP_CONF="${HOP_HOME}/hop-conf.sh"
elif [[ -x "./hop-conf.sh" ]]; then
  HOP_CONF="$(pwd)/hop-conf.sh"
  HOP_HOME="$(pwd)"
elif [[ -x "${REPO_ROOT}/assemblies/client/target/hop/hop-conf.sh" ]]; then
  HOP_HOME="${REPO_ROOT}/assemblies/client/target/hop"
  HOP_CONF="${HOP_HOME}/hop-conf.sh"
else
  echo "ERROR: hop-conf.sh not found. Set HOP_HOME to a Hop install that includes the native Spark plugin." >&2
  exit 1
fi

if [[ ! -x "${HOP_CONF}" ]]; then
  echo "ERROR: not executable: ${HOP_CONF}" >&2
  exit 1
fi

mkdir -p "${DIST_DIR}"
echo ">>> HOP_HOME=${HOP_HOME}"
echo ">>> SAMPLE_DIR=${SAMPLE_DIR}"
echo ">>> DIST_DIR=${DIST_DIR}"

echo ">>> Generating native-provided fat jar (Spark provided by cluster)..."
(
  cd "${HOP_HOME}"
  ./hop-conf.sh \
    --generate-fat-jar="${DIST_DIR}/${FAT_JAR_NAME}" \
    --spark-client-version=native-provided
)

# Note: Hop has two different "project" flags:
#   -j / (enable for this command)  → sets PROJECT_HOME  [ProjectsOptionPlugin]
#   -p / --project                  → name for create/delete/list only [ManageProjects]
# You do NOT need to register spark-demo in hop-config for the export below.
# Optional GUI convenience: hop-conf -pc -p spark-demo -ph "${SAMPLE_DIR}"

echo ">>> Exporting Native Spark project package from ${SAMPLE_DIR}..."
(
  cd "${HOP_HOME}"
  ./hop-conf.sh \
    --export-spark-project="${DIST_DIR}/${PACKAGE_NAME}" \
    --export-spark-project-home="${SAMPLE_DIR}"
)

echo ">>> Copying sample data into dist (for inspection; cluster uses /data/hop-data)..."
mkdir -p "${DIST_DIR}/data"
cp -f "${SAMPLE_DIR}/data/customers-sample.csv" "${DIST_DIR}/data/"
cp -f "${SAMPLE_DIR}/data/countries.csv" "${DIST_DIR}/data/"
cp -f "${SAMPLE_DIR}/cluster-env.json" "${DIST_DIR}/"

# Host bind mount for the cluster data plane (compose: …/hop-data → /data/hop-data)
HOP_DATA_HOST_DIR="${HOP_DATA_HOST_DIR:-${DIST_DIR}/hop-data}"
mkdir -p "${HOP_DATA_HOST_DIR}/out" "${HOP_DATA_HOST_DIR}/executions" "${HOP_DATA_HOST_DIR}/packages"
echo ">>> Host data plane: ${HOP_DATA_HOST_DIR} (mounted as /data/hop-data in the cluster)"

echo
echo "Done. Contents of ${DIST_DIR}:"
ls -la "${DIST_DIR}"
echo
echo "Next:"
if [[ "${DIST_DIR}" == "/tmp/spark-demo-dist" ]]; then
  echo "  docker compose -f ${REPO_ROOT}/docker/integration-tests/integration-tests-spark-native-cluster.yaml up -d --build --scale spark-worker=2"
else
  echo "  HOP_DIST_DIR=${DIST_DIR} HOP_DATA_HOST_DIR=${HOP_DATA_HOST_DIR} \\"
  echo "    docker compose -f ${REPO_ROOT}/docker/integration-tests/integration-tests-spark-native-cluster.yaml up -d --build --scale spark-worker=2"
fi
echo "  docker compose -f ${REPO_ROOT}/docker/integration-tests/integration-tests-spark-native-cluster.yaml exec spark /opt/hop-samples/spark-demo/scripts/spark-submit-demo.sh"
echo "  # Workflow Executor country demo:"
echo "  docker compose -f ${REPO_ROOT}/docker/integration-tests/integration-tests-spark-native-cluster.yaml exec spark /opt/hop-samples/spark-demo/scripts/spark-submit-workflows-demo.sh"
echo "  # Nested Native Spark pipelines (per country) demo:"
echo "  docker compose -f ${REPO_ROOT}/docker/integration-tests/integration-tests-spark-native-cluster.yaml exec spark /opt/hop-samples/spark-demo/scripts/spark-submit-pipelines-demo.sh"
echo "  # Inspect on the host (outputs + execution info):"
echo "  ls -la ${HOP_DATA_HOST_DIR}/out ${HOP_DATA_HOST_DIR}/executions"

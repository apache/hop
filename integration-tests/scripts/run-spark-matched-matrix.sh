#!/bin/bash

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

# Matched-pair Spark matrix: for each version V, use Spark client pack V + cluster V.
#
# Unlike run-spark-matrix.sh (fixed Hop client 3.5.8 vs varying cluster), this script:
#   1. Materialises lib/spark-clients/<V>/ into the Hop install
#   2. Rebuilds hop-base + hop-beam images so the fat jar embeds pack V
#   3. Runs the smoke suite with HOP_SPARK_CLIENT_VERSION=V and SPARK_VERSION=V
#
# Prerequisites: assemblies/client package built (hop-client zip present).
#
# Usage:
#   ./integration-tests/scripts/run-spark-matched-matrix.sh KEEP_IMAGES=true
#   SPARK_VERSIONS="3.5.8 3.4.4" ./integration-tests/scripts/run-spark-matched-matrix.sh

set -u

CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "${CURRENT_DIR}/../.." >/dev/null 2>&1 && pwd)"
REPORT_DIR="${CURRENT_DIR}/../surefire-reports"
COMMITTED_REPORT="${CURRENT_DIR}/../spark/MATRIX-REPORT-MATCHED.md"
MATRIX_REPORT="${REPORT_DIR}/spark-matched-matrix-report.md"
RUN_TESTS="${CURRENT_DIR}/run-tests-docker.sh"
MATERIALIZE="${REPO_ROOT}/tools/spark-client-pack/materialize-pack.sh"
DOCKER_FILES_DIR="$(cd "${CURRENT_DIR}/../../docker/integration-tests" >/dev/null 2>&1 && pwd)"
HOP_HOME="${REPO_ROOT}/assemblies/client/target/hop"

if [ -z "${SPARK_VERSIONS:-}" ]; then
  # Default: current 3.5.x pin. Pre-3.5 fails on Java 21 (SPARK-42369) even when matched.
  # Override to re-document the ceiling: SPARK_VERSIONS="3.5.8 3.4.4"
  SPARK_VERSIONS="3.5.8"
fi

FORWARD_ARGS=("$@")
mkdir -p "${REPORT_DIR}"

GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BEAM_VERSION=$(
  grep -m1 'apache-beam.version' "${REPO_ROOT}/pom.xml" |
    sed -E 's/.*<apache-beam.version>([^<]+)<\/apache-beam.version>.*/\1/' || true
)

hadoop_version_for_spark() {
  case "$1" in
  3.0.* | 3.1.* | 3.2.*) echo "3.2" ;;
  *) echo "3" ;;
  esac
}

evaluate_result() {
  local report_file="$1"
  RESULT="FAIL"
  NOTES="—"
  if [ ! -f "${report_file}" ]; then
    NOTES="no surefire report"
    return
  fi
  if grep -q '<failure' "${report_file}"; then
    if grep -q 'environment_setup' "${report_file}"; then
      NOTES="docker/environment failed"
    elif grep -q 'InvalidClassException\|serialVersionUID' "${report_file}"; then
      NOTES="serialVersionUID skew (client/cluster still mismatched?)"
    elif grep -q 'DirectByteBuffer' "${report_file}"; then
      NOTES="Java 21 DirectByteBuffer incompatibility"
    elif grep -q 'stopped SparkContext' "${report_file}"; then
      NOTES="stopped SparkContext"
    elif grep -q 'ERROR:' "${report_file}"; then
      NOTES="pipeline ERROR"
    else
      NOTES="test failed"
    fi
    return
  fi
  if ! grep -q '<testcase ' "${report_file}"; then
    NOTES="empty report"
    return
  fi
  RESULT="PASS"
  if grep -q 'Beam pipeline execution has finished' "${report_file}"; then
    NOTES="matched pack+cluster smoke finished"
  else
    NOTES="hop-run exit 0"
  fi
}

# Ensure hop tree exists (same unzip as run-tests-docker)
if [ ! -d "${HOP_HOME}" ]; then
  echo "Extracting hop client zip into ${HOP_HOME} ..."
  unzip -o -q "${REPO_ROOT}/assemblies/client/target/"*.zip -d "${REPO_ROOT}/assemblies/client/target/"
fi

{
  echo "# Hop Beam Spark *matched* client/cluster matrix"
  echo
  echo "Generated: ${GENERATED_AT}"
  echo
  echo "- Hop: \`2.19.0-SNAPSHOT\` · Beam: \`${BEAM_VERSION}\`"
  echo "- Each cell: **Spark client pack V** = **cluster V** (Java 21 driver + cluster)"
  echo "- Suite: \`main-0001-test-spark-cluster\`"
  echo
  echo "| Client pack | Cluster | Result | Notes |"
  echo "| --- | --- | --- | --- |"
} >"${MATRIX_REPORT}"

OVERALL_RC=0
VERSION_TIMEOUT_SEC="${VERSION_TIMEOUT_SEC:-720}"

for SPARK_VERSION in ${SPARK_VERSIONS}; do
  echo "==========================================="
  echo "Matched matrix: client ${SPARK_VERSION} + cluster ${SPARK_VERSION}"
  echo "==========================================="

  HADOOP_VERSION="$(hadoop_version_for_spark "${SPARK_VERSION}")"
  VERSION_SAFE=${SPARK_VERSION//./_}
  VERSION_REPORT="${REPORT_DIR}/surefile_spark_matched_${VERSION_SAFE}.xml"

  # 1) Materialise pack into the Hop install that docker base image will COPY
  bash "${MATERIALIZE}" "${SPARK_VERSION}" "${HOP_HOME}"

  # 2) Force rebuild of base + beam images so fat jar embeds this pack
  #    (base image must pick up newly added lib/spark-clients/)
  export HOP_SPARK_CLIENT_VERSION="${SPARK_VERSION}"
  export SPARK_VERSION
  export HADOOP_VERSION
  export SKIP_SUREFIRE_CLEAN=true
  export KEEP_IMAGES="${KEEP_IMAGES:-true}"

  rm -f "${REPORT_DIR}/surefile_spark.xml"
  rm -f "${REPORT_DIR}/failed_tests" "${REPORT_DIR}/passed_tests"

  # run-tests-docker.sh will: unzip client zip, re-materialise pack, copy pack → lib/spark-client,
  # invalidate/rebuild hop-base + hop-beam images, then run smoke. Do not pre-build images here
  # (a pre-build is wiped by unzip inside run-tests-docker).
  set +e
  timeout --signal=TERM --kill-after=60 "${VERSION_TIMEOUT_SEC}" \
    bash "${RUN_TESTS}" \
      PROJECT_NAME=spark \
      SPARK_VERSION="${SPARK_VERSION}" \
      HADOOP_VERSION="${HADOOP_VERSION}" \
      HOP_SPARK_CLIENT_VERSION="${SPARK_VERSION}" \
      KEEP_IMAGES="${KEEP_IMAGES:-true}" \
      "${FORWARD_ARGS[@]}"
  RC=$?
  set -e

  PROJECT_NAME=spark SPARK_VERSION="${SPARK_VERSION}" HADOOP_VERSION="${HADOOP_VERSION}" \
    HOP_SPARK_CLIENT_VERSION="${SPARK_VERSION}" \
    docker compose -f "${DOCKER_FILES_DIR}/integration-tests-spark.yaml" down --remove-orphans >/dev/null 2>&1 || true

  if [ -f "${REPORT_DIR}/surefile_spark.xml" ]; then
    cp -f "${REPORT_DIR}/surefile_spark.xml" "${VERSION_REPORT}"
  fi

  evaluate_result "${VERSION_REPORT}"
  if [ ${RC} -eq 124 ] || [ ${RC} -eq 137 ]; then
    RESULT="FAIL"
    NOTES="timed out (${NOTES})"
  fi
  if [ "${RESULT}" != "PASS" ]; then
    OVERALL_RC=1
  fi

  echo "| ${SPARK_VERSION} | ${SPARK_VERSION} | ${RESULT} | ${NOTES} |" >>"${MATRIX_REPORT}"
  echo "Matched result ${SPARK_VERSION}: ${RESULT} (${NOTES})"
done

{
  echo
  echo "## How to re-run"
  echo
  echo '```bash'
  echo "./integration-tests/scripts/run-spark-matched-matrix.sh KEEP_IMAGES=true"
  echo "SPARK_VERSIONS=\"3.5.8 3.4.4\" ./integration-tests/scripts/run-spark-matched-matrix.sh"
  echo '```'
  echo
  echo "## Notes"
  echo
  echo "- Client pack materialised by \`tools/spark-client-pack/materialize-pack.sh\`."
  echo "- Fat jar built with \`hop-conf --spark-client-version=V\` inside hop-beam-image."
  echo "- Driver uses \`HOP_SPARK_CLIENT_VERSION=V\` so classpath matches the fat jar."
  echo "- Compare with fixed-client results in \`MATRIX-REPORT.md\`."
} >>"${MATRIX_REPORT}"

cp -f "${MATRIX_REPORT}" "${COMMITTED_REPORT}"
echo
cat "${MATRIX_REPORT}"
exit ${OVERALL_RC}

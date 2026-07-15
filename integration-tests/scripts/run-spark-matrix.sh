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

# Run the Hop Beam Spark smoke suite against multiple Spark 3.x standalone versions.
# Hop and Beam stay fixed; only the cluster image SPARK_VERSION changes.
#
# Usage:
#   ./integration-tests/scripts/run-spark-matrix.sh
#   SPARK_VERSIONS="3.5.8 3.4.4" ./integration-tests/scripts/run-spark-matrix.sh
#   ./integration-tests/scripts/run-spark-matrix.sh KEEP_IMAGES=true
#
# Prerequisites: assemblies/client must already be built (same as run-tests-docker.sh).

set -u

CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "${CURRENT_DIR}/../.." >/dev/null 2>&1 && pwd)"
REPORT_DIR="${CURRENT_DIR}/../surefire-reports"
# Committed snapshot of the last measured matrix (refreshed by this script)
COMMITTED_REPORT="${CURRENT_DIR}/../spark/MATRIX-REPORT.md"
MATRIX_REPORT="${REPORT_DIR}/spark-matrix-report.md"
RUN_TESTS="${CURRENT_DIR}/run-tests-docker.sh"
DOCKER_COMPOSE_FILE="$(cd "${CURRENT_DIR}/../../docker/integration-tests" >/dev/null 2>&1 && pwd)/integration-tests-spark.yaml"

# Representative patch releases for each Spark 3.x line.
# Override with SPARK_VERSIONS="3.5.8 3.2.4" etc.
# Note: Hop client mode bundles spark-core at plugins/engines/beam spark.version.
# Clusters on a different Spark minor typically fail with InvalidClassException
# (ApplicationDescription serialVersionUID mismatch). That is still a measured result.
if [ -z "${SPARK_VERSIONS:-}" ]; then
  SPARK_VERSIONS="3.2.4 3.3.4 3.4.4 3.5.8"
fi

# Map Spark line → Hadoop binary suffix used on archive.apache.org
# 3.2.x ships as bin-hadoop3.2; 3.3+ use bin-hadoop3.
hadoop_version_for_spark() {
  case "$1" in
  3.0.* | 3.1.* | 3.2.*) echo "3.2" ;;
  *) echo "3" ;;
  esac
}

FORWARD_ARGS=("$@")

mkdir -p "${REPORT_DIR}"

HOP_VERSION="2.19.0-SNAPSHOT"
BEAM_VERSION="unknown"
if [ -f "${REPO_ROOT}/pom.xml" ]; then
  BEAM_VERSION=$(
    grep -m1 'apache-beam.version' "${REPO_ROOT}/pom.xml" |
      sed -E 's/.*<apache-beam.version>([^<]+)<\/apache-beam.version>.*/\1/' || true
  )
fi
if [ -f "${REPO_ROOT}/plugins/engines/beam/pom.xml" ]; then
  SPARK_CLIENT_VERSION=$(
    grep -m1 'spark.version' "${REPO_ROOT}/plugins/engines/beam/pom.xml" |
      sed -E 's/.*<spark.version>([^<]+)<\/spark.version>.*/\1/' || true
  )
else
  SPARK_CLIENT_VERSION="unknown"
fi

GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Evaluate a surefire report. Sets RESULT and NOTES.
# Note: run-tests.sh increments failure counters inside a pipeline subshell, so
# suite-level failures=/errors= attributes are often wrong. Trust <failure> tags.
evaluate_result() {
  local report_file="$1"
  RESULT="FAIL"
  NOTES="—"

  if [ ! -f "${report_file}" ]; then
    NOTES="no surefire report produced"
    return
  fi

  if grep -q '<failure' "${report_file}"; then
    if grep -q 'environment_setup' "${report_file}"; then
      NOTES="docker/environment failed to start"
    elif grep -q 'InvalidClassException\|serialVersionUID' "${report_file}"; then
      NOTES="Spark client/cluster version skew (serialVersionUID)"
    elif grep -q 'NoClassDefFoundError' "${report_file}"; then
      NOTES="NoClassDefFoundError (classpath)"
    elif grep -q 'UnsupportedClassVersionError' "${report_file}"; then
      NOTES="UnsupportedClassVersionError (Java mismatch)"
    elif grep -q 'Invalid Spark URL' "${report_file}"; then
      NOTES="Invalid Spark URL (driver hostname)"
    elif grep -q 'Servlet class org.glassfish.jersey' "${report_file}"; then
      NOTES="Spark UI servlet clash"
    elif grep -q 'stopped SparkContext' "${report_file}"; then
      NOTES="stopped SparkContext (often version skew)"
    elif grep -q 'ERROR:' "${report_file}"; then
      NOTES="pipeline ERROR (see surefire log)"
    else
      NOTES="test failed (see surefire log)"
    fi
    return
  fi

  if ! grep -q '<testcase ' "${report_file}"; then
    NOTES="empty surefire report"
    return
  fi

  RESULT="PASS"
  if grep -q 'Beam pipeline execution has finished' "${report_file}"; then
    NOTES="smoke pipeline finished"
  else
    NOTES="hop-run exit 0"
  fi
}

{
  echo "# Hop Beam Spark support matrix"
  echo
  echo "Generated: ${GENERATED_AT}"
  echo
  echo "- Hop: \`${HOP_VERSION}\`"
  echo "- Beam: \`${BEAM_VERSION}\`"
  echo "- Client Spark libraries (\`spark.version\`): \`${SPARK_CLIENT_VERSION}\` (scala 2.12)"
  echo "- Driver JVM: Java 21 (Hop requirement)"
  echo "- Cluster JVM: Java 21 (matches Hop bytecode in fat jar)"
  echo "- Suite: \`integration-tests/spark\` smoke (\`main-0001-test-spark-cluster\`)"
  echo "- Cluster: Spark standalone master + worker (parameterized \`SPARK_VERSION\`)"
  echo
  echo "| Spark version | Result | Surefire report | Notes |"
  echo "| --- | --- | --- | --- |"
} >"${MATRIX_REPORT}"

OVERALL_RC=0

for SPARK_VERSION in ${SPARK_VERSIONS}; do
  echo "==========================================="
  echo "Matrix run: Spark ${SPARK_VERSION}"
  echo "==========================================="

  VERSION_SAFE=${SPARK_VERSION//./_}
  VERSION_REPORT="${REPORT_DIR}/surefile_spark_${VERSION_SAFE}.xml"
  HADOOP_VERSION="$(hadoop_version_for_spark "${SPARK_VERSION}")"

  echo "Cluster binary: spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"

  # Drop the live surefile so a failed environment cannot inherit a prior PASS report
  rm -f "${REPORT_DIR}/surefile_spark.xml"
  rm -f "${REPORT_DIR}/failed_tests" "${REPORT_DIR}/passed_tests"

  # Per-version wall clock (seconds). Version-skew / Java mismatches can thrash
  # executors for a long time without a hard timeout.
  VERSION_TIMEOUT_SEC="${VERSION_TIMEOUT_SEC:-600}"

  set +e
  export SKIP_SUREFIRE_CLEAN=true
  export SPARK_VERSION
  export HADOOP_VERSION
  export KEEP_IMAGES="${KEEP_IMAGES:-true}"
  # shellcheck disable=SC2086
  timeout --signal=TERM --kill-after=60 "${VERSION_TIMEOUT_SEC}" \
    bash "${RUN_TESTS}" \
      PROJECT_NAME=spark \
      SPARK_VERSION="${SPARK_VERSION}" \
      HADOOP_VERSION="${HADOOP_VERSION}" \
      KEEP_IMAGES="${KEEP_IMAGES:-true}" \
      "${FORWARD_ARGS[@]}"
  RC=$?
  set -e

  if [ ${RC} -eq 124 ] || [ ${RC} -eq 137 ]; then
    echo "Matrix run for Spark ${SPARK_VERSION} timed out after ${VERSION_TIMEOUT_SEC}s"
  fi

  # Always tear down this version's stack before the next rebuild
  PROJECT_NAME=spark SPARK_VERSION="${SPARK_VERSION}" HADOOP_VERSION="${HADOOP_VERSION}" \
    docker compose -f "${DOCKER_COMPOSE_FILE}" down --remove-orphans >/dev/null 2>&1 || true

  if [ -f "${REPORT_DIR}/surefile_spark.xml" ]; then
    cp -f "${REPORT_DIR}/surefile_spark.xml" "${VERSION_REPORT}"
  fi

  evaluate_result "${VERSION_REPORT}"

  # Enrich notes from the matrix log / known environmental failures when surefire is empty
  if [ "${RESULT}" = "FAIL" ] && [ "${NOTES}" = "docker/environment failed to start" ]; then
    if docker logs "$(docker ps -aq --filter name=integration-tests-spark 2>/dev/null | head -1)" 2>/dev/null |
      grep -q 'DirectByteBuffer'; then
      NOTES="Spark does not start on Java 21 (DirectByteBuffer)"
    fi
  fi
  if [ ${RC} -eq 124 ] || [ ${RC} -eq 137 ]; then
    RESULT="FAIL"
    NOTES="timed out after ${VERSION_TIMEOUT_SEC}s (${NOTES})"
  fi

  if [ "${RESULT}" != "PASS" ]; then
    OVERALL_RC=1
  fi
  # Script exit code from docker is informational only (container often exits 0)
  if [ ${RC} -ne 0 ] && [ ${RC} -ne 124 ] && [ ${RC} -ne 137 ] && [ "${RESULT}" = "PASS" ]; then
    NOTES="${NOTES}; docker rc=${RC}"
  fi

  echo "| ${SPARK_VERSION} | ${RESULT} | \`surefile_spark_${VERSION_SAFE}.xml\` | ${NOTES} |" >>"${MATRIX_REPORT}"
  echo "Matrix result for Spark ${SPARK_VERSION}: ${RESULT} (docker_rc=${RC}, notes=${NOTES})"
done

{
  echo
  echo "## How to re-run"
  echo
  echo '```bash'
  echo "# Full matrix"
  echo "./integration-tests/scripts/run-spark-matrix.sh KEEP_IMAGES=true"
  echo
  echo "# Single version"
  echo "SPARK_VERSION=3.5.8 ./integration-tests/scripts/run-tests-docker.sh PROJECT_NAME=spark KEEP_IMAGES=true"
  echo '```'
  echo
  echo "## Notes"
  echo
  echo "- Beam upstream docs still list Spark 3.2.x as the supported line; this matrix is **empirical** for Hop pipelines on Beam ${BEAM_VERSION}."
  echo "- Hive catalog workflows are deferred (\`optional-0002/0003-*.hwf\`) and not part of this matrix."
  echo "- **Client mode** (Hop GUI / hop-run → \`spark://master\`) uses Spark libraries bundled with Hop (\`${SPARK_CLIENT_VERSION}\`)."
  echo "  Clusters on a different Spark minor almost always fail with \`InvalidClassException\` / serialVersionUID mismatch on \`ApplicationDescription\`."
  echo "  Supported client-mode line is therefore the Hop \`spark.version\` minor (currently 3.5.x)."
  echo "- Spark 3.2.x binaries use the \`bin-hadoop3.2\` artifact name; 3.3+ use \`bin-hadoop3\`."
} >>"${MATRIX_REPORT}"

# Refresh the committed project copy for docs / PR visibility
cp -f "${MATRIX_REPORT}" "${COMMITTED_REPORT}"

echo
echo "Matrix report written to:"
echo "  ${MATRIX_REPORT}"
echo "  ${COMMITTED_REPORT}"
echo
cat "${MATRIX_REPORT}"

exit ${OVERALL_RC}

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
#   SPARK_VERSIONS="3.5.7 3.4.4" ./integration-tests/scripts/run-spark-matrix.sh
#   ./integration-tests/scripts/run-spark-matrix.sh JENKINS_USER=... KEEP_IMAGES=true
#
# Prerequisites: assemblies/client must already be built (same as run-tests-docker.sh).

set -u

CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPORT_DIR="${CURRENT_DIR}/../surefire-reports"
MATRIX_REPORT="${REPORT_DIR}/spark-matrix-report.md"
RUN_TESTS="${CURRENT_DIR}/run-tests-docker.sh"

# Representative patch releases for each Spark 3.x line (bin-hadoop3).
# Override with SPARK_VERSIONS="3.5.7 3.2.4" etc.
if [ -z "${SPARK_VERSIONS:-}" ]; then
  SPARK_VERSIONS="3.2.4 3.3.4 3.4.4 3.5.7"
fi

# Extra args forwarded to run-tests-docker.sh (JENKINS_*, GCP_KEY_FILE, KEEP_IMAGES, ...)
FORWARD_ARGS=("$@")

mkdir -p "${REPORT_DIR}"

HOP_VERSION="unknown"
BEAM_VERSION="unknown"
if [ -f "${CURRENT_DIR}/../../pom.xml" ]; then
  HOP_VERSION=$(grep -m1 '<version>' "${CURRENT_DIR}/../../pom.xml" | sed -E 's/.*<version>([^<]+)<\/version>.*/\1/' || true)
  BEAM_VERSION=$(grep -m1 'apache-beam.version' "${CURRENT_DIR}/../../pom.xml" | sed -E 's/.*<apache-beam.version>([^<]+)<\/apache-beam.version>.*/\1/' || true)
fi

{
  echo "# Hop Beam Spark support matrix"
  echo
  echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "- Hop: \`${HOP_VERSION}\`"
  echo "- Beam: \`${BEAM_VERSION}\`"
  echo "- Driver JVM: Java 21 (Hop requirement)"
  echo "- Cluster JVM: Java 21 (matches Hop bytecode in fat jar)"
  echo "- Suite: \`integration-tests/spark\` smoke (\`main-0001-test-spark-cluster\`)"
  echo
  echo "| Spark version | Result | Surefire report |"
  echo "| --- | --- | --- |"
} >"${MATRIX_REPORT}"

OVERALL_RC=0

for SPARK_VERSION in ${SPARK_VERSIONS}; do
  echo "==========================================="
  echo "Matrix run: Spark ${SPARK_VERSION}"
  echo "==========================================="

  # Isolate reports per version
  VERSION_SAFE=${SPARK_VERSION//./_}
  VERSION_REPORT="${REPORT_DIR}/surefile_spark_${VERSION_SAFE}.xml"

  set +e
  # Keep prior matrix surefire copies; still rebuild cluster images per version
  SKIP_SUREFIRE_CLEAN=true \
  SPARK_VERSION="${SPARK_VERSION}" \
  KEEP_IMAGES="${KEEP_IMAGES:-true}" \
    bash "${RUN_TESTS}" \
      PROJECT_NAME=spark \
      SPARK_VERSION="${SPARK_VERSION}" \
      KEEP_IMAGES="${KEEP_IMAGES:-true}" \
      "${FORWARD_ARGS[@]}"
  RC=$?
  set -e

  if [ -f "${REPORT_DIR}/surefile_spark.xml" ]; then
    cp -f "${REPORT_DIR}/surefile_spark.xml" "${VERSION_REPORT}"
  fi

  if [ ${RC} -eq 0 ] && [ -f "${VERSION_REPORT}" ] && ! grep -q 'failures="[1-9]' "${VERSION_REPORT}" && ! grep -q 'errors="[1-9]' "${VERSION_REPORT}"; then
    RESULT="PASS"
  else
    RESULT="FAIL"
    OVERALL_RC=1
  fi

  echo "| ${SPARK_VERSION} | ${RESULT} | \`surefile_spark_${VERSION_SAFE}.xml\` |" >>"${MATRIX_REPORT}"
  echo "Matrix result for Spark ${SPARK_VERSION}: ${RESULT} (rc=${RC})"
done

{
  echo
  echo "## Notes"
  echo
  echo "- Beam upstream docs still list Spark 3.2.x as the supported line; this matrix is empirical for Hop pipelines."
  echo "- Hive catalog workflows are deferred (\`optional-0002/0003-*.hwf\`) and not part of this matrix."
  echo "- Re-run a single version: \`SPARK_VERSION=3.5.7 ./integration-tests/scripts/run-tests-docker.sh PROJECT_NAME=spark\`"
} >>"${MATRIX_REPORT}"

echo
echo "Matrix report written to ${MATRIX_REPORT}"
cat "${MATRIX_REPORT}"

exit ${OVERALL_RC}

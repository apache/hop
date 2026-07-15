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

# Materialise a Spark client pack for Hop under lib/spark-clients/<version>/.
#
# Usage:
#   ./tools/spark-client-pack/materialize-pack.sh 3.4.4
#   ./tools/spark-client-pack/materialize-pack.sh 3.4.4 /path/to/hop
#   SPARK_CLIENT_VERSIONS="3.3.4 3.4.4 3.5.8" ./tools/spark-client-pack/materialize-pack.sh
#
# Then generate a matching fat jar:
#   HOP_SPARK_CLIENT_VERSION=3.4.4 ./hop-conf.sh --generate-fat-jar=/tmp/hop-spark-3.4.4.jar \
#       --spark-client-version=3.4.4
#
# And run Hop with the same pack on the driver classpath:
#   HOP_SPARK_CLIENT_VERSION=3.4.4 ./hop-run.sh ...

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)"
PACK_POM="${SCRIPT_DIR}/pom.xml"

# Default Hop install to materialise into (client assembly extract or live install)
HOP_HOME_RAW="${2:-${HOP_HOME:-${REPO_ROOT}/assemblies/client/target/hop}}"
# Always absolute so mvn -DoutputDirectory and existence checks are cwd-independent
HOP_HOME="$(cd "${HOP_HOME_RAW}" 2>/dev/null && pwd || true)"
if [ -z "${HOP_HOME}" ]; then
  mkdir -p "${HOP_HOME_RAW}"
  HOP_HOME="$(cd "${HOP_HOME_RAW}" && pwd)"
fi

if [ -n "${1:-}" ] && [[ "${1}" != *"="* ]]; then
  VERSIONS="$1"
elif [ -n "${SPARK_CLIENT_VERSIONS:-}" ]; then
  VERSIONS="${SPARK_CLIENT_VERSIONS}"
else
  VERSIONS="3.5.8"
fi

MVN="${REPO_ROOT}/mvnw"
if [ ! -x "${MVN}" ]; then
  MVN="mvn"
fi

# Only keep the same artifact set the beam assembly puts in lib/spark-client
keep_pack_jars() {
  local dir="$1"
  local version="$2"
  # Remove jars that are not part of the curated pack (transitive noise)
  find "${dir}" -maxdepth 1 -type f -name '*.jar' | while read -r jar; do
    base="$(basename "${jar}")"
    case "${base}" in
    spark-core_2.12-"${version}".jar | \
      spark-streaming_2.12-"${version}".jar | \
      spark-launcher_2.12-"${version}".jar | \
      spark-kvstore_2.12-"${version}".jar | \
      spark-network-common_2.12-"${version}".jar | \
      spark-network-shuffle_2.12-"${version}".jar | \
      spark-unsafe_2.12-"${version}".jar | \
      spark-tags_2.12-"${version}".jar | \
      spark-common-utils_2.12-"${version}".jar | \
      spark-sql-api_2.12-"${version}".jar)
      ;;
    *)
      rm -f "${jar}"
      ;;
    esac
  done
}

for VERSION in ${VERSIONS}; do
  OUT_DIR="${HOP_HOME}/lib/spark-clients/${VERSION}"
  echo "==========================================="
  echo "Materialising Spark client pack ${VERSION}"
  echo "  pom: ${PACK_POM}"
  echo "  out: ${OUT_DIR}"
  echo "==========================================="

  mkdir -p "${OUT_DIR}"
  rm -f "${OUT_DIR}"/*.jar

  "${MVN}" -f "${PACK_POM}" \
    -Dspark.client.version="${VERSION}" \
    dependency:copy-dependencies \
    -DoutputDirectory="${OUT_DIR}" \
    -DincludeScope=runtime \
    -q

  keep_pack_jars "${OUT_DIR}" "${VERSION}"

  echo "Pack contents:"
  ls -1 "${OUT_DIR}" | sed 's/^/  /'

  if [ ! -f "${OUT_DIR}/spark-core_2.12-${VERSION}.jar" ]; then
    echo "ERROR: spark-core_2.12-${VERSION}.jar missing from pack" >&2
    exit 1
  fi
done

echo "Done. Use HOP_SPARK_CLIENT_VERSION=<version> with hop-run / hop-conf --spark-client-version."

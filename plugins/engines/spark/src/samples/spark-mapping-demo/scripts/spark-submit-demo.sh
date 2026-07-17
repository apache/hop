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

# Run inside the Spark master container (see integration-tests-spark-native-cluster.yaml).
#
#   docker compose ... exec spark /opt/hop-samples/spark-mapping-demo/scripts/spark-submit-demo.sh
#
# Expects:
#   /opt/hop-dist/hop-native-spark4-submit.jar   (native-provided fat jar)
#   /opt/hop-dist/spark-mapping-demo.zip        (Native Spark project package)
#   /data/hop-data                              (shared volume on master + workers)
#
# Environment overrides:
#   FAT_JAR, PACKAGE_ZIP, RUN_CONFIG, PIPELINE_PATH, SPARK_MASTER, HOP_DATA_DIR, CLUSTER_ENV

set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/spark}"
FAT_JAR="${FAT_JAR:-/opt/hop-dist/hop-native-spark4-submit.jar}"
PACKAGE_ZIP="${PACKAGE_ZIP:-/opt/hop-dist/spark-mapping-demo.zip}"
CLUSTER_ENV="${CLUSTER_ENV:-/opt/hop-dist/cluster-env.json}"
SAMPLE_DATA="${SAMPLE_DATA:-/opt/hop-samples/spark-mapping-demo/data/customers-sample.csv}"
HOP_DATA_DIR="${HOP_DATA_DIR:-/data/hop-data}"
SPARK_MASTER_URL="${SPARK_MASTER:-spark://spark:7077}"
RUN_CONFIG="${RUN_CONFIG:-spark-cluster}"
PIPELINE_PATH="${PIPELINE_PATH:-pipelines/01-enrich-with-mapping.hpl}"
DRIVER_HOST="${SPARK_DRIVER_HOST:-spark}"

echo "=== Hop Native Spark mapping demo submit ==="
echo "SPARK_HOME     = ${SPARK_HOME}"
echo "FAT_JAR        = ${FAT_JAR}"
echo "PACKAGE_ZIP    = ${PACKAGE_ZIP}"
echo "SPARK_MASTER   = ${SPARK_MASTER_URL}"
echo "PIPELINE_PATH  = ${PIPELINE_PATH}"
echo "RUN_CONFIG     = ${RUN_CONFIG}"
echo "HOP_DATA_DIR   = ${HOP_DATA_DIR}"
echo "DRIVER_HOST    = ${DRIVER_HOST}"

if [[ ! -f "${FAT_JAR}" ]]; then
  echo "ERROR: fat jar not found: ${FAT_JAR}" >&2
  echo "Run prepare-dist.sh on the host and mount HOP_DIST_DIR into /opt/hop-dist." >&2
  exit 1
fi
if [[ ! -f "${PACKAGE_ZIP}" ]]; then
  echo "ERROR: project package not found: ${PACKAGE_ZIP}" >&2
  echo "Export with: hop-conf -j spark-mapping-demo --export-spark-project=... " >&2
  exit 1
fi
if [[ ! -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  echo "ERROR: spark-submit not found under ${SPARK_HOME}" >&2
  exit 1
fi

# Seed shared data volume (visible to every worker)
mkdir -p "${HOP_DATA_DIR}/out"
if [[ -f "${SAMPLE_DATA}" ]]; then
  cp -f "${SAMPLE_DATA}" "${HOP_DATA_DIR}/customers-sample.csv"
  echo ">>> Seeded ${HOP_DATA_DIR}/customers-sample.csv"
else
  echo "WARNING: sample CSV not found at ${SAMPLE_DATA}" >&2
fi

# Optional env file for HOP_DATA=file:///data/hop-data
HOP_CONFIG_ARG=()
if [[ -f "${CLUSTER_ENV}" ]]; then
  HOP_CONFIG_ARG=(--HopConfigFile="${CLUSTER_ENV}")
  echo ">>> Using cluster env file ${CLUSTER_ENV}"
fi

echo ">>> spark-submit (client mode on master; workers receive package via SparkFiles)..."
set -x
"${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --class org.apache.hop.spark.run.MainSpark \
  --conf "spark.driver.host=${DRIVER_HOST}" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --conf "spark.ui.enabled=false" \
  --conf "spark.sql.shuffle.partitions=4" \
  "${FAT_JAR}" \
  --HopProjectPackage="${PACKAGE_ZIP}" \
  --HopPipelinePath="${PIPELINE_PATH}" \
  --HopRunConfigurationName="${RUN_CONFIG}" \
  ${HOP_CONFIG_ARG[@]+"${HOP_CONFIG_ARG[@]}"}
set +x

echo
echo "=== Submit finished ==="
echo "Check output under shared volume:"
echo "  ls -la ${HOP_DATA_DIR}/out/enriched || true"
ls -la "${HOP_DATA_DIR}/out/enriched" 2>/dev/null || echo "(output dir not listed yet — check Spark UI / logs)"
if [[ -d "${HOP_DATA_DIR}/out/enriched" ]]; then
  echo "--- sample rows ---"
  head -n 20 "${HOP_DATA_DIR}/out/enriched"/* 2>/dev/null || true
fi

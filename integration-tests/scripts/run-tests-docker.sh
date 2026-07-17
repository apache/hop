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
#

CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
DOCKER_FILES_DIR="$(cd ${CURRENT_DIR}/../../docker/integration-tests/ && pwd)"
EXECUTED_COMPOSE_FILES=("${DOCKER_FILES_DIR}/integration-tests-base.yaml")

for ARGUMENT in "$@"; do

  # Quote so glob characters in values (e.g. TEST_FILTER='*0077*') are preserved.
  # cut -f2- keeps values that themselves contain '='.
  KEY=$(echo "${ARGUMENT}" | cut -f1 -d=)
  VALUE=$(echo "${ARGUMENT}" | cut -f2- -d=)

  case "$KEY" in
  PROJECT_NAME) PROJECT_NAME=${VALUE} ;;
  TEST_FILTER) TEST_FILTER=${VALUE} ;;
  JENKINS_USER) JENKINS_USER=${VALUE} ;;
  JENKINS_UID) JENKINS_UID=${VALUE} ;;
  JENKINS_GROUP) JENKINS_GROUP=${VALUE} ;;
  JENKINS_GID) JENKINS_GID=${VALUE} ;;
  GCP_KEY_FILE) GCP_KEY_FILE=${VALUE} ;;
  KEEP_IMAGES) KEEP_IMAGES=${VALUE} ;;
  CLIENT_UNZIP) CLIENT_UNZIP=${VALUE} ;;
  SPARK_VERSION) SPARK_VERSION=${VALUE} ;;
  HADOOP_VERSION) HADOOP_VERSION=${VALUE} ;;
  SPARK_BASE_URL) SPARK_BASE_URL=${VALUE} ;;
  HOP_SPARK_CLIENT_VERSION) HOP_SPARK_CLIENT_VERSION=${VALUE} ;;
  *) ;;
  esac

done

# Default Spark standalone version for integration-tests/spark (overridable for matrix runs)
if [ -z "${SPARK_VERSION}" ]; then
  SPARK_VERSION="3.5.8"
fi
if [ -z "${HADOOP_VERSION}" ]; then
  HADOOP_VERSION="3"
fi
# Prefer the fast CDN (current patch only); the Dockerfile falls back to archive.apache.org
# for historical matrix versions the CDN no longer carries.
if [ -z "${SPARK_BASE_URL}" ]; then
  SPARK_BASE_URL="https://dlcdn.apache.org/spark"
fi
# Optional: match driver + fat-jar Spark client pack to a cluster minor (see tools/spark-client-pack)
export SPARK_VERSION HADOOP_VERSION SPARK_BASE_URL
export HOP_SPARK_CLIENT_VERSION="${HOP_SPARK_CLIENT_VERSION:-}"

if [ -z "${PROJECT_NAME}" ]; then
  PROJECT_NAME="*"
fi

# Optional filter for main*.hwf basenames (substring or glob, comma-separated).
# Passed into the test container as TEST_FILTER; when set, run-tests.sh uses the
# classic per-workflow runner so only matching main*.hwf files execute. Examples:
#   ./run-tests-docker.sh PROJECT_NAME=transforms TEST_FILTER=0077-merge-rows
#   ./run-tests-docker.sh PROJECT_NAME=transforms TEST_FILTER='*0077*'
#   ./run-tests-docker.sh PROJECT_NAME=transforms TEST_FILTER='0077-merge-rows,0076-other'
if [ -z "${TEST_FILTER}" ]; then
  TEST_FILTER=""
fi
export TEST_FILTER

# Match the host/workspace owner when not overridden. ASF Jenkins (Jenkinsfile.daily)
# always passes the agent identity explicitly:
#   JENKINS_USER=${USER} JENKINS_UID=$(id -u) JENKINS_GROUP=$(id -gn) JENKINS_GID=$(id -g)
# Using the same defaults locally keeps the container user aligned with the bind-mounted
# integration-tests/ tree, so writes under ${PROJECT_HOME}/output (and elsewhere) succeed.
if [ -z "${JENKINS_USER}" ]; then
  JENKINS_USER="$(id -un 2>/dev/null || echo jenkins)"
fi

if [ -z "${JENKINS_UID}" ]; then
  JENKINS_UID="$(id -u 2>/dev/null || echo 1000)"
fi

if [ -z "${JENKINS_GROUP}" ]; then
  JENKINS_GROUP="$(id -gn 2>/dev/null || echo jenkins)"
fi

if [ -z "${JENKINS_GID}" ]; then
  JENKINS_GID="$(id -g 2>/dev/null || echo 1000)"
fi

echo "Integration-test container identity: user=${JENKINS_USER} uid=${JENKINS_UID} group=${JENKINS_GROUP} gid=${JENKINS_GID}"

if [ -z "${SUREFIRE_REPORT}" ]; then
  SUREFIRE_REPORT="true"
fi

if [ -z "${GCP_KEY_FILE}" ]; then
  GCP_KEY_FILE="./docker/integration-tests/resource/dummyfile"
fi

if [ -z "${HOP_OPTIONS}" ] ; then 
  HOP_OPTIONS="${HOP_OPTIONS} -Djavax.net.ssl.keyStore=./docker/integration-tests/resource/keystore.jks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=./docker/integration-tests/resource/mail/conf/keystore "
fi

if [ -z "${KEEP_IMAGES}" ]; then
  KEEP_IMAGES="false"
fi

# Unzip the client zip into assemblies/client/target/hop by default. Set CLIENT_UNZIP=false to
# skip when target/hop already exists (e.g. after patching a single plugin jar).
if [ -z "${CLIENT_UNZIP}" ]; then
  CLIENT_UNZIP="true"
fi

# Cleanup surefire reports (matrix runs set SKIP_SUREFIRE_CLEAN=true to keep per-version copies)
if [ "${SKIP_SUREFIRE_CLEAN:-false}" != "true" ]; then
  rm -rf "${CURRENT_DIR}"/../surefire-reports
fi
mkdir -p "${CURRENT_DIR}"/../surefire-reports/
chmod 777 "${CURRENT_DIR}"/../surefire-reports/

# Pre-create project output/ dirs on the host and make them world-writable.
# ASF Jenkins passes a container UID that matches the agent workspace owner, so ownership
# alone is enough there. World-writable output/ is a belt-and-suspenders for:
#   - local runs where someone overrides JENKINS_UID to a fixed value
#   - compose files that hardcode build-arg UIDs when not using this script's --build-arg
# Pipelines that write temp artifacts (Excel/ODS writer, etc.) use ${PROJECT_HOME}/output.
for d in "${CURRENT_DIR}"/../${PROJECT_NAME}/; do
  if [[ "$d" != *"scripts/" ]] && [[ "$d" != *"surefire-reports/" ]] && [[ "$d" != *"hopweb/" ]]; then
    if [ -d "$d" ] && [ ! -f "$d/disabled.txt" ]; then
      mkdir -p "$d/output"
      chmod 777 "$d/output" 2>/dev/null || true
    fi
  fi
done

HOP_CLIENT_TARGET_DIR="${CURRENT_DIR}/../../assemblies/client/target"
HOP_DIR="${HOP_CLIENT_TARGET_DIR}/hop"

# Unzip Hop client unless skipped and a usable hop folder is already present
if [ "${CLIENT_UNZIP}" = "true" ] || [ ! -d "${HOP_DIR}" ]; then
  if [ "${CLIENT_UNZIP}" != "true" ] && [ ! -d "${HOP_DIR}" ]; then
    echo "CLIENT_UNZIP=${CLIENT_UNZIP} but ${HOP_DIR} does not exist; unzipping client zip"
  else
    echo "Unzipping Hop client into ${HOP_CLIENT_TARGET_DIR} (CLIENT_UNZIP=${CLIENT_UNZIP})"
  fi
  unzip -o -q "${HOP_CLIENT_TARGET_DIR}"/*.zip -d "${HOP_CLIENT_TARGET_DIR}/"
else
  echo "Skipping client unzip (CLIENT_UNZIP=${CLIENT_UNZIP}, using existing ${HOP_DIR})"
fi

# Versioned Spark client packs are not in the client zip. Re-materialise after unzip so
# HOP_SPARK_CLIENT_VERSION=… finds lib/spark-clients/<ver>/ (includes spark-streaming, etc.).
# Also copy the selected pack into lib/spark-client/ so the default driver classpath always
# has spark-core + spark-streaming even if hop-run only loads lib/spark-client/*.
HOP_HOME_FOR_PACK="${CURRENT_DIR}/../../assemblies/client/target/hop"
if [ -n "${HOP_SPARK_CLIENT_VERSION}" ]; then
  MATERIALIZE_SCRIPT="${CURRENT_DIR}/../../tools/spark-client-pack/materialize-pack.sh"
  if [ -f "${MATERIALIZE_SCRIPT}" ]; then
    echo "Materialising Spark client pack ${HOP_SPARK_CLIENT_VERSION} into ${HOP_HOME_FOR_PACK}"
    bash "${MATERIALIZE_SCRIPT}" "${HOP_SPARK_CLIENT_VERSION}" "${HOP_HOME_FOR_PACK}"
    PACK_DIR="${HOP_HOME_FOR_PACK}/lib/spark-clients/${HOP_SPARK_CLIENT_VERSION}"
    if [ -d "${PACK_DIR}" ] && [ -f "${PACK_DIR}/spark-core_2.12-${HOP_SPARK_CLIENT_VERSION}.jar" ]; then
      echo "Activating pack ${HOP_SPARK_CLIENT_VERSION} as lib/spark-client (driver classpath)"
      rm -rf "${HOP_HOME_FOR_PACK}/lib/spark-client"
      mkdir -p "${HOP_HOME_FOR_PACK}/lib/spark-client"
      cp -a "${PACK_DIR}/." "${HOP_HOME_FOR_PACK}/lib/spark-client/"
      # Prove critical jars are present for the driver
      ls -1 "${HOP_HOME_FOR_PACK}/lib/spark-client"/spark-core*.jar \
            "${HOP_HOME_FOR_PACK}/lib/spark-client"/spark-streaming*.jar
    else
      echo "ERROR: Spark client pack incomplete at ${PACK_DIR}" >&2
      ls -la "${PACK_DIR}" 2>/dev/null || true
      exit 1
    fi
  else
    echo "WARNING: ${MATERIALIZE_SCRIPT} not found; pack ${HOP_SPARK_CLIENT_VERSION} may be missing"
  fi
fi

# Bust docker cache for the hop COPY layer when packs change
if [ -d "${HOP_HOME_FOR_PACK}" ]; then
  date -u +%Y-%m-%dT%H:%M:%SZ > "${HOP_HOME_FOR_PACK}/.spark-client-pack-stamp"
fi

# Drop stale base/beam images when using a versioned Spark pack so COPY hop picks up jars
if [ -n "${HOP_SPARK_CLIENT_VERSION}" ]; then
  echo "Invalidating hop-base-image / hop-beam-image for Spark client pack ${HOP_SPARK_CLIENT_VERSION}"
  docker rmi hop-beam-image 2>/dev/null || true
  docker rmi hop-base-image 2>/dev/null || true
fi

# Build base image only once (must run AFTER pack materialise so jars are in the image)
docker compose -f ${DOCKER_FILES_DIR}/integration-tests-base.yaml build --build-arg JENKINS_USER=${JENKINS_USER} --build-arg JENKINS_UID=${JENKINS_UID} --build-arg JENKINS_GROUP=${JENKINS_GROUP} --build-arg JENKINS_GID=${JENKINS_GID} --build-arg GCP_KEY_FILE=${GCP_KEY_FILE}

# The Hop fat jar (needed only by the Beam runners: spark/flink/gcp) is expensive to build, so it
# lives in a separate image (hop-beam-image) that we build lazily and only once, the first time a
# project that actually references the fat jar is about to run.
BEAM_IMAGE_BUILT="false"

# Loop over project folders
for d in "${CURRENT_DIR}"/../${PROJECT_NAME}/; do


  if [[ "$d" != *"scripts/" ]] && [[ "$d" != *"surefire-reports/" ]] && [[ "$d" != *"hopweb/" ]]; then
    # If there is a file called disabled.txt the project is disabled
    if [ ! -f "$d/disabled.txt" ]; then

      PROJECT_NAME=$(basename $d)

      echo "Project name: ${PROJECT_NAME}"
      echo "project path: $d"
      echo "docker compose path: ${DOCKER_FILES_DIR}"

      # If this project references the Hop fat jar (Beam runners), make sure hop-beam-image exists.
      # Built once per run, and only when such a project is actually enabled.
      if [ "${BEAM_IMAGE_BUILT}" != "true" ] && grep -rqs "hop-fatjar.jar" "$d" 2>/dev/null; then
        echo "Project ${PROJECT_NAME} needs the Hop fat jar; building hop-beam-image (once)."
        if [ -n "${HOP_SPARK_CLIENT_VERSION}" ]; then
          echo "Spark client pack for fat jar: ${HOP_SPARK_CLIENT_VERSION}"
        fi
        HOP_SPARK_CLIENT_VERSION="${HOP_SPARK_CLIENT_VERSION}" \
          docker compose -f ${DOCKER_FILES_DIR}/integration-tests-beam-base.yaml build \
            --build-arg HOP_SPARK_CLIENT_VERSION="${HOP_SPARK_CLIENT_VERSION}"
        EXECUTED_COMPOSE_FILES=("${EXECUTED_COMPOSE_FILES[@]}" "${DOCKER_FILES_DIR}/integration-tests-beam-base.yaml")
        BEAM_IMAGE_BUILT="true"
      fi

      # Check if specific compose exists

      if [ -n "${TEST_FILTER}" ]; then
        echo "TEST_FILTER: ${TEST_FILTER}"
      fi

      if [ -f "${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml" ]; then
        echo "Project compose exists."
        EXECUTED_COMPOSE_FILES=("${EXECUTED_COMPOSE_FILES[@]}" "${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml")
        # Rebuild project images so SPARK_VERSION (and similar) build args take effect.
        # hop_server also must rebuild: its hop-server service image (apache/hop:Development
        # from docker/Dockerfile) otherwise stays cached and can miss client-side assembly
        # plugins needed by remote-export ITs (main-0008/0009/0010).
        if [ "${PROJECT_NAME}" = "spark" ]; then
          echo "Spark IT cluster version: ${SPARK_VERSION} (hadoop ${HADOOP_VERSION})"
          PROJECT_NAME=${PROJECT_NAME} TEST_FILTER=${TEST_FILTER} SPARK_VERSION=${SPARK_VERSION} HADOOP_VERSION=${HADOOP_VERSION} SPARK_BASE_URL=${SPARK_BASE_URL} \
            docker compose -f ${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml up --build --abort-on-container-exit
        elif [ "${PROJECT_NAME}" = "hop_server" ]; then
          echo "Rebuilding hop_server images so remote Hop Server matches current assemblies"
          PROJECT_NAME=${PROJECT_NAME} TEST_FILTER=${TEST_FILTER} \
            docker compose -f ${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml up --build --abort-on-container-exit
        else
          PROJECT_NAME=${PROJECT_NAME} TEST_FILTER=${TEST_FILTER} docker compose -f ${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml up --abort-on-container-exit
        fi
      else
        echo "Project compose does not exists."
        PROJECT_NAME=${PROJECT_NAME} TEST_FILTER=${TEST_FILTER} docker compose -f ${DOCKER_FILES_DIR}/integration-tests-base.yaml up --abort-on-container-exit
      fi
    fi
  fi

  # Create final report
  if [ "${SUREFIRE_REPORT}" = "true" ]; then
    if [ ! -f "${CURRENT_DIR}/../surefire-reports/surefile_${PROJECT_NAME}.xml" ]; then
      echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" >"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
      echo "<testsuite xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"https://maven.apache.org/surefire/maven-surefire-plugin/xsd/surefire-test-report-3.0.xsd\" version=\"3.0\" name=\"${PROJECT_NAME}\" time=\"0\" tests=\"1\" errors=\"1\" skipped=\"0\" failures=\"0\">" >>"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
      echo "<testcase name=\"environment_setup\" time=\"1\"><failure type=\"could not start\"></failure><system-out><![CDATA[ Could not start docker environment ]]></system-out><system-err><![CDATA[ Could not start docker environment ]]></system-err></testcase>" >>"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
      echo "</testsuite>" >>"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
    fi
  fi
done

echo "Keep images value: ${KEEP_IMAGES}"
# Cleanup all images
if [ ! "${KEEP_IMAGES}" = "true" ]; then
  for d in "${EXECUTED_COMPOSE_FILES[@]}"; do
    echo "Removing: " $d
    PROJECT_NAME="" docker compose -f $d down --rmi all --remove-orphans
  done
fi

# Print Final Results
# Use CURRENT_DIR (script location) for both existence checks and reads. Relative
# paths like ../surefire-reports/ only work when cwd is integration-tests/scripts/;
# ASF Jenkins and local runs often invoke this script from the repo root.
if [ -f "${CURRENT_DIR}/../surefire-reports/passed_tests" ]; then
  echo -e "\033[1;32mPassed tests:"
  PASSED_TESTS="$(cat "${CURRENT_DIR}/../surefire-reports/passed_tests")"
  echo -e "\033[1;32m${PASSED_TESTS}"
fi
if [ -f "${CURRENT_DIR}/../surefire-reports/failed_tests" ]; then
  echo -e "\033[1;91mFailed tests:"
  FAILED_TESTS="$(cat "${CURRENT_DIR}/../surefire-reports/failed_tests")"
  echo -e "\033[1;91m${FAILED_TESTS}"
fi

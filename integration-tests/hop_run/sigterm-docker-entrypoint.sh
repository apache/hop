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

# Runs a pipeline or workflow ($1) that only ends when it is stopped through the entry point of
# the Hop Docker image (docker/resources/run.sh, mounted on /docker-resources), sends the entry
# point SIGTERM like docker stop or a Kubernetes pod eviction does, and checks that the signal
# reached the JVM and that hop-run stopped the run before it exited (issue #9168).

set -u

FILE="${1}"
DOCKER_RESOURCES="${DOCKER_RESOURCES:-/docker-resources}"
WORK="$(mktemp -d)"
LOG="${WORK}/entrypoint.log"

# The environment of the Hop image (docker/Dockerfile), with a project for this test.
DEPLOYMENT_PATH="$(dirname "$(command -v hop-run.sh)")"
export DEPLOYMENT_PATH
mkdir -p "${WORK}/config"
cp "${DEPLOYMENT_PATH}/config/hop-config.json" "${WORK}/config/"
export HOP_CONFIG_FOLDER="${WORK}/config"
export HOP_LOG_PATH="${WORK}/hop.err.log"
export HOP_LOG_LEVEL=Basic
export HOP_FILE_PATH="${PROJECT_HOME}/${FILE}"
export HOP_PROJECT_NAME=hop-run-sigterm
export HOP_PROJECT_FOLDER="${PROJECT_HOME}"
export HOP_PROJECT_CONFIG_FILE_NAME=project-config.json
export HOP_PARENT_PROJECT_CONFIG_FILE_NAME=project-config.json
export HOP_ENVIRONMENT_NAME=environment1
export HOP_RUN_CONFIG=local
for variable in HOP_SHARED_JDBC_FOLDERS HOP_PROJECT_DIRECTORY HOP_PARENT_PROJECT_NAME \
  HOP_PARENT_PROJECT_FOLDER HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS HOP_COMMAND \
  HOP_COMMAND_PARAMETERS HOP_RUN_PARAMETERS HOP_START_ACTION HOP_RUN_METADATA_EXPORT \
  HOP_SYSTEM_PROPERTIES HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH HOP_CONFIG_OPTIONS \
  HOP_SERVER_METADATA_FOLDER HOP_SERVER_KEYSTORE HOP_SERVER_KEYSTORE_PASSWORD \
  HOP_SERVER_KEY_PASSWORD HOP_SERVER_MAX_LOG_LINES HOP_SERVER_MAX_LOG_TIMEOUT \
  HOP_SERVER_MAX_OBJECT_TIMEOUT; do
  export "${variable}="
done

bash "${DOCKER_RESOURCES}/run.sh" >"${LOG}" 2>&1 &
PID=$!

# shellcheck source=/dev/null
. "${PROJECT_HOME}/sigterm-checks.sh"

waitForLog "${LOG}" "Starting (pipeline|workflow): " 180

# Let the run get going.
sleep 3

# docker stop signals the entry point, PID 1 in the container.
START=$(date +%s)
kill -TERM "${PID}"
wait "${PID}"
CODE=$?
ELAPSED=$(($(date +%s) - START))

checkStopped "${LOG}" "${CODE}" "${ELAPSED}"
grep -q "The run has stopped, hop-run exits." "${HOP_LOG_PATH}" ||
  fail "the log file ${HOP_LOG_PATH} misses the end of the run"
echo "The Docker entry point stopped ${FILE} in ${ELAPSED}s after SIGTERM"

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

set -Euo pipefail

log() {
  # shellcheck disable=SC2046
  echo $(date '+%Y/%m/%d %H:%M:%S')" - ${1}"
}

exitWithCode() {
  echo "${1}" >/tmp/exitcode.txt
  # log "wrote exit code ${1} to /tmp/exitcode.txt"
  exit "${1}"
}

#   write the hop-server config to a configuration file
#   to avoid the password of the server being shown in ps
#
#   bind the server to 0.0.0.0 to be able to expose the port
#   out of the docker container
#
write_server_config() {
  HOP_SERVER_USER=${HOP_SERVER_USER:-cluster}
  HOP_SERVER_PASS=${HOP_SERVER_PASS:-cluster}
  HOP_SERVER_HOSTNAME=${HOP_SERVER_HOSTNAME:-0.0.0.0}

  HOP_SERVER_XML=/tmp/hop-server.xml

  log "Writing a hop-server config file to "${HOP_SERVER_XML}

  echo "<hop-server-config>" >${HOP_SERVER_XML}
  # shellcheck disable=SC2129
  echo "  <hop-server>" >>${HOP_SERVER_XML}
  echo "    <name>Hop Server</name>" >>${HOP_SERVER_XML}
  echo "    <hostname>${HOP_SERVER_HOSTNAME}</hostname>" >>${HOP_SERVER_XML}
  echo "    <port>${HOP_SERVER_PORT}</port>" >>${HOP_SERVER_XML}
  echo "    <username>${HOP_SERVER_USER}</username>" >>${HOP_SERVER_XML}
  echo "    <password>${HOP_SERVER_PASS}</password>" >>${HOP_SERVER_XML}

  # If an SSL configuration is needed we need to include it here
  #
  if [ -n "${HOP_SERVER_KEYSTORE}" ]; then
    log "Configuring SSL with key store file: ${HOP_SERVER_KEYSTORE}"
    # shellcheck disable=SC2129
    echo "    <sslConfig>" >>${HOP_SERVER_XML}
    echo "      <keyStore>${HOP_SERVER_KEYSTORE}</keyStore>" >>${HOP_SERVER_XML}
    echo "      <keyStorePassword>${HOP_SERVER_KEYSTORE_PASSWORD}</keyStorePassword>" >>${HOP_SERVER_XML}
    if [ -n "${HOP_SERVER_KEY_PASSWORD}" ]; then
      echo "      <keyPassword>${HOP_SERVER_KEY_PASSWORD}</keyPassword>" >>${HOP_SERVER_XML}
    fi
    echo "    </sslConfig>" >>${HOP_SERVER_XML}
  fi
  echo "  </hop-server>" >>${HOP_SERVER_XML}

  # If the metadata folder is set, include it in the configuration
  #
  if [ -n "${HOP_SERVER_METADATA_FOLDER}" ]; then
    log "The server metadata is in folder: ${HOP_SERVER_METADATA_FOLDER}"
    echo "  <metadata_folder>${HOP_SERVER_METADATA_FOLDER}</metadata_folder>" >>${HOP_SERVER_XML}
  fi
  # The time (in minutes) it takes for a log line to be cleaned up in memory.
  #
  if [ -n "${HOP_SERVER_MAX_LOG_LINES}" ]; then
    log "The maximum amount of log lines kept is: ${HOP_SERVER_MAX_LOG_LINES}"
    echo "  <max_log_lines>${HOP_SERVER_MAX_LOG_LINES}</max_log_lines>" >>${HOP_SERVER_XML}
  fi
  # The time (in minutes) that log lines are kept in memory by the server
  #
  if [ -n "${HOP_SERVER_MAX_LOG_TIMEOUT}" ]; then
    log "Log lines timeout (in minutes) is: ${HOP_SERVER_MAX_LOG_TIMEOUT}"
    echo "  <max_log_timeout_minutes>${HOP_SERVER_MAX_LOG_TIMEOUT}</max_log_timeout_minutes>" >>${HOP_SERVER_XML}
  fi
  # The time (in minutes) it takes for a pipeline or workflow execution to be removed from the server status.
  #
  if [ -n "${HOP_SERVER_MAX_OBJECT_TIMEOUT}" ]; then
    log "Object timeout (in minutes) is: ${HOP_SERVER_MAX_OBJECT_TIMEOUT}"
    echo "  <object_timeout_minutes>${HOP_SERVER_MAX_OBJECT_TIMEOUT}</object_timeout_minutes>" >>${HOP_SERVER_XML}
  fi

  echo "</hop-server-config>" >>${HOP_SERVER_XML}
}

# retrieve files from volume
# ... done via Dockerfile via specifying a volume ...

# allow customisation
# e.g. to fetch hop project files from S3 or github
if [ -f "${HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH}" ]; then
  log "Sourcing custom entry point extension: ${HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH}"
  # shellcheck disable=SC1090
  source "${HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH}"
fi

# The common execution options for short and long lived containers
# The default log level is Basic
#
HOP_EXEC_OPTIONS="--level=${HOP_LOG_LEVEL}"

# For backward compatibility we'll still understand the HOP_PROJECT_DIRECTORY variable
#
if [ -z "${HOP_PROJECT_FOLDER}" ]; then
  if [ -n "${HOP_PROJECT_DIRECTORY}" ]; then
    log "Using deprecated option ${HOP_PROJECT_DIRECTORY} for option HOP_PROJECT_FOLDER"
    HOP_PROJECT_FOLDER="${HOP_PROJECT_DIRECTORY}"
  fi
fi

# If we need to set system properties
# By default this is not set
#
if [ -n "${HOP_SYSTEM_PROPERTIES}" ]; then
  log "Setting system properties at runtime: ${HOP_SYSTEM_PROPERTIES}"
  HOP_EXEC_OPTIONS="${HOP_EXEC_OPTIONS} --system-properties=${HOP_SYSTEM_PROPERTIES}"
fi

# If a project folder is defined we assume that we want to create it in the container
#
if [ -n "${HOP_PROJECT_FOLDER}" ]; then
  # We need the project folder to be set...
  #
  if [ -z "${HOP_PROJECT_NAME}" ]; then
    log "Error: please set variable HOP_PROJECT_NAME to create a project"
    exitWithCode 9
  else
    log "The project folder for ${HOP_PROJECT_NAME} is set to: ${HOP_PROJECT_FOLDER}"
  fi

  # The project folder should exist
  #
  if [ ! -d "${HOP_PROJECT_FOLDER}" ]; then
    log "Error: the folder specified in variable HOP_PROJECT_FOLDER does not exist: ${HOP_PROJECT_FOLDER}"
    exitWithCode 9
  else
    log "The specified project folder exists"
  fi

  log "Registering project ${HOP_PROJECT_NAME} in the Hop container configuration"
  log "${DEPLOYMENT_PATH}/hop/hop-conf.sh --project=${HOP_PROJECT_NAME} --project-create --project-home='${HOP_PROJECT_FOLDER}' --project-config-file='${HOP_PROJECT_CONFIG_FILE_NAME}'"

  if $("${DEPLOYMENT_PATH}"/hop/hop-conf.sh -pl | grep -q -E "^  ${HOP_PROJECT_NAME} :"); then
    log "project ${HOP_PROJECT_NAME} already exists"
  else
    "${DEPLOYMENT_PATH}"/hop/hop-conf.sh \
      --project="${HOP_PROJECT_NAME}" \
      --project-create \
      --project-home="${HOP_PROJECT_FOLDER}" \
      --project-config-file="${HOP_PROJECT_CONFIG_FILE_NAME}"
  fi

  HOP_EXEC_OPTIONS="${HOP_EXEC_OPTIONS} --project=${HOP_PROJECT_NAME}"

  # If we have environment files specified we want to create an environment as well:
  #
  if [ -n "${HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS}" ]; then

    if [ -z "${HOP_ENVIRONMENT_NAME}" ]; then
      log "Error: please set variable HOP_ENVIRONMENT_NAME to create an environment"
      exitWithCode 9
    fi

    log "Registering environment ${HOP_ENVIRONMENT_NAME} in the Hop container configuration"
    log "${DEPLOYMENT_PATH}/hop/hop-conf.sh --environment-create --environment=${HOP_ENVIRONMENT_NAME} --environment-project=${HOP_PROJECT_NAME} --environment-config-files='${HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS}'"

    if $("${DEPLOYMENT_PATH}"/hop/hop-conf.sh -el | grep -q -E -x "^  ${HOP_ENVIRONMENT_NAME}"); then
      log "environment ${HOP_ENVIRONMENT_NAME} already exists"
    else
      "${DEPLOYMENT_PATH}"/hop/hop-conf.sh \
        --environment="${HOP_ENVIRONMENT_NAME}" \
        --environment-create \
        --environment-project="${HOP_PROJECT_NAME}" \
        --environment-purpose="Apache Hop docker container" \
        --environment-config-files="${HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS}"
    fi

    HOP_EXEC_OPTIONS="${HOP_EXEC_OPTIONS} --environment=${HOP_ENVIRONMENT_NAME}"
  else
    log "Not creating an environment in the container"
  fi

else
  log "Not creating a project or environment in the container"
fi

if [ -z "${HOP_FILE_PATH}" ]; then
  write_server_config
  log "Starting a hop-server on port "${HOP_SERVER_PORT}
  "${DEPLOYMENT_PATH}"/hop/hop-server.sh \
    "${HOP_EXEC_OPTIONS}" \
    /tmp/hop-server.xml \
    2>&1 | tee ${HOP_LOG_PATH}

  exitWithCode "${PIPESTATUS[0]}"
else

  if [ -z "${HOP_RUN_CONFIG}" ]; then
    log "Please specify which run configuration you want to use to execute with variable HOP_RUN_CONFIG"
    exitWithCode 9
  fi

  log "Running a single hop workflow / pipeline (${HOP_FILE_PATH})"
  "${DEPLOYMENT_PATH}"/hop/hop-run.sh \
    --file="${HOP_FILE_PATH}" \
    --runconfig="${HOP_RUN_CONFIG}" \
    --parameters="${HOP_RUN_PARAMETERS}" \
    ${HOP_EXEC_OPTIONS} \
    2>&1 | tee "${HOP_LOG_PATH}"

  exitWithCode "${PIPESTATUS[0]}"
fi

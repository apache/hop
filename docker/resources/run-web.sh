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

# The common execution options for short and long lived containers
# The default log level is Basic
#
HOP_EXEC_OPTIONS="--level=${HOP_LOG_LEVEL}"

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
  log "${DEPLOYMENT_PATH}/hop-conf.sh --project=${HOP_PROJECT_NAME} --project-create --project-home='${HOP_PROJECT_FOLDER}' --project-config-file='${HOP_PROJECT_CONFIG_FILE_NAME}'"

  if $("${DEPLOYMENT_PATH}"/hop-conf.sh -pl | grep -q -E "^  ${HOP_PROJECT_NAME} :"); then
    log "project ${HOP_PROJECT_NAME} already exists"
  else
    "${DEPLOYMENT_PATH}"/hop-conf.sh \
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
    log "${DEPLOYMENT_PATH}/hop-conf.sh --environment-create --environment=${HOP_ENVIRONMENT_NAME} --environment-project=${HOP_PROJECT_NAME} --environment-config-files='${HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS}'"

    if $("${DEPLOYMENT_PATH}"/hop-conf.sh -el | grep -q -E -x "^  ${HOP_ENVIRONMENT_NAME}"); then
      log "environment ${HOP_ENVIRONMENT_NAME} already exists"
    else
      "${DEPLOYMENT_PATH}"/hop-conf.sh \
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


# if we have a /config/tomcat-users.xml file, copy it to the conf folder.
if [ -f "/config/tomcat-users.xml" ]; then
    log "copying users file to /usr/local/tomcat/conf/"
    cp /config/tomcat-users.xml /usr/local/tomcat/conf/
fi

# if we have a /config/web.xml file, copy it to the WEB-INF folder.
if [ -f "/config/web.xml" ]; then
    log "copying web.xml file to /usr/local/tomcat/conf/"
    cp /config/web.xml /usr/local/tomcat/webapps/ROOT/WEB-INF/
fi

#
# Stopping a running hop web container with 'docker stop' is obviously possible.
# Doing it with CTRL-C is just more convenient.
# So we'll start the catalina.sh script in the background and wait until
# we trap SIGINT or SIGTERM. At that point we'll simply stop Tomcat.
#

catalina.sh run &
pid="$!"
log "Running Apache Tomcat / Hop Web with PID ${pid}"
trap "log 'Stopping Tomcat'; catalina.sh stop" SIGINT SIGTERM

while kill -0 $pid > /dev/null 2>&1; do
    wait
done

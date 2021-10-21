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

set -Eeuo pipefail

BASENAME="${0##*/}"

log() {
    echo `date '+%Y/%m/%d %H:%M:%S'`" - ${1}"
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
    HOP_SERVER_MASTER=${HOP_SERVER_MASTER:-Y}
    HOP_SERVER_HOSTNAME=${HOP_SERVER_HOSTNAME:-0.0.0.0}

    HOP_SERVER_XML=/tmp/hop-server.xml

    log "Writing a hop-server config file to "${HOP_SERVER_XML}

    echo "<hop-server-config>" > ${HOP_SERVER_XML}
    echo "  <hop-server>" >> ${HOP_SERVER_XML}
    echo "    <name>Hop Server</name>" >> ${HOP_SERVER_XML}
    echo "    <hostname>${HOP_SERVER_HOSTNAME}</hostname>" >> ${HOP_SERVER_XML}
    echo "    <port>${HOP_SERVER_PORT}</port>" >> ${HOP_SERVER_XML}
    echo "    <username>${HOP_SERVER_USER}</username>" >> ${HOP_SERVER_XML}
    echo "    <password>${HOP_SERVER_PASS}</password>" >> ${HOP_SERVER_XML}
    echo "  </hop-server>" >> ${HOP_SERVER_XML}
    echo "</hop-server-config>" >> ${HOP_SERVER_XML}

}

# retrieve files from volume
# ... done via Dockerfile via specifying a volume ... 

# allow customisation
# e.g. to fetch hop project files from S3 or github
if test -f "${HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH}"; then
  echo "Sourcing custom entry point extension: ${HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH}"
  source ${HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH}
fi

if [ -z "${HOP_FILE_PATH}" ]
then

    write_server_config
    log "Starting a hop-server on port "${HOP_SERVER_PORT}
    ${DEPLOYMENT_PATH}/hop/hop-server.sh /tmp/hop-server.xml

else

  log "Registering project config with Hop"
  log "${DEPLOYMENT_PATH}/hop/hop-conf.sh --project=${HOP_PROJECT_NAME} --project-create --project-home='${HOP_PROJECT_DIRECTORY}' --project-config-file='${HOP_PROJECT_CONFIG_FILE_NAME}'"

  ${DEPLOYMENT_PATH}/hop/hop-conf.sh \
  --project=${HOP_PROJECT_NAME} \
  --project-create \
  --project-home="${HOP_PROJECT_DIRECTORY}" \
  --project-config-file="${HOP_PROJECT_CONFIG_FILE_NAME}"

  log "Registering environment config with Hop"
  log "${DEPLOYMENT_PATH}/hop/hop-conf.sh --environment-create --environment=${HOP_ENVIRONMENT_NAME} --environment-project=${HOP_PROJECT_NAME} --environment-config-files='${HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS}'"

  ${DEPLOYMENT_PATH}/hop/hop-conf.sh \
  --environment=${HOP_ENVIRONMENT_NAME} \
  --environment-create \
  --environment-project=${HOP_PROJECT_NAME} \
  --environment-config-files="${HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS}"

  log "Running a single hop workflow / pipeline (${HOP_FILE_PATH})"
  ${DEPLOYMENT_PATH}/hop/hop-run.sh \
    --file=${HOP_FILE_PATH} \
    --project=${HOP_PROJECT_NAME} \
    --environment=${HOP_ENVIRONMENT_NAME} \
    --runconfig=${HOP_RUN_CONFIG} \
    --level=${HOP_LOG_LEVEL} \
    --parameters=${HOP_RUN_PARAMETERS} \
    2>&1 | tee ${HOP_LOG_PATH}
fi
  

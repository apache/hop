#!/bin/bash
####################################################################### 
# "PROJECT_STARTUP_JOB"
# path to Kettle job from within volume
#
# "KETTLE_LOG_LEVEL"
# values are [Basic / Debug] 
#######################################################################

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

    log "Writing a hop-server config file to /tmp/hopserver.xml"

    echo "<slave_config><slaveserver><name>master</name><hostname>0.0.0.0</hostname><port>8080</port><master>${HOP_SERVER_MASTER}</master><username>${HOP_SERVER_USER}</username><password>${HOP_SERVER_PASS}</password></slaveserver></slave_config>" > /tmp/hopserver.xml
}

# retrieve files from volume
# ... done via Dockerfile via specifying a volume ... 


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

if [ -z "${HOP_FILE_PATH}" ]
then
    write_server_config
    log "Starting a hop-server on port 8080"
    ${DEPLOYMENT_PATH}/hop/hop-server.sh /tmp/hopserver.xml
else
    
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
  

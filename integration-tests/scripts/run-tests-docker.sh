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

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DOCKER_FILES_DIR="$(cd ${CURRENT_DIR}/../../docker/integration-tests/ && pwd)"

for ARGUMENT in "$@"
do

    KEY=$(echo $ARGUMENT | cut -f1 -d=)
    VALUE=$(echo $ARGUMENT | cut -f2 -d=)   

    case "$KEY" in
            PROJECT_NAME)    PROJECT_NAME=${VALUE} ;;
            JENKINS_USER)    REPOSITORY_NAME=${VALUE} ;;
            JENKINS_UID)    REPOSITORY_NAME=${VALUE} ;;
            JENKINS_GROUP)    REPOSITORY_NAME=${VALUE} ;;
            JENKINS_GID)    REPOSITORY_NAME=${VALUE} ;;
            *)   
    esac    


done


if [ -z "${PROJECT_NAME}" ]; then
    PROJECT_NAME="*"
fi

if [ -z "${JENKINS_USER}" ]; then
    JENKINS_USER="jenkins"
fi

if [ -z "${JENKINS_UID}" ]; then
    JENKINS_UID="1000"
fi

if [ -z "${JENKINS_GROUP}" ]; then
    JENKINS_GROUP="jenkins"
fi

if [ -z "${JENKINS_GID}" ]; then
    JENKINS_GID="1000"
fi

#Cleanup surefire reports
rm -rf "${CURRENT_DIR}"/../surefire-reports
mkdir -p "${CURRENT_DIR}"/../surefire-reports/

#Loop over project folders
for d in "${CURRENT_DIR}"/../${PROJECT_NAME}/ ; do

    if [[ "$d" != *"scripts/" ]] && [[ "$d" != *"surefire-reports/" ]] && [[ "$d" != *"hopweb/" ]]; then

        PROJECT_NAME=$(basename $d)

        echo "Project name: ${PROJECT_NAME}"
        echo "project path: $d"
        echo "docker compose path: ${DOCKER_FILES_DIR}"

        #Check if specific compose exists

        if [ -f "${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml" ]; then
            echo "Project compose exists."
            PROJECT_NAME=${PROJECT_NAME} docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml build --build-arg JENKINS_USER=${JENKINS_USER} --build-arg JENKINS_UID=${JENKINS_UID} --build-arg JENKINS_GROUP=${JENKINS_GROUP} --build-arg JENKINS_GID=${JENKINS_GID}
            PROJECT_NAME=${PROJECT_NAME} docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml up --abort-on-container-exit
            docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml down --rmi all
        else
            echo "Project compose does not exists."
            PROJECT_NAME=${PROJECT_NAME} docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-base.yaml build --build-arg JENKINS_USER=${JENKINS_USER} --build-arg JENKINS_UID=${JENKINS_UID} --build-arg JENKINS_GROUP=${JENKINS_GROUP} --build-arg JENKINS_GID=${JENKINS_GID}
            PROJECT_NAME=${PROJECT_NAME} docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-base.yaml up --abort-on-container-exit
            docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-base.yaml down
        fi
    fi
done

#Cleanup all images
docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-base.yaml down --rmi all --remove-orphans
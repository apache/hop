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

for ARGUMENT in "$@"; do

  KEY=$(echo $ARGUMENT | cut -f1 -d=)
  VALUE=$(echo $ARGUMENT | cut -f2 -d=)

  case "$KEY" in
  PROJECT_NAME) PROJECT_NAME=${VALUE} ;;
  JENKINS_USER) JENKINS_USER=${VALUE} ;;
  JENKINS_UID) JENKINS_UID=${VALUE} ;;
  JENKINS_GROUP) JENKINS_GROUP=${VALUE} ;;
  JENKINS_GID) JENKINS_GID=${VALUE} ;;
  GCP_KEY_FILE) GCP_KEY_FILE=${VALUE} ;;
  KEEP_IMAGES) KEEP_IMAGES=${VALUE} ;;
  *) ;;
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

if [ -z "${SUREFIRE_REPORT}" ]; then
  SUREFIRE_REPORT="true"
fi

if [ -z "${GCP_KEY_FILE}" ]; then
  GCP_KEY_FILE=""
fi

if [ -z "${KEEP_IMAGES}" ]; then
  KEEP_IMAGES="false"
fi

#Cleanup surefire reports
rm -rf "${CURRENT_DIR}"/../surefire-reports
mkdir -p "${CURRENT_DIR}"/../surefire-reports/

#Build base image only once
  docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-base.yaml build --build-arg JENKINS_USER=${JENKINS_USER} --build-arg JENKINS_UID=${JENKINS_UID} --build-arg JENKINS_GROUP=${JENKINS_GROUP} --build-arg JENKINS_GID=${JENKINS_GID} --build-arg GCP_KEY_FILE=${GCP_KEY_FILE}


#Loop over project folders
for d in "${CURRENT_DIR}"/../${PROJECT_NAME}/; do

  if [[ "$d" != *"scripts/" ]] && [[ "$d" != *"surefire-reports/" ]] && [[ "$d" != *"hopweb/" ]]; then
    # If there is a file called disabled.txt the project is disabled
    #
    if [ ! -f "$d/disabled.txt" ]; then

      PROJECT_NAME=$(basename $d)

      echo "Project name: ${PROJECT_NAME}"
      echo "project path: $d"
      echo "docker compose path: ${DOCKER_FILES_DIR}"

      #Check if specific compose exists

      if [ -f "${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml" ]; then
        echo "Project compose exists."
        PROJECT_NAME=${PROJECT_NAME} docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-${PROJECT_NAME}.yaml up --abort-on-container-exit
      else
        echo "Project compose does not exists."
        PROJECT_NAME=${PROJECT_NAME} docker-compose -f ${DOCKER_FILES_DIR}/integration-tests-base.yaml up --abort-on-container-exit
      fi
    fi
  fi

  #create final report
  if [ "${SUREFIRE_REPORT}" == "true" ]; then
    if  [ ! -f "${CURRENT_DIR}/../surefire-reports/surefile_${PROJECT_NAME}.xml" ]; then
          echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" >"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
          echo "<testsuite xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"https://maven.apache.org/surefire/maven-surefire-plugin/xsd/surefire-test-report-3.0.xsd\" version=\"3.0\" name=\"${PROJECT_NAME}\" time=\"0\" tests=\"0\" errors=\"0\" skipped=\"0\" failures=\"0\">" >>"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
          echo "</testsuite>" >>"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
    fi
  fi
done

echo "Keep images value: ${KEEP_IMAGES}"
#Cleanup all images
if [ ! "${KEEP_IMAGES}" == "true" ]; then
  for d in ${DOCKER_FILES_DIR}/integration-tests-*.yaml; do
    docker-compose --log-level ERROR -f $d down --rmi all --remove-orphans
  done
fi

#Print Final Results
if [ -f "${CURRENT_DIR}/../surefire-reports/passed_tests" ]; then
  echo -e "\033[1;32mPassed tests:"
  PASSED_TESTS="$(cat ../surefire-reports/passed_tests)"
  echo -e "\033[1;32m${PASSED_TESTS}"
fi
if [ -f "${CURRENT_DIR}/../surefire-reports/failed_tests" ]; then
    echo -e "\033[1;91mFailed tests:"
    FAILED_TESTS="$(cat ../surefire-reports/failed_tests)"
    echo -e "\033[1;91m${FAILED_TESTS}"
fi

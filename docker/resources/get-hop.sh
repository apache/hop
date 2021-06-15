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

### THIS SCRIPT IS NOT USED ANY MORE
# just left here in case we ever need it again

# Script used to fetch the latest snapshot version of project hop

set -ex

#Branch name variable
echo Branch Parameter: ${BRANCH_NAME}

# Artifactory location
SERVER=https://repository.apache.org/content/repositories

# Use Snapshot when branch is master else latest release
if [[ "${BRANCH_NAME}" = "master" ]]
then
    REPO=snapshots
else
    REPO=releases
fi

# Maven artifact location
NAME=hop-assemblies-client
ARTIFACT=org/apache/hop/${NAME}
URL_PATH=${SERVER}/${REPO}/${ARTIFACT}
VERSION=$( curl -s "${URL_PATH}/maven-metadata.xml" -o - | grep '<version>' | sed 's/.*<version>\([^<]*\)<\/version>.*/\1/' )
echo version: ${VERSION}
BUILD=$( curl -s "${URL_PATH}/${VERSION}/maven-metadata.xml" | grep '<value>' | head -1 | sed 's/.*<value>\([^<]*\)<\/value>.*/\1/' )
echo build: ${BUILD}

#If build is empty then use version (release)
if [ -z "$build" ]
then
    build=${VERSION}
fi

ZIP=${NAME}-${BUILD}.zip
URL=${URL_PATH}/${VERSION}/${ZIP}

# Download
echo ${URL}
curl -q -N ${URL} -o ${DEPLOYMENT_PATH}/hop.zip

# Unzip
unzip -q ${DEPLOYMENT_PATH}/hop.zip -d ${DEPLOYMENT_PATH}
chmod -R 700 ${DEPLOYMENT_PATH}/hop

# Cleanup
rm ${DEPLOYMENT_PATH}/hop.zip


#!/bin/bash
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


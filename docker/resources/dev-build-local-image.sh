#!/bin/zsh

# USE THIS FOR LOCAL DEV ONLY
cd ../../..
# create new build
mvn clean install
# unzip new build
unzip assemblies/client/target/hop-client-*.zip
# build docker image
LATEST_BUILD_VERSION=`date '+%Y%m%d%H%M%S'`
docker build . -f docker/Dockerfile -t apache-hop:${LATEST_BUILD_VERSION}

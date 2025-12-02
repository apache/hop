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

# set working dir to current location
#
cd "${0%/*}"

REST_TARGET=../rest/target/webapp

# unzip files for docker image
#
unzip -qu ../rest/target/hop-rest*.war -d ${REST_TARGET}
unzip -qu ../assemblies/plugins/dist/target/hop-assemblies-*.zip -d ../assemblies/plugins/dist/target/

# Copy recent changes in libraries.
#
echo "Copying Hop jar files from the target folders."
cp ../core/target/hop-core-*SNAPSHOT.jar ${REST_TARGET}/WEB-INF/lib/
cp ../engine/target/hop-engine-*SNAPSHOT.jar ${REST_TARGET}/WEB-INF/lib
cp ../ui/target/hop-ui-*SNAPSHOT.jar ${REST_TARGET}/WEB-INF/lib/
cp ../rap/target/hop-*SNAPSHOT.jar ${REST_TARGET}/WEB-INF/lib/

# Copy recent changes to a few plugins.
#
cp ../plugins/engines/beam/target/hop-plugins*.jar ../assemblies/plugins/dist/target/plugins/engines/beam/
cp ../plugins/misc/projects/target/hop-plugins*.jar ../assemblies/plugins/dist/target/plugins/misc/projects/

# Build the docker image.
#
docker build ../ -f rest.Dockerfile -t hop-rest # --progress=plain --no-cache

# Cleanup
#
# rm -rf ${REST_TARGET}/
# rm -rf ../assemblies/plugins/dist/target/plugins

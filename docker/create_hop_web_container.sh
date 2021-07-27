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

#set working dir to current location
cd "${0%/*}"

#unzip files for docker image
unzip ../assemblies/web/target/hop.war -d ../assemblies/web/target/webapp
unzip ../assemblies/plugins/dist/target/hop-assemblies-*.zip -d ../assemblies/plugins/dist/target/

#copy recent changes in libraries...
cp ../core/target/hop-core-*SNAPSHOT.jar ../assemblies/web/target/webapp/WEB-INF/lib/
cp ../engine/target/hop-engine-*SNAPSHOT.jar ../assemblies/web/target/webapp/WEB-INF/lib/
cp ../ui/target/hop-ui-*SNAPSHOT.jar ../assemblies/web/target/webapp/WEB-INF/lib/
cp ../rap/target/hop-*SNAPSHOT.jar ../assemblies/web/target/webapp/WEB-INF/lib/

#build docker image
docker build ../ -f Dockerfile.web -t hop-web

#cleanup
rm -rf ../assemblies/web/target/webapp
rm -rf ../assemblies/plugins/dist/target/plugins

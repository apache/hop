#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

echo "Starting docker deamon"

sudo systemctl start docker

echo "Starting Hop Web UI tests"

# make sure the chrome driver is executable.
chmod +x /home/hop/src/test/resources/chromedriver
if [ $? -eq 0 ]
then
  echo "Chrome driver is executable"
else
  echo "Failed to make sure Chrome driver is executable. Tests won't run"
  exit 1
fi

echo "Configuration file used for the tests:"
echo "======================================"
cat /home/hop/src/test/resources/hopwebtest.properties

# run the Selenium Hop Web tests
echo "Starting to run Hop Web UI tests"
mvn test

# copy the reports and generated images to the surefire volume
# the build will be marked as failed when there are failed tests, so don't bother checking the exit code.
echo "copying test reports"
sudo cp -r target/surefire-reports/* /surefire-reports
if [ -d "target/images" ] ; then
  echo "copying screenshots"
  sudo cp -r target/images /surefire-reports
fi

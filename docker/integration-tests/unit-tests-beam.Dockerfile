#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Beam variant of the integration-test image.
#
# The Beam-based run configurations (Spark, Flink, Google Dataflow) ship all of Hop to their
# workers as a single fat jar. Generating that fat jar takes a couple of minutes and produces a
# ~700MB file, so it is deliberately kept out of the shared base image (unit-tests.Dockerfile) and
# built here instead. This image is only built/used when a project that references the fat jar
# actually runs (see integration-tests-beam-base.yaml and scripts/run-tests-docker.sh).

FROM hop-base-image

# Runs as the (already configured) hop/jenkins user inherited from the base image, which owns the
# Hop installation under ${DEPLOYMENT_PATH}/hop.
RUN cd ${DEPLOYMENT_PATH}/hop \
  && ./hop-conf.sh --generate-fat-jar=/tmp/hop-fatjar.jar

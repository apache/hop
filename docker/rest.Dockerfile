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
#

FROM tomcat:10-jdk17-openjdk
LABEL maintainer="Apache Hop Team"
ENV HOP_CONFIG_FOLDER=""
ENV HOP_AES_ENCODER_KEY=""
ENV HOP_AUDIT_FOLDER="${CATALINA_HOME}/webapps/ROOT/audit"
ENV HOP_CONFIG_FOLDER="${CATALINA_HOME}/webapps/ROOT/config"
# specify the hop log level
ENV HOP_LOG_LEVEL="Basic"
# any JRE settings you want to pass on
# The “-XX:+AggressiveHeap” tells the container to use all memory assigned to the container. 
# this removed the need to calculate the necessary heap Xmx
ENV HOP_OPTIONS="-Xmx4g"
ENV HOP_PASSWORD_ENCODER_PLUGIN="Hop"
ENV HOP_PLUGIN_BASE_FOLDERS="plugins"
# path to jdbc drivers
ENV HOP_SHARED_JDBC_FOLDERS=""
ENV HOP_REST_CONFIG_FOLDER="/config"

# Set TOMCAT start variables
ENV CATALINA_OPTS='${HOP_OPTIONS} \
  -DHOP_AES_ENCODER_KEY="${HOP_AES_ENCODER_KEY}" \
  -DHOP_AUDIT_FOLDER="${HOP_AUDIT_FOLDER}" \
  -DHOP_CONFIG_FOLDER="${HOP_CONFIG_FOLDER}" \
  -DHOP_LOG_LEVEL="${HOP_LOG_LEVEL}" \
  -DHOP_PASSWORD_ENCODER_PLUGIN="${HOP_PASSWORD_ENCODER_PLUGIN}" \
  -DHOP_PLUGIN_BASE_FOLDERS="${HOP_PLUGIN_BASE_FOLDERS}" \
  -DHOP_REST_CONFIG_FOLDER="${HOP_REST_CONFIG_FOLDER}" \
  -DHOP_SHARED_JDBC_FOLDERS="${HOP_SHARED_JDBC_FOLDERS}"\'

# Cleanup and create folder
#
RUN    rm -rf webapps/*

# Copy resources
#
COPY ./assemblies/plugins/dist/target/plugins "${CATALINA_HOME}"/plugins
COPY ./rest/target/hop-rest*.war "${CATALINA_HOME}"/webapps/hop.war

# Copy the run script
#
COPY ./docker/resources/run-rest.sh /tmp/

RUN mkdir -p "$CATALINA_HOME"/lib/swt/linux/x86_64

CMD ["/bin/bash", "/tmp/run-rest.sh"]

#
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
# CastleMock 1.68 for the HTTP integration tests.
#
# Docker Hub removed castlemock/castlemock, and the v1.68 GitHub release no longer
# publishes castlemock.war. Build that war from the v1.68 tag and deploy it the same
# way the archived official image did: Tomcat 11, context /castlemock, port 8080,
# default user admin/admin. The HTTP tests import a project at
# http://castlemock:8080/castlemock/api/rest/core/project/soap/import.
#

FROM maven:3.9.11-eclipse-temurin-21 AS build

WORKDIR /src
ADD https://github.com/castlemock/castlemock/archive/refs/tags/v1.68.tar.gz /tmp/castlemock.tar.gz
RUN tar -xzf /tmp/castlemock.tar.gz -C /src --strip-components=1 \
    && mvn -B -DskipTests -pl deploy/deploy-tomcat/deploy-tomcat-war -am package

FROM tomcat:11.0.5-jdk21-temurin-noble

RUN rm -rf /usr/local/tomcat/webapps/ROOT \
    /usr/local/tomcat/webapps/docs \
    /usr/local/tomcat/webapps/examples \
    /usr/local/tomcat/webapps/manager \
    /usr/local/tomcat/webapps/host-manager
COPY --from=build /src/deploy/deploy-tomcat/deploy-tomcat-war/target/castlemock.war \
    /usr/local/tomcat/webapps/castlemock.war

EXPOSE 8080

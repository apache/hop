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

################################################################################
# Unified Multi-Stage Dockerfile for Apache Hop
# 
# This Dockerfile can build all Hop container images using multi-stage builds:
# - hop (client/server)
# - hop-web
# - hop-rest
# - hop-dataflow-template
#
# Build arguments:
#   HOP_BUILD_FROM_SOURCE: If "github", clones from GitHub; else uses local source
#   HOP_GIT_REPO: GitHub repository URL
#   HOP_GIT_TAG: Git tag/branch to build from
#   HOP_VERSION: Version string for labeling
#   TARGET_IMAGE: Which image to build (client, web, rest, dataflow)
#   BUILDER_TYPE: Builder flavor (full, fast)
################################################################################

# Global build arguments (must be declared before any FROM to use in FROM statements)
ARG BUILDER_TYPE=full

################################################################################
# Stage 1: Source preparation
################################################################################
FROM alpine:latest AS source-github
ARG HOP_GIT_REPO=https://github.com/apache/hop.git
ARG HOP_GIT_TAG=main

RUN apk add --no-cache git
WORKDIR /build
RUN git clone --depth 1 --branch ${HOP_GIT_TAG} ${HOP_GIT_REPO} hop

################################################################################
# Stage 2a: Full Maven Builder (slower, complete build)
################################################################################
FROM maven:3.9-eclipse-temurin-17 AS builder-full
ARG HOP_BUILD_FROM_SOURCE=local
ARG MAVEN_OPTS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1"
ARG MAVEN_THREADS="1C"

# Copy source - either from git clone or local context
# For GitHub builds, copy from the git clone stage
COPY --from=source-github /build/hop /tmp/github-hop

# For local builds, copy directly to /build first (more efficient)
COPY . /build

# If building from GitHub, replace /build with GitHub source
RUN if [ "${HOP_BUILD_FROM_SOURCE}" = "github" ]; then \
        rm -rf /build/* /build/.* 2>/dev/null || true; \
        cp -a /tmp/github-hop/. /build/; \
    fi

WORKDIR /build

# Verify pom.xml exists
RUN if [ ! -f "pom.xml" ]; then \
        echo "ERROR: pom.xml not found in /build"; \
        echo "Contents of /build:"; \
        ls -la /build/ || true; \
        exit 1; \
    fi

# Build with Maven - produces artifacts in target/ folders
RUN mvn clean install -T ${MAVEN_THREADS} -B -C -e -V \
    -Djacoco.skip=true \
    -Drat.skip=true \
    -Dcheckstyle.skip=true \
    -Dspotless.skip=true \
    -DskipTests=true \
    -Daether.syncContext.named.time=300 \
    --file pom.xml



################################################################################
# Stage 2b: Fast Builder (uses pre-built artifacts, for local dev)
################################################################################
# This builder skips Maven and just copies pre-built artifacts
# Use this when you've already run "mvn clean install" locally
# Expects the project to be built with artifacts in target/ folders
FROM alpine:latest AS builder-fast

WORKDIR /build

# Copy pre-built artifacts from context (must exist!)
COPY ./assemblies/client/target/hop-client-*.zip /build/assemblies/client/target/
COPY ./assemblies/web/target/hop.war /build/assemblies/web/target/
COPY ./assemblies/plugins/target/hop-assemblies-*.zip /build/assemblies/plugins/target/
COPY ./rest/target/hop-rest*.war /build/rest/target/
COPY ./docker/resources/ /build/docker/resources/

# builder-fast produces the same artifacts as builder-full:
# - /build/assemblies/client/target/hop-client-*.zip
# - /build/assemblies/web/target/hop.war
# - /build/assemblies/plugins/target/hop-assemblies-*.zip
# - /build/rest/target/hop-rest*.war
# - /build/docker/resources/*
#
# These will be extracted and prepared in Stage 3

################################################################################
# Stage 2c: Builder Selector
################################################################################
# This stage selects which builder to use based on BUILDER_TYPE build arg
# Default is "full" for complete Maven build
# NOTE: BUILDER_TYPE is declared globally at the top of this file
FROM builder-${BUILDER_TYPE} AS builder-selected

################################################################################
# Stage 3: Common Preparation Stage (ALL preparation logic in ONE place!)
################################################################################
# This stage:
# 1. Extracts/unzips artifacts from Stage 2
# 2. Generates fat jar
# 3. Prepares optimized directory structures for final images
#
# ANY new builder flavor just needs to produce the same artifacts as Stage 2,
# and this stage will handle everything else!
################################################################################
FROM alpine:latest AS builder

# Install tools needed for preparation
RUN apk add --no-cache unzip zip bash openjdk17-jre

WORKDIR /build

# Copy artifacts from selected builder
COPY --from=builder-selected /build/ /build/

# Step 1: Unzip and extract all assemblies
RUN echo "=== Extracting assemblies ===" && \
    # Unzip client assembly
    if [ -f assemblies/client/target/hop-client-*.zip ]; then \
        cd assemblies/client/target && \
        unzip -q hop-client-*.zip && \
        cd /build; \
    else \
        echo "ERROR: Client assembly not found" && exit 1; \
    fi && \
    # Unzip web assembly
    if [ -f assemblies/web/target/hop.war ]; then \
        cd assemblies/web/target && \
        unzip -q hop.war -d webapp && \
        cd /build; \
    else \
        echo "WARNING: Web assembly not found, skipping"; \
    fi && \
    # Unzip plugins assembly
    if [ -d assemblies/plugins/target ] && ls assemblies/plugins/target/hop-assemblies-*.zip 1> /dev/null 2>&1; then \
        cd assemblies/plugins/target && \
        unzip -q hop-assemblies-*.zip && \
        cd /build; \
    else \
        echo "WARNING: Plugins assembly not found, will use built plugins directly"; \
    fi

# Step 2: Generate fat jar for dataflow template
RUN if [ -f /build/assemblies/client/target/hop/hop-conf.sh ]; then \
        /build/assemblies/client/target/hop/hop-conf.sh \
        --generate-fat-jar=/tmp/hop-fatjar.jar; \
    else \
        echo "ERROR: hop-conf.sh not found" && exit 1; \
    fi

# Step 3: Prepare optimized directory structures for final images
RUN mkdir -p /build/hop-web-prepared/webapps/ROOT && \
    cp -r /build/assemblies/web/target/webapp/* /build/hop-web-prepared/webapps/ROOT/ && \
    cp -r /build/assemblies/client/target/hop/config /build/hop-web-prepared/webapps/ROOT/ && \
    cp -r /build/assemblies/client/target/hop/plugins /build/hop-web-prepared/ && \
    cp -r /build/assemblies/client/target/hop/lib/jdbc/ /build/hop-web-prepared/jdbc-drivers && \
    cp -r /build/assemblies/client/target/hop/lib/beam/* /build/hop-web-prepared/webapps/ROOT/WEB-INF/lib/ && \
    cp -r /build/assemblies/client/target/hop/lib/core/* /build/hop-web-prepared/webapps/ROOT/WEB-INF/lib/ && \
    rm /build/hop-web-prepared/webapps/ROOT/WEB-INF/lib/hop-ui-rcp* && \
    mkdir -p /build/hop-web-prepared/bin && \
    cp -r /build/docker/resources/run-web.sh /build/hop-web-prepared/bin/run-web.sh

# Make scripts executable
RUN chmod +x /build/hop-web-prepared/webapps/ROOT/*.sh \
    && chmod +x /build/hop-web-prepared/bin/run-web.sh

    # Fix hop-config.json
RUN sed -i 's/config\/projects/${HOP_CONFIG_FOLDER}\/projects/g' /build/hop-web-prepared/webapps/ROOT/config/hop-config.json

# Set the correct classpath for hop scripts
RUN sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' /build/hop-web-prepared/webapps/ROOT/hop-run.sh
RUN sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' /build/hop-web-prepared/webapps/ROOT/hop-conf.sh
RUN sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' /build/hop-web-prepared/webapps/ROOT/hop-search.sh
RUN sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' /build/hop-web-prepared/webapps/ROOT/hop-encrypt.sh
RUN sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' /build/hop-web-prepared/webapps/ROOT/hop-import.sh
RUN sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' /build/hop-web-prepared/webapps/ROOT/hop-search.sh


# Prepare Hop Client directory structure
RUN mkdir -p /build/hop-client-prepared && \
    # Copy entire hop installation
    cp -r /build/assemblies/client/target/hop/* /build/hop-client-prepared/ && \
    # Copy run scripts
    cp /build/docker/resources/run.sh /build/hop-client-prepared/run.sh && \
    cp /build/docker/resources/load-and-execute.sh /build/hop-client-prepared/load-and-execute.sh && \
    chmod +x /build/hop-client-prepared/run.sh /build/hop-client-prepared/load-and-execute.sh

# Prepare Hop REST directory structure
RUN mkdir -p /build/hop-rest-prepared/plugins && \
    mkdir -p /build/hop-rest-prepared/webapps && \
    mkdir -p /build/hop-rest-prepared/lib/swt/linux/x86_64 && \
    mkdir -p /build/hop-rest-prepared/bin && \
    # Copy plugins
    cp -r /build/assemblies/plugins/target/plugins/* /build/hop-rest-prepared/plugins/ && \
    # Copy REST war
    cp /build/rest/target/hop-rest*.war /build/hop-rest-prepared/webapps/hop.war && \
    # Copy run script
    cp /build/docker/resources/run-rest.sh /build/hop-rest-prepared/bin/run-rest.sh && \
    chmod +x /build/hop-rest-prepared/bin/run-rest.sh

################################################################################
# Stage 4a: Hop Client/Server Image (Standard)
################################################################################
FROM alpine:latest AS client

# Build arguments
ARG HOP_UID=501
ARG HOP_GID=501

# Environment variables
ENV DEPLOYMENT_PATH=/opt/hop
ENV VOLUME_MOUNT_POINT=/files
ENV HOP_LOG_LEVEL=Basic
ENV HOP_FILE_PATH=
ENV HOP_LOG_PATH=${DEPLOYMENT_PATH}/hop.err.log
ENV HOP_SHARED_JDBC_FOLDERS=
ENV HOP_PROJECT_NAME=
ENV HOP_PROJECT_DIRECTORY=
ENV HOP_PROJECT_FOLDER=
ENV HOP_PROJECT_CONFIG_FILE_NAME=project-config.json
ENV HOP_ENVIRONMENT_NAME=
ENV HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS=
ENV HOP_RUN_CONFIG=
ENV HOP_RUN_PARAMETERS=
ENV HOP_START_ACTION=
ENV HOP_RUN_METADATA_EXPORT=
ENV HOP_SYSTEM_PROPERTIES=
ENV HOP_OPTIONS=-XX:+AggressiveHeap
ENV HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH=
ENV HOP_SERVER_USER=cluster
ENV HOP_SERVER_PASSWORD=cluster
ENV HOP_SERVER_HOSTNAME=0.0.0.0
ENV HOP_SERVER_PORT=8080
ENV HOP_SERVER_SHUTDOWNPORT=8079
ENV HOP_SERVER_METADATA_FOLDER=
ENV HOP_SERVER_KEYSTORE=
ENV HOP_SERVER_KEYSTORE_PASSWORD=
ENV HOP_SERVER_KEY_PASSWORD=
ENV HOP_SERVER_MAX_LOG_LINES=
ENV HOP_SERVER_MAX_LOG_TIMEOUT=
ENV HOP_SERVER_MAX_OBJECT_TIMEOUT=
ENV HOP_CONFIG_OPTIONS=

# Install required packages
RUN addgroup -g ${HOP_GID} -S hop \
    && adduser -u ${HOP_UID} -S -D -G hop hop \
    && chmod 777 -R /tmp && chmod o+t -R /tmp \
    && apk update \
    && apk --no-cache add bash curl fontconfig msttcorefonts-installer openjdk17-jre procps \
    && update-ms-fonts \
    && fc-cache -f \
    && rm -rf /var/cache/apk/* \
    && mkdir ${DEPLOYMENT_PATH} \
    && mkdir ${VOLUME_MOUNT_POINT} \
    && chown hop:hop ${DEPLOYMENT_PATH} \
    && chown hop:hop ${VOLUME_MOUNT_POINT}

# Copy pre-assembled Hop Client structure from builder (SINGLE layer!)
COPY --from=builder --chown=hop:hop /build/hop-client-prepared/ ${DEPLOYMENT_PATH}/

# Expose ports
EXPOSE 8080 8079

# Configure
VOLUME ["/files"]
USER hop
ENV PATH=${PATH}:${DEPLOYMENT_PATH}/hop
WORKDIR /home/hop

CMD bash -c "$DEPLOYMENT_PATH/run.sh"

################################################################################
# Stage 4b: Hop Web Image
################################################################################
FROM tomcat:10-jdk17 AS web

# Build arguments
ARG HOP_UID=501
ARG HOP_GID=501

# Environment variables
ENV DEPLOYMENT_PATH=/usr/local/tomcat/webapps/ROOT
ENV HOP_AES_ENCODER_KEY=""
ENV HOP_AUDIT_FOLDER="${CATALINA_HOME}/webapps/ROOT/audit"
ENV HOP_CONFIG_FOLDER="${CATALINA_HOME}/webapps/ROOT/config"
ENV HOP_LOG_LEVEL="Basic"
ENV HOP_OPTIONS="-XX:+AggressiveHeap -Dorg.eclipse.rap.rwt.resourceLocation=/tmp/rwt-resources"
ENV HOP_PASSWORD_ENCODER_PLUGIN="Hop"
ENV HOP_PLUGIN_BASE_FOLDERS=${CATALINA_HOME}/plugins
ENV HOP_SHARED_JDBC_FOLDERS="${CATALINA_HOME}/jdbc-drivers"
ENV HOP_WEB_THEME="light"
ENV HOP_GUI_ZOOM_FACTOR=1.0
ENV HOP_PROJECT_FOLDER=
ENV HOP_PROJECT_CONFIG_FILE_NAME=project-config.json
ENV HOP_ENVIRONMENT_NAME=environment1
ENV HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS=

# Set TOMCAT start variables
ENV CATALINA_OPTS='${HOP_OPTIONS} \
  -DHOP_AES_ENCODER_KEY="${HOP_AES_ENCODER_KEY}" \
  -DHOP_AUDIT_FOLDER="${HOP_AUDIT_FOLDER}" \
  -DHOP_CONFIG_FOLDER="${HOP_CONFIG_FOLDER}" \
  -DHOP_LOG_LEVEL="${HOP_LOG_LEVEL}" \
  -DHOP_PASSWORD_ENCODER_PLUGIN="${HOP_PASSWORD_ENCODER_PLUGIN}" \
  -DHOP_PLUGIN_BASE_FOLDERS="${HOP_PLUGIN_BASE_FOLDERS}" \
  -DHOP_SHARED_JDBC_FOLDERS="${HOP_SHARED_JDBC_FOLDERS}" \
  -DHOP_WEB_THEME="${HOP_WEB_THEME}" \
  -DHOP_GUI_ZOOM_FACTOR="${HOP_GUI_ZOOM_FACTOR}"'

# Create Hop user
RUN groupadd -r hop -g ${HOP_GID} \
    && useradd -d /home/hop -u ${HOP_UID} -m -s /bin/bash -g hop hop \
    && rm -rf webapps/* \
    && mkdir "${CATALINA_HOME}"/webapps/ROOT \
    && mkdir "${HOP_AUDIT_FOLDER}" \
    && chown -R hop:hop /usr/local/tomcat

# Copy resources (matching original Dockerfile.web layer structure)
COPY --from=builder --chown=hop /build/hop-web-prepared/ "${CATALINA_HOME}"

USER hop

CMD bash -c "$CATALINA_HOME/bin/run-web.sh"


################################################################################
# Stage 4c: Hop Dataflow Template Image
################################################################################
FROM gcr.io/dataflow-templates-base/java17-template-launcher-base AS dataflow

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Copy fat jar from builder
COPY --from=builder /tmp/hop-fatjar.jar ./

ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.apache.hop.beam.run.MainDataflowTemplate"
ENV FLEX_TEMPLATE_JAVA_CLASSPATH="${WORKDIR}/*"

ENTRYPOINT ["/opt/google/dataflow/java_template_launcher"]

################################################################################
# Image Variants/Flavors
################################################################################
# These stages extend base stages to create variants with additional features
# Stage names follow pattern: <base>-<variant> (e.g., web-beam, client-minimal)
################################################################################

################################################################################
# Stage 4e: Hop Web with Beam (includes fat jar for Dataflow)
################################################################################
FROM web AS web-beam
LABEL variant="beam"

# Copy fat jar from builder into web image for Dataflow integration
COPY --from=builder /tmp/hop-fatjar.jar /root/hop-fatjar.jar
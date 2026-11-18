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

FROM tomcat:10-jdk21
LABEL maintainer="Apache Hop Team"
# The UID for the created user
ARG HOP_UID=501
# The GID for the created user
ARG HOP_GID=501
# path to where the artifacts should be deployed to
ENV DEPLOYMENT_PATH=/usr/local/tomcat/webapps/ROOT
ENV HOP_AES_ENCODER_KEY=""
ENV HOP_AES_ENCODER_KEY_FILE=""
# Writable by the hop user without a bind mount (per-user data under users/<name>/)
ENV HOP_AUDIT_FOLDER="/tmp/hop-web-audit"
ENV HOP_CONFIG_FOLDER="${CATALINA_HOME}/webapps/ROOT/config"
# specify the hop log level
ENV HOP_LOG_LEVEL="Basic"
# any JRE settings you want to pass on
# The “-XX:+AggressiveHeap” tells the container to use all memory assigned to the container. 
# this removed the need to calculate the necessary heap Xmx
ENV HOP_OPTIONS="-XX:+AggressiveHeap"
ENV HOP_PASSWORD_ENCODER_PLUGIN="Hop"
ENV HOP_PLUGIN_BASE_FOLDERS="plugins"
# path to jdbc drivers
ENV HOP_SHARED_JDBC_FOLDERS="${CATALINA_HOME}/jdbc-drivers"
ENV HOP_GUI_ZOOM_FACTOR=1.0

ENV HOP_PROJECT_FOLDER=
# name of the project config file including file extension
ENV HOP_PROJECT_CONFIG_FILE_NAME=project-config.json
# optional one-level parent project (see issue #2596)
ENV HOP_PARENT_PROJECT_NAME=
ENV HOP_PARENT_PROJECT_FOLDER=
ENV HOP_PARENT_PROJECT_CONFIG_FILE_NAME=project-config.json
# environment to use with hop run
ENV HOP_ENVIRONMENT_NAME=environment1
# comma separated list of paths to environment config files (including filename and file extension).
ENV HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS=
# Metadata export JSON itself (the document hop-conf --export-metadata writes).
# Imported into the shared metadata folder before a project is registered.
# No file and no volume: put the document in this variable. Leave empty to skip.
ENV HOP_SHARED_METADATA_IMPORT_JSON=
# Path of that same metadata export when it is already a file in the container.
# Set only one of HOP_SHARED_METADATA_IMPORT_JSON and HOP_SHARED_METADATA_IMPORT_FILE.
ENV HOP_SHARED_METADATA_IMPORT_FILE=
# hop run configuration to use


# Set TOMCAT start variables
ENV CATALINA_OPTS='${HOP_OPTIONS} \
  -DHOP_AES_ENCODER_KEY="${HOP_AES_ENCODER_KEY}" \
  -DHOP_AES_ENCODER_KEY_FILE="${HOP_AES_ENCODER_KEY_FILE}" \
  -DHOP_AUDIT_FOLDER="${HOP_AUDIT_FOLDER}" \
  -DHOP_CONFIG_FOLDER="${HOP_CONFIG_FOLDER}" \
  -DHOP_LOG_LEVEL="${HOP_LOG_LEVEL}" \
  -DHOP_PASSWORD_ENCODER_PLUGIN="${HOP_PASSWORD_ENCODER_PLUGIN}" \
  -DHOP_PLUGIN_BASE_FOLDERS="${HOP_PLUGIN_BASE_FOLDERS}" \
  -DHOP_SHARED_JDBC_FOLDERS="${HOP_SHARED_JDBC_FOLDERS}" \
  -DHOP_GUI_ZOOM_FACTOR="${HOP_GUI_ZOOM_FACTOR}"'

# Create Hop user
# fonts-noto-cjk: the canvas is painted server-side; without a CJK font the JVM measures
# Chinese/Japanese/Korean names as missing-glyph boxes and lays them out too narrow (#8528)
RUN apt-get update \
    && apt-get install -y --no-install-recommends fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r hop -g ${HOP_GID} \
    && useradd -d /home/hop -u ${HOP_UID} -m -s /bin/bash -g hop hop \
    && rm -rf webapps/* \
    && mkdir "${CATALINA_HOME}"/webapps/ROOT \
    && mkdir -p "${HOP_AUDIT_FOLDER}" \
    && chown hop:hop "${HOP_AUDIT_FOLDER}"

# Hop Web as assemblies/web-war lays it out (the same content as hop-web-<version>.war and
# hop-web-<version>-plugins.zip): the web app, then plugins/ and jdbc-drivers/ into CATALINA_HOME.
COPY ./assemblies/web-war/target/webapp/ "${CATALINA_HOME}"/webapps/ROOT/
COPY ./assemblies/web-war/target/tomcat/ "${CATALINA_HOME}"/
# Marketplace-optional plugins, when tools/install-wave1-plugins.sh installed them into
# target/optional (Jenkins does): their plugins, their jars under lib/core (the Beam SDKs, for
# one) into WEB-INF/lib, their JDBC drivers and their sample projects. The folders exist, empty,
# after every build.
COPY ./assemblies/web-war/target/optional/plugins/ "${CATALINA_HOME}"/plugins/
COPY ./assemblies/web-war/target/optional/lib/core/ "${CATALINA_HOME}"/webapps/ROOT/WEB-INF/lib/
COPY ./assemblies/web-war/target/optional/lib/jdbc/ "${CATALINA_HOME}"/jdbc-drivers/
COPY ./assemblies/web-war/target/optional/config/ "${CATALINA_HOME}"/webapps/ROOT/config/
COPY --chown=hop ./docker/resources/run-web.sh /tmp/
# Tomcat configuration with response compression (see the comments in the file)
COPY ./docker/resources/server.xml "${CATALINA_HOME}"/conf/server.xml

RUN mkdir -p "$CATALINA_HOME"/lib/swt/linux/x86_64

RUN chmod +x ${CATALINA_HOME}/webapps/ROOT/hop ${CATALINA_HOME}/webapps/ROOT/*.sh

# point to the plugins folder
ENV HOP_PLUGIN_BASE_FOLDERS=$CATALINA_HOME/plugins

# Set permissions on project folder
RUN chown -R hop:hop /usr/local/tomcat

USER hop

CMD ["/bin/bash", "/tmp/run-web.sh"]

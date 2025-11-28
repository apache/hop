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

FROM tomcat:10-jdk17
LABEL maintainer="Apache Hop Team"
# The UID for the created user
ARG HOP_UID=501
# The GID for the created user
ARG HOP_GID=501
# path to where the artifacts should be deployed to
ENV DEPLOYMENT_PATH=/usr/local/tomcat/webapps/ROOT
ENV HOP_AES_ENCODER_KEY=""
ENV HOP_AUDIT_FOLDER="${CATALINA_HOME}/webapps/ROOT/audit"
ENV HOP_CONFIG_FOLDER="${CATALINA_HOME}/webapps/ROOT/config"
# specify the hop log level
ENV HOP_LOG_LEVEL="Basic"
# any JRE settings you want to pass on
# The “-XX:+AggressiveHeap” tells the container to use all memory assigned to the container. 
# this removed the need to calculate the necessary heap Xmx
ENV HOP_OPTIONS="-XX:+AggressiveHeap -Dorg.eclipse.rap.rwt.resourceLocation=/tmp/rwt-resources"
ENV HOP_PASSWORD_ENCODER_PLUGIN="Hop"
ENV HOP_PLUGIN_BASE_FOLDERS="plugins"
# path to jdbc drivers
ENV HOP_SHARED_JDBC_FOLDERS="${CATALINA_HOME}/jdbc-drivers"
ENV HOP_WEB_THEME="light"
ENV HOP_GUI_ZOOM_FACTOR=1.0

ENV HOP_PROJECT_FOLDER=
# name of the project config file including file extension
ENV HOP_PROJECT_CONFIG_FILE_NAME=project-config.json
# environment to use with hop run
ENV HOP_ENVIRONMENT_NAME=environment1
# comma separated list of paths to environment config files (including filename and file extension).
ENV HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS=
# hop run configuration to use


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
    && mkdir "${HOP_AUDIT_FOLDER}"

# Copy resources
COPY ./assemblies/web/target/webapp/ "${CATALINA_HOME}"/webapps/ROOT/
COPY ./assemblies/client/target/hop/config "${CATALINA_HOME}"/webapps/ROOT/config
COPY ./assemblies/client/target/hop/lib/core "${CATALINA_HOME}"/webapps/ROOT/WEB-INF/lib
COPY ./assemblies/client/target/hop/lib/beam "${CATALINA_HOME}"/webapps/ROOT/WEB-INF/lib
COPY ./assemblies/client/target/hop/plugins "${CATALINA_HOME}"/plugins
COPY ./assemblies/client/target/hop/lib/jdbc/ "${CATALINA_HOME}"/jdbc-drivers
COPY --chown=hop ./docker/resources/run-web.sh /tmp/

# Fix hop-config.json
RUN sed -i 's/config\/projects/${HOP_CONFIG_FOLDER}\/projects/g' "${CATALINA_HOME}"/webapps/ROOT/config/hop-config.json

RUN mkdir -p "$CATALINA_HOME"/lib/swt/linux/x86_64

# set the correct classpath for hop-conf and hop-run
RUN  sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' ${CATALINA_HOME}/webapps/ROOT/hop-run.sh
RUN  sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' ${CATALINA_HOME}/webapps/ROOT/hop-conf.sh
RUN  sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' ${CATALINA_HOME}/webapps/ROOT/hop-search.sh
RUN  sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' ${CATALINA_HOME}/webapps/ROOT/hop-encrypt.sh
RUN  sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' ${CATALINA_HOME}/webapps/ROOT/hop-import.sh
RUN  sed -i 's&lib/core/*&../../lib/*:WEB-INF/lib/*:lib/core/*&g' ${CATALINA_HOME}/webapps/ROOT/hop-search.sh

RUN chmod +x ${CATALINA_HOME}/webapps/ROOT/*.sh

# point to the plugins folder
ENV HOP_PLUGIN_BASE_FOLDERS=$CATALINA_HOME/plugins

# Set permissions on project folder
RUN chown -R hop:hop /usr/local/tomcat

USER hop

CMD ["/bin/bash", "/tmp/run-web.sh"]

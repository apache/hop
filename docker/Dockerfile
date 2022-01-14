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

FROM adoptopenjdk/openjdk8:alpine-slim
LABEL maintainer="Apache Hop Team"
# path to where the artifacts should be deployed to
ENV DEPLOYMENT_PATH=/opt/hop
# volume mount point
ENV VOLUME_MOUNT_POINT=/files
# parent directory in which the hop config artifacts live
# ENV HOP_HOME= ...
# specify the hop log level
ENV HOP_LOG_LEVEL=Basic
# path to hop workflow or pipeline e.g. ~/project/main.hwf
ENV HOP_FILE_PATH=
# file path to hop log file, e.g. ~/hop.err.log
ENV HOP_LOG_PATH=$DEPLOYMENT_PATH/hop.err.log
# path to jdbc drivers
ENV HOP_SHARED_JDBC_FOLDER=
# name of the Hop project to use
ENV HOP_PROJECT_NAME=project1
# path to the home of the Hop project..
ENV HOP_PROJECT_DIRECTORY=
ENV HOP_PROJECT_FOLDER=
# name of the project config file including file extension
ENV HOP_PROJECT_CONFIG_FILE_NAME=project-config.json
# environment to use with hop run
ENV HOP_ENVIRONMENT_NAME=environment1
# comma separated list of paths to environment config files (including filename and file extension).
ENV HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS=
# hop run configuration to use
ENV HOP_RUN_CONFIG=
# parameters that should be passed on to the hop-run command
# specify as comma separated list, e.g. PARAM_1=aaa,PARAM_2=bbb
ENV HOP_RUN_PARAMETERS=
# System properties that should be set
# specify as comma separated list, e.g. PROP1=xxx,PROP2=yyy
ENV HOP_SYSTEM_PROPERTIES=
# any JRE settings you want to pass on
# The “-XX:+AggressiveHeap” tells the container to use all memory assigned to the container. 
# this removed the need to calculate the necessary heap Xmx
ENV HOP_OPTIONS=-XX:+AggressiveHeap
# Path to custom entrypoint extension script file - optional
# e.g. to fetch Hop project files from S3 or gitlab
ENV HOP_CUSTOM_ENTRYPOINT_EXTENSION_SHELL_FILE_PATH=
# The server user
ENV HOP_SERVER_USER=cluster
# The server password
ENV HOP_SERVER_PASSWORD=cluster
# The server hostname
ENV HOP_SERVER_HOSTNAME=0.0.0.0
ENV HOP_SERVER_PORT=8080
# Optional metadata folder to be included in the hop server XML
ENV HOP_SERVER_METADATA_FOLDER=
# Optional server SSL configuration variables
ENV HOP_SERVER_KEYSTORE=
ENV HOP_SERVER_KEYSTORE_PASSWORD=
ENV HOP_SERVER_KEY_PASSWORD=
# Memory optimization options for the server
ENV HOP_SERVER_MAX_LOG_LINES=
ENV HOP_SERVER_MAX_LOG_TIMEOUT=
ENV HOP_SERVER_MAX_OBJECT_TIMEOUT=

# Define en_US.
# ENV LANGUAGE en_US.UTF-8
# ENV LANG en_US.UTF-8
# ENV LC_ALL en_US.UTF-8
# ENV LC_CTYPE en_US.UTF-8
# ENV LC_MESSAGES en_US.UTF-8

# Give the /tmp folder some breathing room
#
RUN chmod 777 -R /tmp && chmod o+t -R /tmp

# Install some fonts
RUN apk --no-cache add msttcorefonts-installer \
        fontconfig \
        font-noto \
        font-noto-adlam \
        font-noto-adlamunjoined \
        font-noto-arabic \
        font-noto-armenian \
        font-noto-avestan \
        font-noto-bamum \
        font-noto-bengali \
        font-noto-buhid \
        font-noto-carian \
        font-noto-chakma \
        font-noto-cherokee && \
    update-ms-fonts && \
    fc-cache -f

# INSTALL REQUIRED PACKAGES AND ADJUST LOCALE
# procps: The package includes the programs ps, top, vmstat, w, kill, free, slabtop, and skill


RUN apk update \
  && apk add --no-cache bash curl procps \
  && rm -rf /var/cache/apk/* \
  && mkdir ${DEPLOYMENT_PATH} \
  && mkdir ${VOLUME_MOUNT_POINT} \
  && adduser -D -s /bin/bash -h /home/hop hop \
  && chown hop:hop ${DEPLOYMENT_PATH} \
  && chown hop:hop ${VOLUME_MOUNT_POINT}


# copy the hop package from the local resources folder to the container image directory
COPY --chown=hop:hop ./assemblies/client/target/hop/ ${DEPLOYMENT_PATH}/hop
COPY --chown=hop:hop ./docker/resources/run.sh ${DEPLOYMENT_PATH}/run.sh
COPY --chown=hop:hop ./docker/resources/load-and-execute.sh ${DEPLOYMENT_PATH}/load-and-execute.sh

# expose 8080 for Hop Server
EXPOSE 8080

# make volume available so that hop pipeline and workflow files can be provided easily
VOLUME ["/files"]
USER hop
ENV PATH=$PATH:${DEPLOYMENT_PATH}/hop
WORKDIR /home/hop
# CMD ["/bin/bash"]
ENTRYPOINT ["/bin/bash", "/opt/hop/run.sh"]

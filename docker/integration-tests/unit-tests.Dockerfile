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

FROM ubuntu:22.04
LABEL maintainer="Apache Hop Team"

# Argument Branch name, used to download correct version
ARG BRANCH_NAME
ENV BRANCH_NAME=$BRANCH_NAME
# path to where the artefacts should be deployed to
ENV DEPLOYMENT_PATH=/opt
# volume mount point
ENV VOLUME_MOUNT_POINT=/files
#Jenkins user an group
ARG JENKINS_USER=hop
ARG JENKINS_GROUP=hop
ARG JENKINS_UID=1000
ARG JENKINS_GID=1000
# Set system properties
ENV DEBIAN_FRONTEND=noninteractive

# any JRE settings you want to pass on
# The “-XX:+AggressiveHeap” tells the container to use all memory assigned to the container. 
# this removed the need to calculate the necessary heap Xmx
ENV HOP_OPTIONS=-XX:+AggressiveHeap

# Set Locale correctly
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# INSTALL REQUIRED PACKAGES AND ADJUST LOCALE
# procps: The package includes the programs ps, top, vmstat, w, kill, free, slabtop, and skill

# Copy the hop package from the local resources folder to the container image directory

RUN apt-get update \
  && apt-get install --assume-yes \
  bash \
  curl \
  procps \
  git \
  python3-pip \
  openjdk-21-jre-headless \
  unzip \
  ttf-mscorefonts-installer \
  locales \
  && apt-get clean \
  && sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen  \
  && locale-gen \
  # Install parquet-tools from Python
  && pip3 install parquet-tools

# Create the container identity from the host UID/GID that run-tests-docker.sh passes in, so
# writes into the bind-mounted integration-tests/ tree land as the workspace owner.
# Those host IDs regularly collide with distro accounts — macOS hands over staff/20, but on
# Ubuntu GID 20 is `dialout` and its own `staff` group sits at GID 50 — so reuse whatever
# already holds the requested ID instead of failing the build. Ownership is applied
# numerically everywhere below for the same reason: the group *name* may resolve to a
# different GID than the one the host asked for.
RUN set -eu; \
  mkdir -p ${VOLUME_MOUNT_POINT}; \
  if ! getent group "${JENKINS_GID}" >/dev/null; then \
    groupadd -g "${JENKINS_GID}" "${JENKINS_GROUP}" 2>/dev/null \
      || groupadd -g "${JENKINS_GID}" "hopgrp${JENKINS_GID}"; \
  fi; \
  if getent passwd "${JENKINS_UID}" >/dev/null; then \
    existing_user="$(getent passwd "${JENKINS_UID}" | cut -d: -f1)"; \
    if [ "${existing_user}" != "${JENKINS_USER}" ]; then \
      usermod -l "${JENKINS_USER}" "${existing_user}"; \
    fi; \
    usermod -d "/home/${JENKINS_USER}" -g "${JENKINS_GID}" "${JENKINS_USER}"; \
  elif getent passwd "${JENKINS_USER}" >/dev/null; then \
    usermod -u "${JENKINS_UID}" -g "${JENKINS_GID}" -d "/home/${JENKINS_USER}" "${JENKINS_USER}"; \
  else \
    useradd -m -d "/home/${JENKINS_USER}" -u "${JENKINS_UID}" -g "${JENKINS_GID}" "${JENKINS_USER}"; \
  fi; \
  mkdir -p "/home/${JENKINS_USER}"; \
  chown ${JENKINS_UID}:${JENKINS_GID} "/home/${JENKINS_USER}" ${DEPLOYMENT_PATH} ${VOLUME_MOUNT_POINT}


COPY --chown=${JENKINS_UID}:${JENKINS_GID} ./assemblies/client/target/hop ${DEPLOYMENT_PATH}/hop

# Wave 1 optional plugins (marketplace) — expect host to have run tools/install-wave1-plugins.sh
# into assemblies/client/target/hop before this build (run-tests-docker.sh and Jenkins do this).
# If a plugin is still missing at runtime, corresponding ITs will fail clearly.

# Placeholder GCP key. The real service-account JSON is never baked into the image: it is
# bind-mounted over this path at run time (see integration-tests-base.yaml + run-tests-docker.sh),
# so the image is identical whether it is built explicitly, implicitly by "compose up", or from
# cache. Baking it in via a build-arg made the image content depend on how it got built.
COPY --chown=${JENKINS_UID}:${JENKINS_GID} ./docker/integration-tests/resource/dummyfile /tmp/google-key-apache-hop-it.json

# Copy mail keystore
COPY --chown=${JENKINS_UID}:${JENKINS_GID} ./docker/integration-tests/resource/mail/conf/keystore /tmp

# Unzip and install in correct location
RUN chown -R ${JENKINS_UID}:${JENKINS_GID} ${DEPLOYMENT_PATH}/hop \
  && chmod 700 ${DEPLOYMENT_PATH}/hop/*.sh

# make volume available so that hop pipeline and workflow files can be provided easily
VOLUME ["/files"]
USER ${JENKINS_USER}
ENV PATH=$PATH:${DEPLOYMENT_PATH}/hop
ENV GOOGLE_APPLICATION_CREDENTIALS="/tmp/google-key-apache-hop-it.json"
ENV HOP_OPTIONS="${HOP_OPTIONS}"
WORKDIR /home/${JENKINS_USER}
# CMD ["/bin/bash"]
ENTRYPOINT []

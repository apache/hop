FROM openjdk:8-alpine
LABEL maintainer="Apache Hop Team"
# Argument Branch name, used to download correct version
ARG BRANCH_NAME
ENV BRANCH_NAME=$BRANCH_NAME
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
# path to hop config directory
# ENV /files/project= DISABLED for now 
# path to jdbc drivers
ENV HOP_SHARED_JDBC_DIRECTORY=
# name of the Hop project to use
ENV HOP_PROJECT_NAME=
# path to the home of the hop project. should start with `/files`.
ENV HOP_PROJECT_DIRECTORY=
# name of the project config file including file extension
ENV HOP_PROJECT_CONFIG_FILE_NAME=project-config.json
# environment to use with hop run
ENV HOP_ENVIRONMENT_NAME=
# comma separated list of paths to environment config files (including filename and file extension). paths should start with `/files`.
ENV HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS=
# hop run configuration to use
ENV HOP_RUN_CONFIG=
# parameters that should be passed on to the hop-run command
# specify as comma separated list, e.g. PARAM_1=aaa,PARAM_2=bbb
ENV HOP_RUN_PARAMETERS=
# any JRE settings you want to pass on
# The “-XX:+AggressiveHeap” tells the container to use all memory assigned to the container. 
# this removed the need to calculate the necessary heap Xmx
ENV HOP_OPTIONS=-XX:+AggressiveHeap

# Define en_US.
# ENV LANGUAGE en_US.UTF-8
# ENV LANG en_US.UTF-8
# ENV LC_ALL en_US.UTF-8
# ENV LC_CTYPE en_US.UTF-8
# ENV LC_MESSAGES en_US.UTF-8

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
COPY --chown=hop:hop ./resources/get-hop.sh ${DEPLOYMENT_PATH}/get-hop.sh
COPY --chown=hop:hop ./resources/run.sh ${DEPLOYMENT_PATH}/run.sh
COPY --chown=hop:hop ./resources/load-and-execute.sh ${DEPLOYMENT_PATH}/load-and-execute.sh


# Fetch the specified hop version 
RUN ${DEPLOYMENT_PATH}/get-hop.sh \
  && chown -R hop:hop ${DEPLOYMENT_PATH}/hop \
  && chmod 700 ${DEPLOYMENT_PATH}/hop/*.sh

EXPOSE 8080

# make volume available so that hop pipeline and workflow files can be provided easily
VOLUME ["/files"]
USER hop
ENV PATH=$PATH:${DEPLOYMENT_PATH}/hop
WORKDIR /home/hop
# CMD ["/bin/bash"]
ENTRYPOINT ["/bin/bash", "/opt/hop/run.sh"]

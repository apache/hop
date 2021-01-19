<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# hop-docker

A **Hop Docker image** supporting both **short-lived** and **long-lived** setups.


## Container Folder Structure


Directory	| Description
---	|---
`/opt/project-hop`	| location of the hop package
`/files`	| here you should mount a directory that contains the **hop and project config** as well as the **workflows and pipelines**.

## Environment Variables

You can provide values for the following environment variables:


Environment Variable	| Required	| Description
---	|----	|---
`HOP_LOG_LEVEL`	| No	| Specify the log level. Default: `Basic`. Optional.
`HOP_FILE_PATH`	| Yes	| Path to hop workflow or pipeline
`HOP_LOG_PATH`	| No	| File path to hop log file
`HOP_CONFIG_DIRECTORY`	| No	| Path to the Hop config folder. DISABLED for now.
`HOP_PROJECT_NAME`	| Yes	| Name of the Hop project to use
`HOP_PROJECT_DIRECTORY`	| Yes	| Path to the home of the hop project. Should start with `/files`.
`HOP_PROJECT_CONFIG_FILE_NAME`	| No	| Name of the project config file including file extension. Defaults to `project-config.json`.
`HOP_ENVIRONMENT_NAME`	| Yes	| Name of the Hop run environment to use
`HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS`	| Yes	| comma separated list of paths to environment config files (including filename and file extension). paths should start with `/files`.
`HOP_RUN_CONFIG`	| Yes	| Name of the Hop run configuration to use
`HOP_RUN_PARAMETERS`	| No	| Parameters that should be passed on to the hop-run command. Specify as comma separated list, e.g. `PARAM_1=aaa,PARAM_2=bbb`. Optional.
`HOP_OPTIONS`	| No	| Any JRE options you want to set
`HOP_SHARED_JDBC_DIRECTORY`	| No	| Path to the directory where the JDCB drivers are located
`HOP_SERVER_USER`	| No	| Username for hop-server, only valid in long-lived containers. Default `cluster`
`HOP_SERVER_PASS`	| No	| Password for hop-server user, only valid in long-lived containers. Default `cluster`

The `Required` column relates to running a short-lived container.

## How to run the Container

The most common use case will be that you run a **short-lived container** to just complete one Hop workflow or pipeline.

Example for running a **workflow**:

```bash
docker run -it --rm \
  --env HOP_LOG_LEVEL=Basic \
  --env HOP_FILE_PATH='${PROJECT_HOME}/pipelines-and-workflows/main.hwf' \
  --env HOP_PROJECT_DIRECTORY=/files/project \
  --env HOP_PROJECT_NAME=project-a \
  --env HOP_ENVIRONMENT_NAME=project-a-test \
  --env HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS=/files/config/project-a-test.json \
  --env HOP_RUN_CONFIG=classic \
  --env HOP_RUN_PARAMETERS=PARAM_LOG_MESSAGE=Hello,PARAM_WAIT_FOR_X_MINUTES=1 \
  -v /path/to/local/dir:/files \
  --name my-simple-hop-container \
  docker pull docker.io/apache/incubator-hop:<tag>
```

If you need a **long-lived container**, this option is also available. Run this command e.g.:

```bash
docker run -it --rm \
  --env HOP_LOG_LEVEL=Basic \
  --env HOP_PROJECT_DIRECTORY=/files/project \
  --env HOP_PROJECT_NAME=project-a \
  --env HOP_ENVIRONMENT_NAME=project-a-test \
  --env HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS=/files/config/project-a-test.json \
  --env HOP_SERVER_USER=admin \
  --env HOP_SERVER_PASS=admin \
  -p 8080:8080
  -v /path/to/local/dir:/files \
  --name my-simple-hop-container \
  docker pull docker.io/apache/incubator-hop:<tag>
```

You can then access the hop-server UI from your dockerhost at `http://localhost:8080`

# Shortcomings

Currently the `hop-server` support is minimal.


#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Checks what reached the DataHub stand-in, then stops it:
# - the pipeline run with the local-lineage run configuration reported its lineage, with the
#   token of the data catalog the lineage sink names;
# - so did the workflow run with the local-lineage workflow run configuration, the pipeline it ran
#   with a run configuration without a sink, and the file that pipeline wrote;
# - the pipeline in publish/ was published without running, with its files and their layouts;
# - the pipeline run with the local run configuration, and the workflow, reported nothing:
#   lineage is not enabled with variables in this project.

LOG=/tmp/lineage-sinks-proposals.log

python3 -I "$(dirname "$0")/0001-check-proposals.py" "${LOG}"
RESULT=$?

if [ -f /tmp/lineage-sinks-stub.pid ]; then
  kill "$(cat /tmp/lineage-sinks-stub.pid)" 2>/dev/null
  rm -f /tmp/lineage-sinks-stub.pid
fi
exit ${RESULT}

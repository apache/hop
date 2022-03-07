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

#
# When stopping the running hop-server container with 'docker stop' it ended up with an timeout
# and an exitcode > 0 because signals are not catched correctly.
# It was not possible to end the container gracefully with Ctrl-C when it was started without -d
# option which might be annoying on a gratefull exit. Therefore this script ( run.sh )
# that catches signals coming from the docker host is introduced.
#

log() {
  echo $(date '+%Y/%m/%d %H:%M:%S')" - ${1}"
}

#
#   catch all signals that come from outside the container
#   to be able to exit gracefully
#
trapper() {
  "$@" &
  pid="$!"
  log "Running the entrypoint script with PID ${pid}"
  trap "log 'Stopping entrypoint script with $pid'; kill -SIGTERM $pid" SIGINT SIGTERM

  while kill -0 $pid >/dev/null 2>&1; do
    wait
  done
}

trapper /opt/hop/load-and-execute.sh "$@"

if [ -f /tmp/exitcode.txt ]; then
  EXIT_CODE=$(cat /tmp/exitcode.txt)
  exit "${EXIT_CODE}"
else
  exit 7
fi

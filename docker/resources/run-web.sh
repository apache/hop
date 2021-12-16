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

log() {
    echo `date '+%Y/%m/%d %H:%M:%S'`" - ${1}"
}

#
# Stopping a running hop web container with 'docker stop' is obviously possible.
# Doing it with CTRL-C is just more convenient.
# So we'll start the catalina.sh script in the background and wait until
# we trap SIGINT or SIGTERM. At that point we'll simply stop Tomcat.
#

catalina.sh run &
pid="$!"
log "Running Apache Tomcat / Hop Web with PID ${pid}"
trap "log 'Stopping Tomcat'; catalina.sh stop" SIGINT SIGTERM

while kill -0 $pid > /dev/null 2>&1; do
    wait
done

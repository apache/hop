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

# Checks that Mask fields in 0002-mask-by-tag.hpl asked the DataHub stub for the customers dataset and
# its schema (issue #9323), then stops the stub.

LOG=/tmp/datahub-stub-requests.log
status=0

expect() {
  if grep -q -E "$1" "${LOG}"; then
    echo "OK: $2"
  else
    echo "FAILED: $2"
    status=1
  fi
}

expect '^hopDatasetExists urn:li:dataset:\(urn:li:dataPlatform:hive,it\.customers,PROD\)$' \
  "Mask fields looked for the customers dataset"
expect '^hopDatasetSchema urn:li:dataset:\(urn:li:dataPlatform:hive,it\.customers,PROD\)$' \
  "Mask fields asked for the schema of the customers dataset"

if [ "${status}" != "0" ]; then
  echo "Requests the stub received:"
  cat "${LOG}"
fi

kill "$(cat /tmp/datahub-stub.pid)" 2>/dev/null
exit ${status}

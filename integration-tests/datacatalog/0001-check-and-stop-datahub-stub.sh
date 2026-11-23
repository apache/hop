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

# Checks that the Check data catalog actions of main-0001-check-data-catalog.hwf asked the DataHub
# stub what they should (issue #9323), then stops the stub. A table asks for the URN lineage gives
# it: the connection's database, its preferred schema and the table.

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

expect '^hopDatasetExists urn:li:dataset:\(urn:li:dataPlatform:postgres,sales\.public\.orders,PROD\)$' \
  "the table asks for the URN lineage gives it"
expect '^hopDatasetExists urn:li:dataset:\(urn:li:dataPlatform:file,/.*/files/customers\.csv,PROD\)$' \
  "the file asks for its path"
expect '^hopDatasetDeprecation urn:li:dataset:\(urn:li:dataPlatform:hive,it\.deprecated,PROD\)$' \
  "the deprecated dataset was asked about its deprecation"
expect '^hopDatasetOperations urn:li:dataset:\(urn:li:dataPlatform:hive,it\.stale,PROD\)$' \
  "the stale dataset was asked about its last change"
expect '^hopDatasetAssertions urn:li:dataset:\(urn:li:dataPlatform:hive,it\.failing,PROD\)$' \
  "the failing dataset was asked about its assertions"
expect '^hopDatasetExists urn:li:dataset:\(urn:li:dataPlatform:hive,it\.unknown,PROD\)$' \
  "the unknown dataset was looked for"

if [ "${status}" != "0" ]; then
  echo "Requests the stub received:"
  cat "${LOG}"
fi

kill "$(cat /tmp/datahub-stub.pid)" 2>/dev/null
exit ${status}

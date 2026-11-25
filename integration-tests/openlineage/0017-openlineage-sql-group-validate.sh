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

# Checks the tables main-0006 reported, in Marquez and in the DataHub stand-in.
# Both run as services in docker/integration-tests/integration-tests-openlineage.yaml.
exec python3 -I "$(dirname "$0")/0017-openlineage-sql-group-validate.py" \
  "${MARQUEZ_API:-http://marquez:5000}" "${DATAHUB_STUB_URL:-http://datahub:8080}"

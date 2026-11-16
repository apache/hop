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

# Checks the DataHub operations of main-0003: each writer loads the 3 source rows, counted by the transform itself.
# DataHub stand-in: service "datahub" in docker/integration-tests/integration-tests-openlineage.yaml.
exec python3 -I "$(dirname "$0")/datahub-operations.py" "${DATAHUB_STUB_URL:-http://datahub:8080}" \
  "0007-openlineage-insertupdate=orders_upsert:INSERT:3" \
  "0008-openlineage-combinationlookup=orders_combi:INSERT:3" \
  "0009-openlineage-dimensionlookup=orders_dim:INSERT:3" \
  "0010-openlineage-pgbulkloader=orders_bulk:INSERT:3"

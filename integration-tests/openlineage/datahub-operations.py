#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Checks the DataHub operations the DataHub lineage sink sent, and the row counts of a run.

Usage: validate.py <stub url> <expectation>...

  <pipeline>=<table>:<type>:<rows>   that pipeline's run reported one operation on the table, of
                                     that type, with that many rows ('-': without a row count)
  <pipeline>=<table>:none            that pipeline's run reported no operation on the table
  run:<name prefix>:<counter>=<n>    the run whose name starts with the prefix has that row count

The DataHub sink delivers on a background thread, so this waits up to a minute until every
pipeline named has reported before it judges. Exit code 0 when all is as expected, 1 otherwise.
"""

import json
import sys
import time
import urllib.request

STUB = sys.argv[1]
OPERATIONS = {}
NONE = {}
RUNS = []
for spec in sys.argv[2:]:
    if spec.startswith("run:"):
        _, prefix, rest = spec.split(":", 2)
        counter, value = rest.split("=", 1)
        RUNS.append((prefix, counter, value))
        continue
    job, target = spec.split("=", 1)
    parts = target.split(":")
    if parts[1] == "none":
        NONE.setdefault(job, []).append(parts[0])
    else:
        rows = None if parts[2] == "-" else int(parts[2])
        OPERATIONS.setdefault(job, []).append((parts[0], parts[1], rows))


def proposals(aspect):
    with urllib.request.urlopen(f"{STUB}/proposals?aspect={aspect}", timeout=10) as response:
        return json.load(response)


def operations():
    """The operations kept so far, by pipeline: (table, type, rows)."""
    found = {}
    for proposal in proposals("operation"):
        value = proposal["value"]
        job = value.get("customProperties", {}).get("hopJob")
        table = proposal["entityUrn"].split(",")[1].split(".")[-1]
        found.setdefault(job, []).append(
            (table, value.get("operationType"), value.get("numAffectedRows"))
        )
    return found


deadline = time.time() + 60
found = operations()
while any(job not in found for job in OPERATIONS) and time.time() < deadline:
    time.sleep(2)
    found = operations()

problems = []
for job, expected in OPERATIONS.items():
    got = found.get(job, [])
    print(f"{job}: {got}")
    for operation in expected:
        if operation not in got:
            problems.append(f"{job}: expected {operation}, got {got}")
    tables = [table for table, _, _ in expected]
    for operation in got:
        if operation[0] in tables and operation not in expected:
            problems.append(f"{job}: unexpected {operation}")
for job, tables in NONE.items():
    got = [op for op in found.get(job, []) if op[0] in tables]
    print(f"{job}: {got}")
    if got:
        problems.append(f"{job}: expected no operation on {tables}, got {got}")

properties = proposals("dataProcessInstanceProperties")
for prefix, counter, value in RUNS:
    runs = [
        p["value"]["customProperties"]
        for p in properties
        if p["value"].get("name", "").startswith(prefix)
        and counter in p["value"].get("customProperties", {})
    ]
    print(f"run {prefix}: {runs[-1] if runs else None}")
    if not runs:
        problems.append(f"no {counter} on the run of {prefix}")
    elif runs[-1][counter] != value:
        problems.append(f"{prefix}: {counter} is {runs[-1][counter]}, not {value}")

if problems:
    print("FAILED:\n  " + "\n  ".join(problems))
    sys.exit(1)
print("All operations and row counts are as expected")

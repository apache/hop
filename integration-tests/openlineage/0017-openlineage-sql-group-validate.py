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

"""Checks the tables main-0006 reported: in Marquez per job, in the DataHub stand-in per task.

Usage: python3 -I 0017-openlineage-sql-group-validate.py <Marquez URL> <DataHub stand-in URL>

The sinks deliver on a background thread, so this waits up to a minute until everything expected
has arrived before it judges. Exit code 0 when all is as expected, 1 otherwise.
"""

import json
import sys
import time
import urllib.request

MARQUEZ = sys.argv[1]
STUB = sys.argv[2]
PIPELINE = "0017-openlineage-sql-group"
ACTION = "main-0006-openlineage-sql-group/Copy sqlgroup_orders"

# Marquez: job -> (tables it reads, tables it writes). Transforms have no jobs of their own here,
# so the pipeline's job holds what its transforms read and wrote.
MARQUEZ_JOBS = {
    PIPELINE: (
        {"sqlgroup_orders", "sqlgroup_customers", "sqlgroup_regions"},
        {"sqlgroup_audit"},
    ),
    ACTION: ({"sqlgroup_orders"}, {"sqlgroup_orders_copy"}),
}
# DataHub: task -> (tables it reads, tables it writes)
DATAHUB_TASKS = {
    "Look up customer": ({"sqlgroup_customers"}, set()),
    "Join region": ({"sqlgroup_regions"}, set()),
    "Write audit rows": (set(), {"sqlgroup_audit"}),
    "Copy sqlgroup_orders": ({"sqlgroup_orders"}, {"sqlgroup_orders_copy"}),
}


def get(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def table(name):
    """sqlgroup_x from hop_database.public.sqlgroup_x, or from a DataHub dataset URN."""
    if name.startswith("urn:li:dataset:"):
        name = name.split(",")[1]
    return name.rsplit(".", 1)[-1]


def all_marquez_jobs():
    try:
        return get(f"{MARQUEZ}/api/v1/namespaces/hop/jobs?limit=1000").get("jobs", [])
    except Exception:
        return []


def marquez_jobs():
    """The expected jobs, also when Marquez put the name of a parent job in front of theirs."""
    found = {}
    for job in all_marquez_jobs():
        for expected in MARQUEZ_JOBS:
            if job["name"] == expected or job["name"].endswith("." + expected):
                inputs, outputs = found.setdefault(expected, (set(), set()))
                inputs.update(table(d["name"]) for d in job.get("inputs", []))
                outputs.update(table(d["name"]) for d in job.get("outputs", []))
    return found


def datahub_tasks():
    found = {}
    try:
        proposals = get(f"{STUB}/proposals?aspect=dataJobInputOutput")
    except Exception:
        return found
    for proposal in proposals:
        urn = proposal.get("entityUrn") or ""
        for task in DATAHUB_TASKS:
            if urn.endswith("," + task + ")"):
                value = proposal.get("value") or {}
                inputs, outputs = found.setdefault(task, (set(), set()))
                inputs.update(table(u) for u in value.get("inputDatasets", []))
                outputs.update(table(u) for u in value.get("outputDatasets", []))
    return found


def problems():
    found = []
    jobs = marquez_jobs()
    for job, (reads, writes) in MARQUEZ_JOBS.items():
        inputs, outputs = jobs.get(job, (set(), set()))
        if not reads <= inputs:
            found.append(f"Marquez: {job} reads {sorted(inputs)}, not {sorted(reads)}")
        if not writes <= outputs:
            found.append(f"Marquez: {job} writes {sorted(outputs)}, not {sorted(writes)}")
    tasks = datahub_tasks()
    for task, (reads, writes) in DATAHUB_TASKS.items():
        inputs, outputs = tasks.get(task, (set(), set()))
        if not reads <= inputs:
            found.append(f"DataHub: task {task} reads {sorted(inputs)}, not {sorted(reads)}")
        if not writes <= outputs:
            found.append(f"DataHub: task {task} writes {sorted(outputs)}, not {sorted(writes)}")
    return found


def main():
    deadline = time.time() + 60
    found = problems()
    while found and time.time() < deadline:
        time.sleep(2)
        found = problems()
    for problem in found:
        print("PROBLEM: " + problem)
    if found:
        names = sorted(j["name"] for j in all_marquez_jobs() if "sql" in j["name"].lower())
        print(f"Marquez jobs: {names}")
        return 1
    print("Database lookup, Database join, Execute row SQL script and the SQL action reported their tables")
    return 0


if __name__ == "__main__":
    sys.exit(main())

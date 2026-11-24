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

"""Checks what main-0007-kafka-lineage reported, from the log of 0007-kafka-lineage-stub.py.

Usage: python3 -I 0007-kafka-lineage-check.py <log>

The sinks deliver on a background thread, so this waits up to a minute until everything expected
has arrived before it judges. Exit code 0 when all is as expected, 1 otherwise.
"""

import json
import sys
import time

TOPIC = "hop-lineage-topic"
NAMESPACE = "kafka://kafka:9092"
URN = f"urn:li:dataset:(urn:li:dataPlatform:kafka,{TOPIC},PROD)"
PRODUCE = "0007-kafka-lineage-produce"
CONSUME = "0007-kafka-lineage-consume"


def lines(path):
    with open(path, encoding="utf-8") as log:
        return [json.loads(line) for line in log if line.strip()]


def has_topic(event, side):
    return {"namespace": NAMESPACE, "name": TOPIC} in event[side]


def edges(value, side):
    """The datasets a dataJobInputOutput aspect, or a patch of one, gives a task on one side."""
    if isinstance(value, list):
        return [
            (op.get("value") or {}).get("destinationUrn")
            for op in value
            if str(op.get("path", "")).startswith(f"/{side}DatasetEdges/")
        ]
    if isinstance(value, dict):
        return list(value.get(f"{side}Datasets") or []) + [
            e.get("destinationUrn") for e in value.get(f"{side}DatasetEdges") or []
        ]
    return []


def problems(kept):
    found = []
    openlineage = [k for k in kept if k["sink"] == "openlineage"]
    datahub = [k for k in kept if k["sink"] == "datahub"]
    for job, side in ((PRODUCE, "outputs"), (CONSUME, "inputs")):
        events = [e for e in openlineage if (e["job"] or "").endswith(job) and has_topic(e, side)]
        if not events:
            found.append(f"OpenLineage: no event of {job} has {NAMESPACE} {TOPIC} in its {side}")
        elif not any(e["eventType"] == "RUNNING" for e in events):
            types = [e["eventType"] for e in events]
            found.append(f"OpenLineage: {job} sent the topic only on {types}, not while it ran")
    for task, side in (("Kafka Producer", "output"), ("Kafka Consumer", "input")):
        found_edges = [
            p
            for p in datahub
            if p["aspectName"] == "dataJobInputOutput"
            and (p["entityUrn"] or "").endswith("," + task + ")")
            and URN in edges(p["value"], side)
        ]
        if not found_edges:
            found.append(f"DataHub: task {task} has no {side} {URN}")
    return found


def main(path):
    deadline = time.time() + 60
    found = problems(lines(path))
    while found and time.time() < deadline:
        time.sleep(2)
        found = problems(lines(path))
    for problem in found:
        print("PROBLEM: " + problem)
    if found:
        print(f"{len(lines(path))} lines kept")
        return 1
    print(f"Both sinks got {TOPIC} from the Kafka Producer and Kafka Consumer, OpenLineage while they ran")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))

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
"""Checks the proposals the DataHub stand-in kept (see 0001-check-and-stop-datahub-stub.sh).

Usage: python3 -I 0001-check-proposals.py <proposal log>
"""

import json
import sys
import time

REPORTED = "0001-lineage-sink-reported"
INHERITED = ("0003-lineage-sink-child-workflow", "0004-lineage-sink-inherited", "inherited.csv")
NOT_REPORTED = ("0002-lineage-sink-not-reported", "main-0001-lineage-sinks")
# Published without running: the flow, its tasks with their files, and the files' layouts
PUBLISHED_FLOW = "publish/0005-lineage-sink-published.hpl"
PUBLISHED_FILES = ("publish/customers.csv", "published-customers.csv")
# The unit test of 0006, run by Run pipeline unit tests in 0003: a passed assertion on its file
UNIT_TESTED_FILE = "unit-tested.csv"
# The expressions of a Formula (run) and a Calculator (published): task properties logic.<field>
LOGIC = (
    (REPORTED, "Measure marker", "marker_length", "LEN([marker])"),
    (PUBLISHED_FLOW, "Double id", "double_id", "ADD(id, id)"),
)
TOKEN = "Bearer it-token"


def proposals(path):
    with open(path, encoding="utf-8") as log:
        return [json.loads(line) for line in log if line.strip()]


def everything_arrived(kept):
    """Whether every run and publication the workflow made has reached the stand-in."""
    expected = (REPORTED, PUBLISHED_FLOW) + INHERITED
    return all(any(name in (p["entityUrn"] or "") for p in kept) for name in expected) and any(
        p["aspectName"] == "assertionRunEvent" for p in kept
    )


def main(path):
    # Delivery is asynchronous: wait for the last of the reported runs, then a little longer for
    # anything that should not have come.
    deadline = time.time() + 60
    while time.time() < deadline and not everything_arrived(proposals(path)):
        time.sleep(1)
    time.sleep(5)
    kept = proposals(path)
    problems = []
    reported = [p for p in kept if REPORTED in (p["entityUrn"] or "")]
    if not reported:
        problems.append(f"nothing about {REPORTED} reached DataHub")
    # A run is a data process instance with an id of its own, not the pipeline's name: only the
    # runs of the reported pipeline reach this stand-in, so any run event is one of them.
    if not any(p["aspectName"] == "dataProcessInstanceRunEvent" for p in kept):
        problems.append(f"no run event of {REPORTED}")
    if not any("it-lineage-sinks" in (p["entityUrn"] or "") for p in reported):
        problems.append("the platform instance of the lineage sink is not in the URNs")
    for name in INHERITED:
        if not any(name in (p["entityUrn"] or "") for p in kept):
            problems.append(f"nothing about {name}: it should report to the sink of its parent")
    published = [p for p in kept if PUBLISHED_FLOW in (p["entityUrn"] or "")]
    aspects = {p["aspectName"] for p in published}
    for aspect in ("dataFlowInfo", "dataJobInfo", "dataJobInputOutput"):
        if aspect not in aspects:
            problems.append(f"the published pipeline has no {aspect}: {sorted(aspects)}")
    for name in PUBLISHED_FILES:
        schemas = [
            p for p in kept if p["aspectName"] == "schemaMetadata" and name in (p["entityUrn"] or "")
        ]
        if not schemas:
            problems.append(f"no schema for {name}, a file the published pipeline declares")
    documents = [p for p in kept if p["aspectName"] == "documentInfo"]
    published_documents = [
        p
        for p in documents
        if any(PUBLISHED_FLOW in a.get("asset", "") for a in p["value"].get("relatedAssets", []))
    ]
    if not published_documents:
        problems.append("no document related to the published pipeline")
    else:
        document = published_documents[-1]["value"]
        text = document.get("contents", {}).get("text", "")
        if "Filename : 0005-lineage-sink-published.hpl" not in text or "Never run" not in text:
            problems.append(f"the published pipeline's document is not what hop doc writes: {text}")
        parent = document.get("parentDocument", {}).get("document")
        if not any(
            p["entityUrn"] == parent and p["value"].get("title", "").startswith("Hop project ")
            for p in documents
        ):
            problems.append(f"the published pipeline's document is not under its project: {parent}")
    runs = [
        p
        for p in kept
        if p["aspectName"] == "assertionRunEvent"
        and UNIT_TESTED_FILE in p.get("value", {}).get("asserteeUrn", "")
    ]
    if not runs:
        problems.append(f"no assertion run on {UNIT_TESTED_FILE} from the unit test")
    elif runs[-1]["value"].get("result", {}).get("type") != "SUCCESS":
        problems.append(f"the unit test assertion did not pass: {runs[-1]['value']}")
    elif not any(
        p["aspectName"] == "assertionInfo" and p["entityUrn"] == runs[-1]["entityUrn"] for p in kept
    ):
        problems.append("the unit test assertion run has no assertion")
    # What a transform calculates its fields with: a Formula that ran, a Calculator published
    for flow, task, field, expression in LOGIC:
        infos = [
            p
            for p in kept
            if p["aspectName"] == "dataJobInfo"
            and flow in (p["entityUrn"] or "")
            and (p["entityUrn"] or "").endswith("," + task + ")")
        ]
        found = infos[-1]["value"].get("customProperties", {}).get("logic." + field) if infos else None
        if found != expression:
            problems.append(f"task {task} of {flow} shows {found} as the logic of {field}, not {expression}")
    for name in NOT_REPORTED:
        if any(name in (p["entityUrn"] or "") for p in kept):
            problems.append(f"{name} reported its lineage, its run configuration has no sink")
    tokens = {p["authorization"] for p in kept}
    if tokens != {TOKEN}:
        problems.append(f"DataHub was written to with {tokens}, not the data catalog's token")

    print(f"{len(kept)} proposals, {len(reported)} about {REPORTED}")
    for problem in problems:
        print("PROBLEM: " + problem)
    if problems:
        return 1
    print("Only the runs with a lineage sink in their run configuration, or their parent's, reported their lineage, the published pipeline was published with its documentation, the unit test is an assertion, and transforms show their expressions")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))

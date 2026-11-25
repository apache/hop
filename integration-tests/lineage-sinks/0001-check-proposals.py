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

And the run events the OpenLineage stand-in kept: the unit test run by 0009 reports its result to
the openlineage-stub sink of 0009's run configuration, which gets neither the start nor the end of
the test run. The result has to end that run, once, or Marquez lists it as running for good. The
test passes, so it ends the run as COMPLETE.

Usage: python3 -I 0001-check-proposals.py <proposal log> <OpenLineage log> <java.io.tmpdir of the Hop run>
"""

import json
import os
import sys
import time

REPORTED = "0001-lineage-sink-reported"
INHERITED = ("0003-lineage-sink-child-workflow", "0004-lineage-sink-inherited", "inherited.csv")
NOT_REPORTED = ("0002-lineage-sink-not-reported", "main-0001-lineage-sinks")
# Published without running: the flow, its tasks with their files, and the files' layouts. The
# names resolve as a run of the pipeline resolves them: its own folder, and its parameter's default.
PUBLISHED_FLOW = "publish/0005-lineage-sink-published.hpl"
PUBLISHED_FILES = ("publish/customers.csv", "/data/published/published-customers.csv")
# The unit test of 0006, run by Run pipeline unit tests in 0003: a passed assertion on its file,
# and nothing else about the run, which runs a changed copy of the pipeline
UNIT_TESTED_FILE = "unit-tested.csv"
UNIT_TESTED_PIPELINE = "0006-lineage-sink-unit-tested"
# The same test run by the Execute tests transform of 0008: its result is an assertion as well
EXECUTE_TESTS_UNIT_TEST = "lineage-sinks-unit-test-execute-tests"
# The unit tests that are assertions on UNIT_TESTED_FILE, with what runs them
ASSERTED_UNIT_TESTS = {
    "lineage-sinks-unit-test": "Run pipeline unit tests",
    EXECUTE_TESTS_UNIT_TEST: "the Execute tests transform",
}
# A workflow the Workflow executor of a tested pipeline starts belongs to the unit test run: neither
# it, the tested pipeline nor the file the workflow writes is reported. The file is there, though:
# the workflow ran.
UNIT_TEST_CHILD = (
    "0007-lineage-sink-unit-test-child-workflow",
    "0007-lineage-sink-unit-tested-executor",
    "unit-test-child-workflow.txt",
)
UNIT_TEST_CHILD_FILE = os.path.join("lineage-sinks-it", "unit-test-child-workflow.txt")
# The expressions of a Formula (run) and a Calculator (published): task properties logic.<field>
LOGIC = (
    (REPORTED, "Measure marker", "marker_length", "LEN([marker])"),
    (PUBLISHED_FLOW, "Double id", "double_id", "ADD(id, id)"),
)
# A table on the server the data catalog gives the platform instance warehouse
WAREHOUSE_TABLE = "urn:li:dataset:(urn:li:dataPlatform:postgres,warehouse.sales.public.customers,DEV)"
WAREHOUSE_INSTANCE = "urn:li:dataPlatformInstance:(urn:li:dataPlatform:postgres,warehouse)"
TOKEN = "Bearer it-token"


def proposals(path):
    with open(path, encoding="utf-8") as log:
        return [json.loads(line) for line in log if line.strip()]


def unit_test_assertions(kept, unit_test):
    """The assertions of a unit test, by its name in the assertion's properties."""
    return {
        p["entityUrn"]
        for p in kept
        if p["aspectName"] == "assertionInfo"
        and (p["value"].get("customProperties") or {}).get("hopUnitTest") == unit_test
    }


def everything_arrived(kept, events):
    """Whether every run and publication the workflow made has reached the stand-ins."""
    expected = (REPORTED, PUBLISHED_FLOW) + INHERITED
    return (
        all(any(name in (p["entityUrn"] or "") for p in kept) for name in expected)
        and all(unit_test_assertions(kept, unit_test) for unit_test in ASSERTED_UNIT_TESTS)
        and any(e["unitTest"] for e in events)
    )


def unit_test_runs_that_end(events):
    """Problems with the runs the OpenLineage sink got unit test results on: each ends once, as
    COMPLETE when its test passed and as FAIL otherwise."""
    problems = []
    results = [e for e in events if e["unitTest"]]
    if not results:
        problems.append("no unit test result reached the openlineage-stub sink")
    for result in results:
        if result.get("passed") is not True:
            problems.append(f"the unit test result of {result['job']} did not pass: {result}")
    for run_id in sorted({e["runId"] for e in results}):
        types = [e["eventType"] for e in events if e["runId"] == run_id]
        ends = [t for t in types if t in ("COMPLETE", "FAIL", "ABORT")]
        job = next(e["job"] for e in results if e["runId"] == run_id)
        passed = all(e.get("passed") is True for e in results if e["runId"] == run_id)
        if len(ends) != 1:
            problems.append(f"run {run_id} of {job} has to end once, not {len(ends)} times: {types}")
        elif ends[0] != ("COMPLETE" if passed else "FAIL"):
            problems.append(f"run {run_id} of {job} ends as {ends[0]}, its test passed: {passed}")
    return problems


def main(path, openlineage_path, tmpdir):
    # Delivery is asynchronous: wait for the last of the reported runs, then a little longer for
    # anything that should not have come.
    deadline = time.time() + 60
    while time.time() < deadline and not everything_arrived(
        proposals(path), proposals(openlineage_path)
    ):
        time.sleep(1)
    time.sleep(5)
    kept = proposals(path)
    events = proposals(openlineage_path)
    problems = unit_test_runs_that_end(events)
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
    unresolved = sorted(
        {
            p["entityUrn"]
            for p in kept
            if "${" in (p["entityUrn"] or "")
            or (p["aspectName"] == "dataJobInputOutput" and "${" in json.dumps(p["value"]))
        }
    )
    if unresolved:
        problems.append(f"published with a variable that was not resolved: {unresolved}")
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
        if any(
            PUBLISHED_FLOW in (a.get("asset") or "")
            for a in (p["value"].get("relatedAssets") or [])
        )
    ]
    if not published_documents:
        problems.append("no document related to the published pipeline")
    else:
        document = published_documents[-1]["value"]
        text = (document.get("contents") or {}).get("text") or ""
        if "Filename : 0005-lineage-sink-published.hpl" not in text or "Never run" not in text:
            problems.append(f"the published pipeline's document is not what hop doc writes: {text}")
        parent = (document.get("parentDocument") or {}).get("document")
        if not any(
            p["entityUrn"] == parent and (p["value"].get("title") or "").startswith("Hop project ")
            for p in documents
        ):
            problems.append(f"the published pipeline's document is not under its project: {parent}")
    # Each unit test by name, so a failing one is named: an assertion on the tested file, that passed
    for unit_test, runner in ASSERTED_UNIT_TESTS.items():
        assertions = unit_test_assertions(kept, unit_test)
        runs = [
            p for p in kept if p["aspectName"] == "assertionRunEvent" and p["entityUrn"] in assertions
        ]
        if not runs:
            problems.append(f"no assertion run of unit test {unit_test}, run by {runner}")
        elif UNIT_TESTED_FILE not in (runs[-1]["value"].get("asserteeUrn") or ""):
            problems.append(f"unit test {unit_test} is not an assertion on {UNIT_TESTED_FILE}: {runs[-1]['value']}")
        elif (runs[-1]["value"].get("result") or {}).get("type") != "SUCCESS":
            problems.append(f"unit test {unit_test}, run by {runner}, did not pass: {runs[-1]['value']}")
    unit_test_runs = sorted({p["entityUrn"] for p in kept if UNIT_TESTED_PIPELINE in (p["entityUrn"] or "")})
    if unit_test_runs:
        problems.append(f"the unit test run reported its own lineage: {unit_test_runs}")
    for name in UNIT_TEST_CHILD:
        leaked = sorted({p["entityUrn"] for p in kept if name in (p["entityUrn"] or "")})
        if leaked:
            problems.append(f"{name} was reported, but a unit test run started it: {leaked}")
    # Nothing leaked above only means something if the workflow ran: unit test
    # lineage-sinks-unit-test-executor runs it, and the file it writes was deleted before the run
    if not os.path.isfile(os.path.join(tmpdir, UNIT_TEST_CHILD_FILE)):
        problems.append(
            "unit test lineage-sinks-unit-test-executor did not run its child workflow: "
            f"no {os.path.join(tmpdir, UNIT_TEST_CHILD_FILE)}"
        )
    # What a transform calculates its fields with: a Formula that ran, a Calculator published
    for flow, task, field, expression in LOGIC:
        infos = [
            p
            for p in kept
            if p["aspectName"] == "dataJobInfo"
            and flow in (p["entityUrn"] or "")
            and (p["entityUrn"] or "").endswith("," + task + ")")
        ]
        found = (
            (infos[-1]["value"].get("customProperties") or {}).get("logic." + field) if infos else None
        )
        if found != expression:
            problems.append(f"task {task} of {flow} shows {found} as the logic of {field}, not {expression}")
    loads = [
        p
        for p in published
        if p["aspectName"] == "dataJobInputOutput" and p["entityUrn"].endswith(",Load warehouse)")
    ]
    outputs = (loads[-1]["value"].get("outputDatasets") or []) if loads else None
    if outputs != [WAREHOUSE_TABLE]:
        problems.append(f"Load warehouse writes {outputs}, not {WAREHOUSE_TABLE}")
    instances = [
        p["value"].get("instance")
        for p in kept
        if p["aspectName"] == "dataPlatformInstance" and p["entityUrn"] == WAREHOUSE_TABLE
    ]
    if WAREHOUSE_INSTANCE not in instances:
        problems.append(f"{WAREHOUSE_TABLE} is not in platform instance warehouse: {instances}")
    for name in NOT_REPORTED:
        if any(name in (p["entityUrn"] or "") for p in kept):
            problems.append(f"{name} reported its lineage, its run configuration has no sink")
    tokens = {p["authorization"] for p in kept}
    if tokens != {TOKEN}:
        problems.append(f"DataHub was written to with {tokens}, not the data catalog's token")

    print(f"{len(kept)} proposals, {len(reported)} about {REPORTED}, {len(events)} OpenLineage events")
    for problem in problems:
        print("PROBLEM: " + problem)
    if problems:
        return 1
    print("Only the runs with a lineage sink in their run configuration, or their parent's, reported their lineage, the published pipeline was published with its documentation, the unit tests are assertions (from Run pipeline unit tests and Execute tests) without the runs they start, a unit test ends its run in OpenLineage, transforms show their expressions, and a table is named with the platform instance of its server")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "/tmp"))

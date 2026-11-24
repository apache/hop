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
"""Checks the lineage main-0022-lineage-on-hop-server got, from the log of 0022-lineage-stub.py.

Usage: python3 -I 0022-lineage-check.py <log>

The pipeline ran on the Hop Server with a run configuration that selects the openlineage-stub sink.
Its lineage has to be one run, the client's, as for a local run: one run id, ended once, with the
file the pipeline wrote on the server, the client's file name and the client's action as its
parent. The sinks deliver on a background thread, so this waits up to a minute for the run's end
and its file before it judges. Exit code 0 when all is as expected, 1 otherwise.
"""

import json
import re
import sys
import time

PIPELINE = "0022-lineage-remote-pipeline"
FILENAME = "${PROJECT_HOME}/0022-lineage-remote-pipeline.hpl"
OUTPUT = "remote-output.txt"
PARENT = "main-0022-lineage-on-hop-server/Run the pipeline on the server"
TERMINAL = ("COMPLETE", "FAIL", "ABORT")


def lines(path):
    """The complete lines of the log. The stub may still be writing the last one: it is skipped."""
    kept = []
    with open(path, encoding="utf-8") as log:
        for line in log:
            if not line.endswith("\n") or not line.strip():
                continue
            try:
                kept.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return kept


def pipeline_events(kept):
    return [e for e in kept if e["job"] == PIPELINE]


def writes_output(event):
    return any((d["name"] or "").endswith(OUTPUT) for d in event["outputs"])


def arrived(kept):
    events = pipeline_events(kept)
    return any(e["eventType"] in TERMINAL for e in events) and any(writes_output(e) for e in events)


def problems(kept):
    events = pipeline_events(kept)
    if not events:
        return [f"no event of {PIPELINE} reached the sink: {sorted({e['job'] for e in kept})}"]
    found = []
    runs = sorted({e["runId"] for e in events})
    if len(runs) != 1:
        found.append(f"{PIPELINE} is {len(runs)} runs, not one: {runs}")
    for run in runs:
        types = [e["eventType"] for e in events if e["runId"] == run]
        ends = [t for t in types if t in TERMINAL]
        if len(ends) != 1:
            found.append(f"run {run} ends {len(ends)} times, not once: {types}")
    if not any(writes_output(e) for e in events):
        outputs = [d["name"] for e in events for d in e["outputs"]]
        found.append(f"no event of {PIPELINE} has the file it wrote on the server: {outputs}")
    # ${PROJECT_HOME} can end in a slash, which a portable file name keeps: ${PROJECT_HOME}//x.hpl
    filenames = sorted({re.sub("/+", "/", e["filename"]) for e in events if e["filename"]})
    if filenames != [FILENAME]:
        found.append(f"{PIPELINE} is named after {filenames}, not the client's file {FILENAME}")
    parents = sorted({e["parentJob"] for e in events if e["parentJob"]})
    if parents != [PARENT]:
        found.append(f"the parent of {PIPELINE} is {parents}, not the client's action {PARENT}")
    return found


def main(path):
    deadline = time.time() + 60
    while time.time() < deadline and not arrived(lines(path)):
        time.sleep(1)
    time.sleep(3)
    kept = lines(path)
    found = problems(kept)
    print(f"{len(kept)} OpenLineage events, {len(pipeline_events(kept))} of {PIPELINE}")
    for problem in found:
        print("PROBLEM: " + problem)
    if found:
        return 1
    print(f"{PIPELINE} ran on the Hop Server as one run, the client's, with the file it wrote there")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))

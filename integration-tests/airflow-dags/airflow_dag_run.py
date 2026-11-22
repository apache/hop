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
"""Runs a DAG of the Hop DAG bundle in the Airflow container and checks the outcome.

Exits 0 when every check holds, 1 otherwise, so a Shell action fails the test workflow.
Standard library only: the test image has Python 3 but no Airflow client.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def call(base, method, path, token=None, body=None, accept_json=True):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(base + path, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    if accept_json:
        request.add_header("Accept", "application/json")
    if token:
        request.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            text = response.read().decode("utf-8")
            return response.status, (json.loads(text) if text else None)
    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(text)
        except ValueError:
            return e.code, text


def fail(message):
    print("FAILED: " + message, flush=True)
    sys.exit(1)


def wait(what, check, timeout, interval=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = check()
        if value:
            return value
        time.sleep(interval)
    fail("timed out after %d s waiting for %s" % (timeout, what))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://airflow:8080")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="admin")
    parser.add_argument("--dag", required=True)
    parser.add_argument("--conf", default="{}", help="the run conf, as a JSON object")
    parser.add_argument("--expect", choices=["success", "failed"], default="success")
    parser.add_argument("--log-contains", action="append", default=[])
    parser.add_argument("--xcom", action="append", default=[], help="KEY=VALUE in the return value")
    parser.add_argument("--schedule", help="the expected cron schedule of the DAG")
    parser.add_argument("--stop-after", type=int, help="mark the run failed this many seconds after the task starts")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    status, body = call(args.url, "POST", "/auth/token", body={"username": args.user, "password": args.password})
    if status not in (200, 201):
        fail("no token: HTTP %s %s" % (status, body))
    token = body["access_token"]
    dag = urllib.parse.quote(args.dag, safe="")

    def dag_listed():
        s, b = call(args.url, "GET", "/api/v2/dags/" + dag, token)
        return b if s == 200 else None

    details = wait("DAG " + args.dag, dag_listed, args.timeout)
    if args.schedule is not None:
        schedule = details.get("timetable_summary")
        print("Schedule of %s: %s" % (args.dag, schedule), flush=True)
        if schedule != args.schedule:
            fail("schedule %r, expected %r" % (schedule, args.schedule))
    if details.get("is_paused"):
        fail("DAG %s is paused" % args.dag)

    run_id = "hop-it-" + uuid.uuid4().hex[:12]
    s, b = call(args.url, "POST", "/api/v2/dags/%s/dagRuns" % dag, token,
                {"dag_run_id": run_id, "logical_date": None, "conf": json.loads(args.conf)})
    if s not in (200, 201):
        fail("trigger: HTTP %s %s" % (s, b))
    print("Started DAG run %s of %s" % (run_id, args.dag), flush=True)
    run_path = "/api/v2/dags/%s/dagRuns/%s" % (dag, urllib.parse.quote(run_id, safe=""))
    task_path = run_path + "/taskInstances/run_workflow"

    if args.stop_after is not None:
        def task_running():
            s, b = call(args.url, "GET", task_path, token)
            return s == 200 and b.get("state") == "running"

        wait("the task to run", task_running, args.timeout)
        time.sleep(args.stop_after)
        s, b = call(args.url, "PATCH", run_path, token, {"state": "failed"})
        if s != 200:
            fail("stop: HTTP %s %s" % (s, b))
        print("Marked the run failed", flush=True)

        def task_ended():
            s, b = call(args.url, "GET", task_path, token)
            return s == 200 and b.get("state") not in ("running", "queued", "scheduled", None)

        wait("the task to end", task_ended, 60, 1)

    def run_finished():
        s, b = call(args.url, "GET", run_path, token)
        return b.get("state") if s == 200 and b.get("state") in ("success", "failed") else None

    state = wait("the run to finish", run_finished, args.timeout)
    print("DAG run %s: %s" % (run_id, state), flush=True)
    if state != args.expect:
        fail("state %s, expected %s" % (state, args.expect))

    if args.log_contains:
        def task_log():
            s, b = call(args.url, "GET", task_path + "/logs/1?full_content=true", token)
            content = b.get("content", []) if isinstance(b, dict) else []
            return "\n".join(e.get("event", "") if isinstance(e, dict) else str(e) for e in content)

        # The last lines of a stopped task can reach the log a little after the run has ended.
        deadline = time.time() + 30
        log = task_log()
        while any(t not in log for t in args.log_contains) and time.time() < deadline:
            time.sleep(2)
            log = task_log()
        for text in args.log_contains:
            if text not in log:
                print(log[-3000:], flush=True)
                fail("the task log does not contain %r" % text)
            print("Task log contains %r" % text, flush=True)

    if args.xcom:
        s, b = call(args.url, "GET", task_path + "/xcomEntries/return_value", token)
        value = b.get("value") if s == 200 else None
        if isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, dict):
            fail("no return value: HTTP %s %s" % (s, b))
        for check in args.xcom:
            key, expected = check.split("=", 1)
            actual = value.get(key)
            actual = json.dumps(actual) if isinstance(actual, (bool, int, float)) else str(actual)
            if actual != expected:
                fail("return value %s is %r, expected %r" % (key, actual, expected))
            print("Return value %s = %s" % (key, actual), flush=True)


if __name__ == "__main__":
    main()

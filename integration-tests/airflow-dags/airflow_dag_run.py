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
    parser.add_argument("--xcom", action="append", default=[], help="KEY=VALUE in the outcome of the task")
    parser.add_argument("--xcom-task", help="the task whose outcome --xcom checks; default: run_workflow, or the last task to end")
    parser.add_argument("--task-state", action="append", default=[], help="TASK=STATE of one task of the run")
    parser.add_argument("--task-ids", help="the run has exactly these tasks, separated by commas")
    parser.add_argument("--schedule", help="the expected cron schedule of the DAG, or None")
    parser.add_argument("--detail", action="append", default=[], help="KEY=JSON value in the details of the DAG")
    parser.add_argument("--tag", action="append", default=[], help="a tag the DAG has")
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
        schedule = str(details.get("timetable_summary"))
        print("Schedule of %s: %s" % (args.dag, schedule), flush=True)
        if schedule != args.schedule:
            fail("schedule %r, expected %r" % (schedule, args.schedule))
    if args.detail or args.tag:
        s, dag_details = call(args.url, "GET", "/api/v2/dags/%s/details" % dag, token)
        if s != 200:
            fail("DAG details: HTTP %s %s" % (s, dag_details))
        for detail in args.detail:
            key, value = detail.split("=", 1)
            print("%s of %s: %r" % (key, args.dag, dag_details.get(key)), flush=True)
            if dag_details.get(key) != json.loads(value):
                fail("%s %r, expected %s" % (key, dag_details.get(key), value))
        tags = [t.get("name") if isinstance(t, dict) else t for t in dag_details.get("tags") or []]
        for tag in args.tag:
            if tag not in tags:
                fail("tag %s not in %s" % (tag, tags))
    if details.get("is_paused"):
        fail("DAG %s is paused" % args.dag)

    run_id = "hop-it-" + uuid.uuid4().hex[:12]
    s, b = call(args.url, "POST", "/api/v2/dags/%s/dagRuns" % dag, token,
                {"dag_run_id": run_id, "logical_date": None, "conf": json.loads(args.conf)})
    if s not in (200, 201):
        fail("trigger: HTTP %s %s" % (s, b))
    print("Started DAG run %s of %s" % (run_id, args.dag), flush=True)
    run_path = "/api/v2/dags/%s/dagRuns/%s" % (dag, urllib.parse.quote(run_id, safe=""))
    tasks_path = run_path + "/taskInstances"

    def task_instances():
        s, b = call(args.url, "GET", tasks_path + "?limit=1000", token)
        instances = b.get("task_instances", []) if s == 200 and isinstance(b, dict) else []
        return sorted(instances, key=lambda t: t.get("start_date") or "~")

    if args.stop_after is not None:
        def task_running():
            return any(t.get("state") == "running" for t in task_instances())

        wait("a task to run", task_running, args.timeout)
        time.sleep(args.stop_after)
        s, b = call(args.url, "PATCH", run_path, token, {"state": "failed"})
        if s != 200:
            fail("stop: HTTP %s %s" % (s, b))
        print("Marked the run failed", flush=True)

        def tasks_ended():
            return all(t.get("state") not in ("running", "queued", "scheduled") for t in task_instances())

        wait("the tasks to end", tasks_ended, 60, 1)

    def run_finished():
        s, b = call(args.url, "GET", run_path, token)
        return b.get("state") if s == 200 and b.get("state") in ("success", "failed") else None

    state = wait("the run to finish", run_finished, args.timeout)
    print("DAG run %s: %s" % (run_id, state), flush=True)
    instances = task_instances()
    print("Tasks: " + ", ".join("%s=%s" % (t["task_id"], t.get("state")) for t in instances), flush=True)
    if state != args.expect:
        fail("state %s, expected %s" % (state, args.expect))

    states = {t["task_id"]: t.get("state") for t in instances}
    if args.task_ids is not None:
        expected_ids = sorted(args.task_ids.split(","))
        if sorted(states) != expected_ids:
            fail("tasks %s, expected %s" % (sorted(states), expected_ids))
        print("Tasks are %s" % ", ".join(expected_ids), flush=True)
    for check in args.task_state:
        task, expected = check.split("=", 1)
        if task not in states:
            fail("the run has no task %s" % task)
        if states[task] != expected:
            fail("task %s is %s, expected %s" % (task, states[task], expected))
        print("Task %s: %s" % (task, expected), flush=True)

    if args.log_contains:
        def task_log():
            parts = []
            for t in task_instances():
                if (t.get("try_number") or 0) < 1:
                    continue
                path = "%s/%s/logs/%d?full_content=true" % (
                    tasks_path, urllib.parse.quote(t["task_id"], safe=""), t["try_number"])
                s, b = call(args.url, "GET", path, token)
                content = b.get("content", []) if isinstance(b, dict) else []
                # The Java SDK puts the exception of a failed task in a field of its own
                parts.extend(
                    (e.get("event", "") + (": " + str(e["error"]) if e.get("error") else ""))
                    if isinstance(e, dict) else str(e)
                    for e in content)
            return "\n".join(parts)

        # The last lines of a stopped task can reach the log a little after the run has ended.
        deadline = time.time() + 30
        log = task_log()
        while any(t not in log for t in args.log_contains) and time.time() < deadline:
            time.sleep(2)
            log = task_log()
        for text in args.log_contains:
            if text not in log:
                print(log[-3000:], flush=True)
                fail("the task logs do not contain %r" % text)
            print("Task logs contain %r" % text, flush=True)

    if args.xcom:
        task = args.xcom_task
        if task is None:
            ended = [t["task_id"] for t in instances if t.get("end_date")]
            task = "run_workflow" if "run_workflow" in states else (
                max((t for t in instances if t.get("end_date")), key=lambda t: t["end_date"])["task_id"] if ended else None)
        if task is None:
            fail("no task has ended")
        value = None
        for key in ("hop_outcome", "return_value"):
            s, b = call(args.url, "GET", "%s/%s/xcomEntries/%s" % (
                tasks_path, urllib.parse.quote(task, safe=""), key), token)
            value = b.get("value") if s == 200 else None
            if isinstance(value, str):
                value = json.loads(value)
            if isinstance(value, dict):
                break
        if not isinstance(value, dict):
            fail("no outcome for task %s: HTTP %s %s" % (task, s, b))
        for check in args.xcom:
            key, expected = check.split("=", 1)
            actual = value.get(key)
            actual = json.dumps(actual) if isinstance(actual, (bool, int, float)) else str(actual)
            if actual != expected:
                fail("outcome %s of task %s is %r, expected %r" % (key, task, actual, expected))
            print("Outcome %s of task %s = %s" % (key, task, actual), flush=True)


if __name__ == "__main__":
    main()

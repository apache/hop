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

"""A small Bulk API 2.0 stub for the Hop integration tests.

It stores the uploaded CSV and returns it as success and failed results. A row
is a failure when any cell is the text FAIL. Result rows are reversed so the
Hop transform has to match them instead of trusting file order.
"""

import csv
import io
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKEN = "Bearer test-token"
JOBS = {}
COUNTER = 0
DELIMITERS = {
    "COMMA": ",",
    "SEMICOLON": ";",
    "TAB": "\t",
    "PIPE": "|",
    "CARET": "^",
    "BACKQUOTE": "`",
}
INGEST = re.compile(r"^/services/data/v[^/]+/jobs/ingest(?:/([^/]+))?(?:/(batches|successfulResults|failedResults))?$")
QUERY = re.compile(r"^/services/data/v[^/]+/jobs/query(?:/([^/]+))?(?:/(results))?$")


def new_id():
    global COUNTER
    COUNTER += 1
    return f"001{COUNTER:015d}"


def read_csv(text, delimiter):
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows = [row for row in reader if any(cell != "" for cell in row)]
    if not rows:
        return [], []
    width = len(rows[0])
    normalized = []
    for row in rows:
        padded = list(row) + [""] * (width - len(row))
        normalized.append(padded[:width])
    return normalized[0], normalized[1:]


def write_csv(header, rows, delimiter, line_ending):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, delimiter=delimiter, lineterminator=line_ending, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/health":
            self.respond(200, "ok", "text/plain")
            return
        if not self.authorized():
            return
        ingest = INGEST.match(path)
        if ingest:
            self.get_ingest(ingest.group(1), ingest.group(2))
            return
        query = QUERY.match(path)
        if query:
            self.get_query(query.group(1), query.group(2))
            return
        self.respond(404, "not found", "text/plain")

    def do_POST(self):
        if not self.authorized():
            return
        body = self.body()
        path = self.path.split("?", 1)[0]
        if INGEST.match(path):
            payload = json.loads(body.decode("utf-8"))
            job_id = new_id()
            JOBS[job_id] = {
                "kind": "ingest",
                "state": "Open",
                "object": payload.get("object"),
                "operation": payload.get("operation"),
                "delimiter": DELIMITERS.get(payload.get("columnDelimiter"), ","),
                "line_ending": "\r\n" if payload.get("lineEnding") == "CRLF" else "\n",
                "csv": "",
            }
            self.respond(200, json.dumps({"id": job_id, "state": "Open"}), "application/json")
            return
        if QUERY.match(path):
            job_id = new_id()
            JOBS[job_id] = {"kind": "query", "state": "JobComplete"}
            self.respond(200, json.dumps({"id": job_id, "state": "JobComplete"}), "application/json")
            return
        self.respond(404, "not found", "text/plain")

    def do_PUT(self):
        if not self.authorized():
            return
        ingest = INGEST.match(self.path.split("?", 1)[0])
        if not ingest or ingest.group(2) != "batches":
            self.respond(404, "not found", "text/plain")
            return
        job = JOBS.get(ingest.group(1))
        if job is None:
            self.respond(404, "not found", "text/plain")
            return
        job["csv"] = self.body().decode("utf-8")
        self.respond(201, "", "text/plain")

    def do_PATCH(self):
        if not self.authorized():
            return
        path = self.path.split("?", 1)[0]
        ingest = INGEST.match(path)
        query = QUERY.match(path)
        job_id = ingest.group(1) if ingest else query.group(1) if query else None
        job = JOBS.get(job_id) if job_id else None
        if job is None:
            self.respond(404, "not found", "text/plain")
            return
        payload = json.loads(self.body().decode("utf-8") or "{}")
        state = payload.get("state", "JobComplete")
        job["state"] = "JobComplete" if state == "UploadComplete" else state
        self.respond(200, json.dumps(self.job_json(job_id, job)), "application/json")

    def get_ingest(self, job_id, suffix):
        job = JOBS.get(job_id)
        if job is None:
            self.respond(404, "not found", "text/plain")
            return
        if suffix is None:
            self.respond(200, json.dumps(self.job_json(job_id, job)), "application/json")
            return
        header, rows = read_csv(job.get("csv", ""), job["delimiter"])
        success = []
        failed = []
        for row in reversed(rows):
            if any(cell == "FAIL" for cell in row):
                failed.append(row + ["", "simulated failure"])
            else:
                id_index = header.index("Id") if "Id" in header else -1
                sf_id = row[id_index] if id_index >= 0 and row[id_index] else new_id()
                success.append(row + [sf_id, "true"])
        if suffix == "successfulResults":
            text = write_csv(header + ["sf__Id", "sf__Created"], success, job["delimiter"], job["line_ending"])
        else:
            text = write_csv(header + ["sf__Id", "sf__Error"], failed, job["delimiter"], job["line_ending"])
        self.respond(200, text, "text/csv")

    def get_query(self, job_id, suffix):
        job = JOBS.get(job_id)
        if job is None:
            self.respond(404, "not found", "text/plain")
            return
        if suffix is None:
            self.respond(200, json.dumps(self.job_json(job_id, job)), "application/json")
            return
        locator = self.headers.get("Sforce-Locator")
        if locator == "page2":
            body = "Id,Name\n001000000000003AAA,Beta\n"
            next_locator = "null"
        else:
            body = "Id,Name\n001000000000001AAA,Acme\n001000000000002AAA,\n"
            next_locator = "page2"
        self.send_response(200)
        encoded = body.encode("utf-8")
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Sforce-Locator", next_locator)
        self.end_headers()
        self.wfile.write(encoded)

    def job_json(self, job_id, job):
        return {
            "id": job_id,
            "state": job["state"],
            "operation": job.get("operation"),
            "object": job.get("object"),
            "numberRecordsProcessed": 0,
            "numberRecordsFailed": 0,
        }

    def authorized(self):
        if self.headers.get("Authorization") == TOKEN:
            return True
        self.respond(401, '[{"errorCode":"INVALID_SESSION_ID"}]', "application/json")
        return False

    def body(self):
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b""

    def respond(self, status, text, content_type):
        encoded = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", 8080), Handler)
    server.serve_forever()

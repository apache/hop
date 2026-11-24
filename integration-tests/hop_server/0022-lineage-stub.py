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
"""An OpenLineage collector for main-0022-lineage-on-hop-server.

It accepts what the OpenLineage lineage sink sends (POST /api/v1/lineage), from the client and from
the Hop Server alike, and writes one JSON line per event to the log file given on the command line.

Usage: python3 -I 0022-lineage-stub.py <port> <log>
"""

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

LOCK = threading.Lock()


def datasets(event, side):
    return [
        {"namespace": d.get("namespace"), "name": d.get("name")} for d in (event.get(side) or [])
    ]


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.reply(200, {"status": "up"})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if not urlparse(self.path).path.endswith("/api/v1/lineage"):
            self.reply(404, {"message": "not found"})
            return
        lines = []
        for event in body if isinstance(body, list) else [body]:
            run = event.get("run") or {}
            job = event.get("job") or {}
            parent = (run.get("facets") or {}).get("parent") or {}
            lines.append(
                {
                    "eventType": event.get("eventType"),
                    "runId": run.get("runId"),
                    "job": job.get("name"),
                    "filename": ((job.get("facets") or {}).get("hop") or {}).get("filename"),
                    "parentJob": (parent.get("job") or {}).get("name"),
                    "inputs": datasets(event, "inputs"),
                    "outputs": datasets(event, "outputs"),
                }
            )
        self.reply(201, {})
        with LOCK, open(LOG, "a", encoding="utf-8") as log:
            for line in lines:
                log.write(json.dumps(line) + "\n")

    def reply(self, status, payload):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    LOG = sys.argv[2]
    ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()

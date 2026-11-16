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
"""A stand-in for DataHub GMS that keeps what the Hop DataHub lineage sink writes to it.

POST /aspects?action=ingestProposalBatch  accepts and keeps the proposals
POST /api/graphql                         answers that DataHub has none of the entities asked for
GET  /proposals[?aspect=<name>]           the proposals kept so far, their aspect values parsed
GET  /health                              200 once it listens
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PROPOSALS = []
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/health":
            self.reply(200, {"status": "up"})
        elif url.path == "/proposals":
            aspect = parse_qs(url.query).get("aspect", [None])[0]
            with LOCK:
                kept = [p for p in PROPOSALS if aspect is None or p["aspectName"] == aspect]
            self.reply(200, kept)
        else:
            self.reply(404, {"message": "not found"})

    def do_POST(self):
        url = urlparse(self.path)
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if url.path == "/aspects" and "ingestProposalBatch" in url.query:
            with LOCK:
                for proposal in body.get("proposals", []):
                    value = proposal.get("aspect", {}).get("value", "{}")
                    proposal["value"] = json.loads(value)
                    PROPOSALS.append(proposal)
            self.reply(200, {"value": []})
        elif url.path == "/api/graphql":
            self.reply(200, {"data": {"entities": []}})
        else:
            self.reply(404, {"message": "not found"})

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
    ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8080"))), Handler).serve_forever()

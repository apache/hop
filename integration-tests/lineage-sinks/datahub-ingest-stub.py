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
"""A stand-in for DataHub's metadata service (GMS), for the lineage sink integration tests.

It accepts what the DataHub lineage sink writes (POST /aspects?action=ingestProposalBatch),
answers its GraphQL questions as a DataHub that has none of the entities asked for, and writes
one JSON line per proposal (entity URN, aspect, the Authorization header) to the log file given
on the command line, with the value of assertion aspects, so a test can check which runs reported
their lineage here.

Usage: python3 -I datahub-ingest-stub.py <port> <proposal log>
"""

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.reply(200, {"status": "up"})

    def do_POST(self):
        url = urlparse(self.path)
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        authorization = self.headers.get("Authorization")
        if url.path.endswith("/aspects") and "ingestProposalBatch" in url.query:
            with LOCK, open(LOG, "a", encoding="utf-8") as log:
                for proposal in body.get("proposals", []):
                    line = {
                        "entityUrn": proposal.get("entityUrn"),
                        "aspectName": proposal.get("aspectName"),
                        "authorization": authorization,
                    }
                    if (proposal.get("aspectName") or "").startswith("assertion"):
                        # What an assertion is about and how its run went
                        line["value"] = json.loads(proposal.get("aspect", {}).get("value", "{}"))
                    log.write(json.dumps(line) + "\n")
            self.reply(200, {"value": []})
        elif url.path.endswith("/api/graphql"):
            # As DataHub answers for entities it doesn't have: one entry per URN asked about, with
            # nothing in it, so the sink writes the schemas Hop knows.
            urns = (body.get("variables") or {}).get("urns") or []
            self.reply(200, {"data": {"entities": [{"urn": urn, "exists": False} for urn in urns]}})
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
    LOG = sys.argv[2]
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()

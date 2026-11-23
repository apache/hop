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

"""A stand-in for DataHub's GraphQL API, for the data catalog integration tests (issue #9323).

It answers the queries of the DataHub data catalog type for a few fixed datasets, refuses a
request without the expected token, and writes one line per request (operation and URN) to the
log file given on the command line, so a test can check what Hop asked.

Usage: python3 -I datahub-stub.py <port> <request log>
"""

import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKEN = "Bearer it-token"
HOUR_MS = 3600 * 1000
CUSTOMERS = "urn:li:dataset:(urn:li:dataPlatform:hive,it.customers,PROD)"


def tags(*names):
    return {"tags": [{"tag": {"urn": "urn:li:tag:" + n, "name": n, "properties": None}} for n in names]}


def terms(path):
    """A term created in the DataHub UI: a generated id in its URN, its path in the parent nodes."""
    *parents, name = path.split(".")
    term = {"urn": "urn:li:glossaryTerm:4f1c", "name": "4f1c", "hierarchicalName": "4f1c",
            "properties": {"name": name},
            "parentNodes": {"nodes": [{"properties": {"name": p}} for p in reversed(parents)]}}
    return {"terms": [{"term": term}]}


# The customers table: email is tagged PII in the ingested schema, name has a glossary term added
# in the DataHub UI (a v2 field path), id and city carry nothing.
CUSTOMERS_SCHEMA = {
    "schemaMetadata": {"fields": [
        {"fieldPath": "id", "nativeDataType": "INTEGER", "type": "NUMBER", "nullable": False, "globalTags": None, "glossaryTerms": None},
        {"fieldPath": "email", "nativeDataType": "VARCHAR(255)", "type": "STRING", "nullable": True, "globalTags": tags("PII"), "glossaryTerms": None},
        {"fieldPath": "name", "nativeDataType": "VARCHAR(100)", "type": "STRING", "nullable": True, "globalTags": None, "glossaryTerms": None},
        {"fieldPath": "city", "nativeDataType": "VARCHAR(100)", "type": "STRING", "nullable": True, "globalTags": None, "glossaryTerms": None},
    ]},
    "editableSchemaMetadata": {"editableSchemaFieldInfo": [
        {"fieldPath": "[version=2.0].[type=string].name", "globalTags": None, "glossaryTerms": terms("Classification.Personal")},
    ]},
}
TEN_MINUTES_MS = 10 * 60 * 1000


def dataset(urn):
    """The state of a dataset: (deprecated, age of the last change in ms, assertion result)."""
    healthy = (False, TEN_MINUTES_MS, "SUCCESS")
    if urn == "urn:li:dataset:(urn:li:dataPlatform:postgres,sales.public.orders,PROD)":
        return healthy
    if urn.startswith("urn:li:dataset:(urn:li:dataPlatform:file,") and urn.endswith(
        "/files/customers.csv,PROD)"
    ):
        return healthy
    return {
        "urn:li:dataset:(urn:li:dataPlatform:hive,it.healthy,PROD)": healthy,
        "urn:li:dataset:(urn:li:dataPlatform:hive,it.deprecated,PROD)": (True, TEN_MINUTES_MS, "SUCCESS"),
        "urn:li:dataset:(urn:li:dataPlatform:hive,it.stale,PROD)": (False, 72 * HOUR_MS, "SUCCESS"),
        "urn:li:dataset:(urn:li:dataPlatform:hive,it.failing,PROD)": (False, TEN_MINUTES_MS, "FAILURE"),
        CUSTOMERS: healthy,
    }.get(urn)


def answer(query, urn):
    now = int(time.time() * 1000)
    if "appConfig" in query:
        return {"me": {"corpUser": {"username": "hop-it"}}, "appConfig": {"appVersion": "stub"}}
    state = dataset(urn)
    if state is None:
        return {"dataset": None}
    deprecated, age, result = state
    if "hopDatasetExists" in query:
        return {"dataset": {"urn": urn, "exists": True}}
    if "hopDatasetDeprecation" in query:
        deprecation = {"deprecated": True, "note": "Use it.healthy", "decommissionTime": 0}
        return {"dataset": {"deprecation": deprecation if deprecated else None}}
    if "hopDatasetOperations" in query:
        return {"dataset": {"operations": [{"lastUpdatedTimestamp": now - age}]}}
    if "hopDatasetSchema" in query:
        return {"dataset": CUSTOMERS_SCHEMA if urn == CUSTOMERS else {"schemaMetadata": None}}
    if "hopDatasetAssertions" in query:
        run = {"timestampMillis": now - HOUR_MS, "result": {"type": result}}
        assertion = {
            "urn": "urn:li:assertion:" + urn,
            "info": {"description": "Row count"},
            "runEvents": {"runEvents": [run]},
        }
        return {"dataset": {"assertions": {"total": 1, "assertions": [assertion]}}}
    return None


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.reply(200, {"status": "up"})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        query = body.get("query", "")
        urn = (body.get("variables") or {}).get("urn", "")
        operation = query.split("(")[0].split("{")[0].split()[-1] if query.split() else ""
        with open(LOG, "a", encoding="utf-8") as log:
            log.write(f"{operation} {urn}\n")
        if self.headers.get("Authorization") != TOKEN:
            self.reply(401, {"message": "Unauthorized"})
            return
        data = answer(query, urn)
        if data is None:
            self.reply(200, {"errors": [{"message": "Unknown query"}]})
        else:
            self.reply(200, {"data": data})

    def reply(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    LOG = sys.argv[2]
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()

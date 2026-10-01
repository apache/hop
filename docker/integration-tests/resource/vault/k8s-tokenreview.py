#!/usr/bin/env python3
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
"""Minimal Kubernetes TokenReview API for Vault Kubernetes auth integration tests.

Also enables Vault Kubernetes auth against this mock. The Hop docker suite uses
``--abort-on-container-exit``, so this process must keep running after setup.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer


NAMESPACE = "default"
SA_NAME = "hop"
JWT_PATH = os.environ.get("JWT_PATH", "/jwt/token")
PORT = int(os.environ.get("PORT", "8080"))
VAULT_ADDR = os.environ.get("VAULT_ADDR", "http://vault:8200").rstrip("/")
VAULT_TOKEN = os.environ.get("VAULT_TOKEN", "myroot")
KUBERNETES_HOST = os.environ.get("KUBERNETES_HOST", "http://k8s-mock:8080")
# RS256 JWT for SA default/hop. Vault does not verify the signature when no
# public keys are configured, but it does require an RSA/ECDSA algorithm.
HOP_SA_JWT = (
    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImhvcC10b2tlbiIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJob3AiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiIxIiwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50OmRlZmF1bHQ6aG9wIn0."
    "diHYA5MSwzDlpHZo6wuPsndgG-TrsGOHr87IDzfqq9-pZG21iM1bCsHt0i1liioEHe7F4XQLlXziDBHLGWKcj_PGjiP4K9FdkOCBjmyJF6hTNN0OrgHzLCKK3RBDVTXVD5xg-kCAhCeIoebg1eCuYTf6r8uiiodKSS8k1ImSdnOjM5E3TMBQpCYdnOIEmUw0uZq9Z1ome2XVrV9h173wp5BHI_fTjJpj7jVwDKhoP8RC1OWMtKsvYKMlkH4sXJqeBm60BuOT1GjLOJ9k0IkOJacLxYlyM1pz73j3J8LaD91_cAeTpyzg6MImCjdPI35pmuhzML4cTubrd097KMWZDg"
)

ready = False


def jwt_subject(token: str) -> str:
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return str(payload.get("sub", ""))
    except Exception:
        return ""


def write_jwt() -> None:
    directory = os.path.dirname(JWT_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(JWT_PATH, "w", encoding="utf-8") as handle:
        handle.write(HOP_SA_JWT)


def vault_write(path: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        VAULT_ADDR + path,
        data=data,
        headers={
            "X-Vault-Token": VAULT_TOKEN,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10)
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        if error.code == 400 and "already in use" in body:
            return
        raise RuntimeError(f"Vault {path} returned {error.code}: {body}") from error


def wait_for_vault() -> None:
    last_error = None
    for _ in range(30):
        try:
            urllib.request.urlopen(VAULT_ADDR + "/v1/sys/health", timeout=3)
            return
        except Exception as error:  # noqa: BLE001
            last_error = error
            time.sleep(1)
    raise RuntimeError(f"Vault at {VAULT_ADDR} did not become ready: {last_error}")


def configure_vault() -> None:
    wait_for_vault()
    vault_write("/v1/sys/auth/kubernetes", {"type": "kubernetes"})
    vault_write(
        "/v1/auth/kubernetes/config",
        {
            "kubernetes_host": KUBERNETES_HOST,
            "disable_iss_validation": True,
            "disable_local_ca_jwt": True,
            "token_reviewer_jwt": "dummy",
        },
    )
    vault_write(
        "/v1/sys/policies/acl/hop-read",
        {"policy": 'path "secret/data/*" { capabilities = ["read"] }'},
    )
    vault_write(
        "/v1/auth/kubernetes/role/hop",
        {
            "bound_service_account_names": SA_NAME,
            "bound_service_account_namespaces": NAMESPACE,
            "policies": ["hop-read"],
            "ttl": "1h",
        },
    )


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/ready"):
            if ready:
                self._send(200, b"ok", "text/plain")
            else:
                self._send(503, b"not ready", "text/plain")
            return
        if self.path.startswith("/healthz"):
            self._send(200, b"ok", "text/plain")
            return
        self._send(200, b'{"kind":"APIVersions","versions":["v1"]}', "application/json")

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b"{}"
        token = ""
        try:
            token = json.loads(body.decode("utf-8")).get("spec", {}).get("token", "")
        except Exception:
            token = ""
        sub = jwt_subject(token)
        authenticated = sub.startswith("system:serviceaccount:")
        review = {
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": authenticated,
                "user": {
                    "username": sub,
                    "uid": "1",
                    "groups": ["system:serviceaccounts"],
                },
            },
        }
        self._send(200, json.dumps(review).encode("utf-8"), "application/json")

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    write_jwt()
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    configure_vault()
    ready = True
    thread.join()

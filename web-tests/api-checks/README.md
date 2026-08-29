<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Hop Server API access checks (manual)

Manual reproduction and verification scripts for the RBAC guard on the embedded
Hop Server API (`/hop/*`) in Hop Web — issue
https://github.com/apache/hop/issues/8150.

These drive the endpoints with plain `curl`, so they are handy for exploring a
running container by hand or from a browser. The **automated** regression test
is `HopServerApiRbacTest` in this module (`web-tests`); these scripts are the
human-facing counterpart, not part of the CI gate.

## Expected behaviour

| Mode | Caller | Read (`status`) | Deploy / run / remove |
|---|---|---|---|
| `BASIC` / `EXTERNAL` / `OAUTH2` | Read-only role | 200 | 403 |
| `BASIC` / `EXTERNAL` / `OAUTH2` | Operator | 200 | run/stop yes, deploy/remove 403 |
| `BASIC` / `EXTERNAL` / `OAUTH2` | User / Admin | 200 | 200 |
| `NONE` (default) | anyone | 403 | 403 |
| `NONE` + `allowUnauthenticatedServerApi` | anyone | 200 | 200 |

## Scripts

| Script | Purpose |
|---|---|
| `probe-api.sh <base-url> [user:pass]` | Sweep every `/hop/*` endpoint for one identity; report reachable / login-redirect / blocked |
| `make-payload.sh <pipeline.hpl> [out.xml]` | Build a valid `addPipeline` body from a `.hpl` (embeds a `local` run configuration) |
| `exec-test.sh <base-url> [user:pass]` | End-to-end: `addPipeline` -> `startPipeline` -> `pipelineStatus` -> `removePipeline`; exit 0 = the pipeline ran |
| `run-matrix.sh <base-url>` | Run `exec-test.sh` for anonymous + the four demo roles and print a summary |

## Bring up a container to test against

From the repository root, using the local development image:

```bash
# BASIC auth, demo users admin/developer/operator/viewer (password = username)
./docker/run-hop-web-local-with-basic.sh --quick        # -> http://localhost:8080

# default configuration, mode NONE (server API closed by default)
docker run -d --name hopweb-none -p 8081:8080 hop-web:local

# mode NONE, server API explicitly opened
docker run -d --name hopweb-none-open -p 8082:8080 \
  -e HOP_WEB_ALLOW_UNAUTHENTICATED_SERVER_API=true hop-web:local
```

## Run the checks

```bash
cd web-tests/api-checks

# Per-endpoint verdict for the read-only role (status 200, mutations 403)
./probe-api.sh http://localhost:8080 viewer:viewer

# Full deploy+run matrix across roles
./run-matrix.sh http://localhost:8080

# The finding: a read-only user must not be able to run a pipeline
./make-payload.sh ../../integration-tests/beam_directrunner/0001-generate-rows.hpl payload.xml
./exec-test.sh http://localhost:8080 viewer:viewer     # expect: blocked

# Default NONE image must refuse the server API
./exec-test.sh http://localhost:8081                   # expect: blocked
```

### In a browser

Log in to `http://localhost:8080` as `viewer`, then in the address bar:

* `http://localhost:8080/hop/status/?xml=Y` — renders (read allowed)
* `http://localhost:8080/hop/startWorkflow/?name=x` — `403 Access denied: run.execute required`

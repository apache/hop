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

# Local Hop Web multi-user auth config

Tomcat BASIC authentication samples for testing Hop Web role-based UI enablement.

## Files

| File | Destination in container |
|---|---|
| `tomcat-users.xml` | `/usr/local/tomcat/conf/tomcat-users.xml` (via `run-web.sh` from `/config`) |
| `web.xml` | `/usr/local/tomcat/webapps/ROOT/WEB-INF/web.xml` (via `run-web.sh` from `/config`) |
| `security-config.json` | Sample only — optional Hop role mapping under `HOP_CONFIG_FOLDER/security/` |

Mount this directory at `/config` when starting the container (`run-hop-web-local-with-users.sh` does this).

Built-in role aliases already recognize `hop-admin`, `hop-user`, `hop-operator`, and `hop-readonly`, so the JSON sample is optional for this setup.

## Test users (local only)

| Username | Password | Container role | Hop role | Expected UI |
|---|---|---|---|---|
| `admin` | `admin` | `hop-admin` | Admin | Full access |
| `developer` | `developer` | `hop-user` | User | CRUD + execute |
| `operator` | `operator` | `hop-operator` | Operator | View + execute; no save/edit |
| `viewer` | `viewer` | `hop-readonly` | Read-only | View only |

**Do not use these passwords outside local development.**

## Run

```bash
./docker/run-hop-web-local-with-users.sh
```

Or mount manually:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/docker/local-auth-config:/config" \
  hop-web:local
```

Then open http://localhost:8080/ui and sign in with one of the users above.
The window title should show the username (e.g. `Hop - … [operator]`).

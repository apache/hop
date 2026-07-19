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

# Local Artifactory for Hop Marketplace testing

Use this stack to exercise plugin install/uninstall **without** ASF Nexus permissions.

## Security model (recommended)

| Action | Who |
|--------|-----|
| **Download / install plugins** (Hop marketplace) | **Anonymous** (read-only on `hop-plugins-local`) |
| **Publish plugin zips** (`publish-wave1-plugins.sh`) | **Admin** (or a deploy user) |
| UI administration | Admin only |

Nobody should put admin credentials into `hop-config.json` or day-to-day Hop usage.
Admin is only for publishing and one-time setup.

Recent Artifactory versions also **require PostgreSQL** (embedded DB is gone). This compose
stack runs Postgres + Artifactory OSS.

## Start

```bash
# First time or after a broken volume layout:
./docker/marketplace-artifactory/start.sh --reset

# Later:
./docker/marketplace-artifactory/start.sh
```

Wait until healthy:

```bash
curl -sf http://localhost:8082/artifactory/api/system/ping && echo OK
```

Open http://localhost:8082 and set the **admin** password on first login.

## Configure anonymous read (Artifactory **OSS** + Platform UI)

**Repo-create REST is Pro-only** on OSS (HTTP 400). Use the **JFrog Platform UI**
at http://localhost:8082/ui/ — menus move every release; use deep links.

**Step-by-step with current UI landmarks:** see **[JFROG-UI.md](./JFROG-UI.md)**.

Quick links (log in as admin first):

| Task | URL |
|------|-----|
| Repositories | http://localhost:8082/ui/admin/repositories |
| Anonymous access | http://localhost:8082/ui/admin/security/general |
| Permissions | http://localhost:8082/ui/admin/user_management/permissions |

Or print the same guide + verify:

```bash
./docker/marketplace-artifactory/configure-anonymous-read.sh
# after UI steps A–C:
export ARTIFACTORY_PASSWORD='your-admin-password'
./docker/marketplace-artifactory/configure-anonymous-read.sh
```

Repo URL for Hop:

```text
http://localhost:8082/artifactory/hop-plugins-local/
```

## Publish Wave 1 plugin zips (authenticated)

Build plugin modules, then deploy with admin (or a dedicated deploy user):

```bash
export ARTIFACTORY_URL=http://localhost:8082/artifactory/hop-plugins-local
export ARTIFACTORY_USER=admin
export ARTIFACTORY_PASSWORD='your-admin-password'
# Maven needs a matching <server> in ~/.m2/settings.xml (id = hop-plugins-local)
./docker/marketplace-artifactory/publish-wave1-plugins.sh
```

`~/.m2/settings.xml` snippet:

```xml
<servers>
  <server>
    <id>hop-plugins-local</id>
    <username>admin</username>
    <password>your-admin-password</password>
  </server>
</servers>
```

## Install from Hop (anonymous — no password)

`hop-config.json` (no username/password required once anonymous read is configured):

```json
{
  "marketplace": {
    "enabled": true,
    "groupId": "org.apache.hop",
    "defaultVersion": "2.19.0-SNAPSHOT",
    "repositories": [
      {
        "id": "local-artifactory",
        "url": "http://localhost:8082/artifactory/hop-plugins-local/"
      }
    ]
  }
}
```

```bash
export HOP_HOME=/path/to/hop
# Do NOT export admin password for install
./hop marketplace install hop-tech-parquet
```

Quick anonymous check:

```bash
# Expect 200 (or 404 if that path is empty), not 401
curl -sI "http://localhost:8082/artifactory/hop-plugins-local/org/apache/hop/" | head -1
```

If you still get **401**, anonymous access or the read permission is not applied — re-run
`configure-anonymous-read.sh` or the UI steps above.

### Optional: authenticated downloads

Corporate Artifactory that cannot allow anonymous can still use:

```bash
export HOP_MARKETPLACE_USERNAME=deploy-reader
export HOP_MARKETPLACE_PASSWORD='…'
```

Prefer a **read-only deploy user**, not admin.

## Security keys (compose bootstrap)

```bash
./docker/marketplace-artifactory/generate-keys.sh
```

| File | Size | How |
|------|------|-----|
| `keys/master.key` | 32 hex chars | `openssl rand -hex 16` |
| `keys/join.key` | 64 hex chars | `openssl rand -hex 32` |
| `.env` | compose env | master/join + Postgres password |

Gitignored. Passed as env vars into the container (not RO file mounts).

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `HTTP 401` on `hop marketplace install` | Enable anonymous + read permission (`configure-anonymous-read.sh`) |
| `Cannot start … PostgreSQL` | Use this compose stack; `./start.sh --reset` |
| Join key / startup hang | `generate-keys.sh` + `start.sh --reset` |
| Publish 401 | Admin `settings.xml` / `ARTIFACTORY_PASSWORD` for deploy only |

## Stop

```bash
docker compose -f docker/marketplace-artifactory/docker-compose.yml \
  --env-file docker/marketplace-artifactory/.env down

# Wipe all data:
docker compose -f docker/marketplace-artifactory/docker-compose.yml \
  --env-file docker/marketplace-artifactory/.env down -v
```

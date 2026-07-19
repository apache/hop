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

## Requirements

Recent **Artifactory OSS 7.x requires PostgreSQL** (the embedded database is gone). This
compose stack runs:

| Service | Purpose |
|---------|---------|
| `postgres` | Required DB for Artifactory |
| `artifactory` | Maven repo for Hop plugin zips |

Also requires Docker Compose and `openssl` on the host.

## Security keys

Artifactory needs a **master key** (AES-128) and a **join key**. Generate once:

```bash
./docker/marketplace-artifactory/generate-keys.sh
```

| File | Size | How |
|------|------|-----|
| `keys/master.key` | 32 hex chars | `openssl rand -hex 16` |
| `keys/join.key` | 64 hex chars | `openssl rand -hex 32` |
| `.env` | compose env | master/join + Postgres password |

Keys and `.env` are gitignored. Keys are passed into the container as
**environment variables** (`JF_SHARED_SECURITY_MASTER_KEY` /
`JF_SHARED_SECURITY_JOIN_KEY`), not bind-mounted files — Artifactory tries to
rewrite key files and RO mounts break startup.

## Start

```bash
# First time or after failed starts with an old volume layout:
./docker/marketplace-artifactory/start.sh --reset

# Normal start:
./docker/marketplace-artifactory/start.sh
```

First boot can take **2–4 minutes**. Watch logs:

```bash
docker logs -f hop-marketplace-artifactory
```

Healthy when:

```bash
curl -sf http://localhost:8082/artifactory/api/system/ping && echo OK
```

Open http://localhost:8082 and complete first-time admin password setup.

### Create the Maven repository

| Field | Value |
|-------|--------|
| Package type | Maven |
| Repository Key | `hop-plugins-local` |
| Repository Layout | maven-2-default |

Base URL:

```text
http://localhost:8082/artifactory/hop-plugins-local/
```

## Publish Wave 1 plugin zips

Build Hop plugins first, then:

```bash
export ARTIFACTORY_URL=http://localhost:8082/artifactory/hop-plugins-local
export ARTIFACTORY_USER=admin
export ARTIFACTORY_PASSWORD=…   # from first-time setup
./docker/marketplace-artifactory/publish-wave1-plugins.sh
```

## Point Hop marketplace at local Artifactory

```json
{
  "marketplace": {
    "enabled": true,
    "groupId": "org.apache.hop",
    "defaultVersion": "2.19.0-SNAPSHOT",
    "repositories": [
      {
        "id": "local-artifactory",
        "url": "http://localhost:8082/artifactory/hop-plugins-local/",
        "username": "admin"
      }
    ]
  }
}
```

Password via env (recommended — avoids storing secrets in hop-config.json):

```bash
export HOP_HOME=/path/to/hop
export HOP_MARKETPLACE_USERNAME=admin
export HOP_MARKETPLACE_PASSWORD='your-artifactory-password'
# aliases also work: ARTIFACTORY_USER / ARTIFACTORY_PASSWORD
./hop marketplace install hop-tech-parquet
./hop marketplace list
```

**HTTP 401** means Artifactory rejected anonymous download — set the env vars (or put
`password` on the repository object) and retry.

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Cannot start … database other than PostgreSQL` | Use this compose stack (includes Postgres). Wipe old volumes: `start.sh --reset` |
| `Join key is missing` | Run `generate-keys.sh`; ensure `.env` is passed to compose (`start.sh` does this) |
| `read-only file system` on join.key | Do not bind-mount key files; use env vars via `.env` |
| Stuck after earlier failed boots | `./start.sh --reset` to drop volumes |
| Low disk warning | Free space under Docker’s data root |

## Stop

```bash
docker compose -f docker/marketplace-artifactory/docker-compose.yml --env-file docker/marketplace-artifactory/.env down
# Wipe data:
docker compose -f docker/marketplace-artifactory/docker-compose.yml --env-file docker/marketplace-artifactory/.env down -v
```

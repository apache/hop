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

# Local Nexus for Hop Marketplace testing

Sonatype **Nexus Repository OSS** is the supported local Maven repository for
developing and testing the Hop plugin marketplace.

It is fully scriptable (no Pro-only APIs, no Postgres, no join keys).

## Security model

| Action | Who |
|--------|-----|
| `hop marketplace install` / GUI install | **Anonymous** read |
| `publish-marketplace-plugins.sh` | **admin** (or a deploy user) |

Do not put admin credentials in day-to-day `hop-config.json`.

## Start

```bash
./docker/marketplace-nexus/start.sh

# Wipe data and re-bootstrap:
./docker/marketplace-nexus/start.sh --reset

# Custom admin password:
NEXUS_ADMIN_PASSWORD='secret' ./docker/marketplace-nexus/start.sh
```

Default after bootstrap:

| | |
|--|--|
| UI | http://localhost:8081/ |
| Admin | `admin` / `hop-nexus-dev` (unless overridden) |
| Hosted repo | http://127.0.0.1:8081/repository/hop-plugins/ |

Prefer **`127.0.0.1`** over `localhost` in Hop config: on some hosts `localhost` resolves
to IPv6 (`::1`) while Docker only publishes IPv4, which surfaces as
`java.net.ConnectException` from the marketplace client.

First start can take **1–3 minutes** while Nexus initializes.

## Publish marketplace plugin zips

Plugin list and module paths come **only** from
`plugins/misc/marketplace/src/main/resources/org/apache/hop/marketplace/optional-plugins.yaml`.
Add a plugin there once; this script packages/deploys it automatically.

```bash
export NEXUS_PASSWORD=hop-nexus-dev   # or your NEXUS_ADMIN_PASSWORD

# Package every registry module, then deploy zips to local Nexus:
./docker/marketplace-nexus/publish-marketplace-plugins.sh --package

# Or package yourself, then deploy only:
# ./mvnw -pl … -am package -DskipTests
./docker/marketplace-nexus/publish-marketplace-plugins.sh

# Artifactory (or any Maven 2 repo):
# export ARTIFACTORY_URL='https://artifactory.example/artifactory/hop-plugins-local'
# export ARTIFACTORY_USER=deploy
# export ARTIFACTORY_PASSWORD=…
# export NEXUS_REPO_ID=artifactory
# ./docker/marketplace-nexus/publish-marketplace-plugins.sh --package
```

`publish-wave1-plugins.sh` remains as a deprecated alias of this script.

## Hop marketplace (anonymous install)

Config is always **`${HOP_CONFIG_FOLDER}/hop-config.json`**. Defaults are ASF
primary + Maven Central; add local Nexus and make it primary for dev:

```bash
cd /path/to/hop          # the unzipped hop-client directory
./hop marketplace repo add --id local-nexus \
  --url http://127.0.0.1:8081/repository/hop-plugins/ --primary
./hop marketplace repo list
./hop marketplace install hop-tech-parquet
# restore production defaults later:
./hop marketplace repo set-primary asf
# or: ./hop marketplace repo reset-defaults
```

Quick check:

```bash
# Expect 200 or 404, not 401
curl -sI "http://127.0.0.1:8081/repository/hop-plugins/" | head -1
```

## Smoke test (CLI install / list / validate / apply / uninstall)

```bash
# Nexus up + marketplace plugins published + hop client unzipped
./docker/marketplace-nexus/smoke-test.sh
```

Uses a temporary `HOP_CONFIG_FOLDER` (does not touch your real hop-config) and
installs two small plugins into `assemblies/client/target/hop` (override with
`HOP_DIR`).

## Scripts

| Script | Role |
|--------|------|
| `start.sh` | Compose up + wait + configure |
| `configure-nexus.sh` | Anonymous + hosted repo (idempotent) |
| `publish-marketplace-plugins.sh` | `deploy-file` of all zips from `optional-plugins.yaml` |
| `publish-wave1-plugins.sh` | Deprecated alias of `publish-marketplace-plugins.sh` |
| `smoke-test.sh` | Anonymous install / list / validate / apply / uninstall |

## Stop

```bash
docker compose -f docker/marketplace-nexus/docker-compose.yml down
# Wipe data:
docker compose -f docker/marketplace-nexus/docker-compose.yml down -v
```

## Production note

Released Hop plugins still go through the **ASF release process** to
`repository.apache.org` → Maven Central. This Nexus stack only mimics Maven
layout for local development.

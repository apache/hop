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

# Local JFrog Artifactory for Hop Marketplace testing

The counterpart of [`../marketplace-nexus`](../marketplace-nexus) for repositories
using **JFrog Artifactory OSS**. Use whichever matches the repository manager you
are developing against; Hop supports both.

Publishing is shared: `../marketplace-nexus/publish-marketplace-plugins.sh` already
deploys to Artifactory through its `ARTIFACTORY_*` environment variables. Only the
server and its configuration live here.

## Start

```bash
./docker/marketplace-artifactory/start.sh

# Wipe data and re-bootstrap:
./docker/marketplace-artifactory/start.sh --reset
```

Default after bootstrap:

| | |
|--|--|
| UI | http://localhost:8082/ui/ |
| Admin | `admin` / `password` |
| Maven repo | http://localhost:8082/artifactory/example-repo-local/ |

`example-repo-local` is the repository the OSS image ships. Creating one over REST is an
Artifactory Pro feature (`This REST API is available only in Artifactory Pro`), so
`start.sh` uses an existing repository rather than making one. Its generic package type
does not matter: Hop matches paths and `.zip` names and never asks what a repository
declares. On a Pro instance, point `ARTIFACTORY_REPO` at a real Maven repository instead.

The image is around 1.5 GB and **first start takes several minutes**. Port 8082 is the
JFrog platform router and serves every REST endpoint Hop uses; 8081 is the legacy
direct port and is published only for convenience.

## Why the stack has two containers

Artifactory 7.98 removed the bundled Derby database and refuses to start on anything else
(`DB Type derby is not allowed`), so the compose file runs PostgreSQL alongside it and
points Artifactory at it with the `JF_SHARED_DATABASE_*` variables.

It also sets `JF_SHARED_SECURITY_MASTERKEY` and `JF_SHARED_SECURITY_JOINKEY`. Supplying
either one turns off the entrypoint's own key generation, so both have to be given;
without them the services sit in `Master key is missing` / `Cluster join: Join key is
missing`, port 8082 never starts listening, and the instance never becomes ready while
port 8081 answers `404` throughout.

Both keys and the database password are throwaways for a local sandbox — never reuse them
anywhere real.

Two symptoms worth recognising, because Artifactory reports neither on the port you are
watching:

| Symptom | Cause |
|---------|-------|
| 8082 never listens, 8081 answers `404` | a missing master or join key |
| `This REST API is available only in Artifactory Pro` | creating repositories, listing or granting permissions — all Pro-only |
| Router loops on `Cluster join: Retry N` | the Access service failed to start; check `docker exec … tail /opt/jfrog/artifactory/var/log/access-service.log` |

## Publish marketplace plugin zips

```bash
export ARTIFACTORY_URL='http://localhost:8082/artifactory/example-repo-local'
export ARTIFACTORY_USER=admin
export ARTIFACTORY_PASSWORD=password
export NEXUS_REPO_ID=artifactory

./docker/marketplace-nexus/publish-marketplace-plugins.sh --package
```

## Point Hop at it

To keep your real configuration untouched, point Hop at a throwaway one. It has to exist
first — Hop does not create the folder, and a missing one fails during startup with
`Parent directory ... does not exist!` before the command runs:

```bash
export HOP_CONFIG_FOLDER=/tmp/hopconfig
mkdir -p "${HOP_CONFIG_FOLDER}"
```

```bash
cd /path/to/hop          # the unzipped hop-client directory
./hop marketplace repo add --id artifactory \
  --url http://localhost:8082/artifactory/example-repo-local/ --primary --browse
./hop marketplace query
./hop marketplace install hop-tech-parquet
# restore production defaults later:
./hop marketplace repo reset-defaults
```

With credentials configured, Hop browses using **AQL** — one request for the whole
repository. Without them it browses by **walking `/api/storage`**, because Artifactory
requires an authenticated user for AQL.

`start.sh` turns anonymous access on, but that is only half of anonymous read: the
anonymous user also needs read permission on the repository, which is a permission target,
and those are **Pro-only**. On OSS the symptom changes from `401` to `403` and no further.
Grant it in the UI (Administration → User Management → Permissions) if you want to
exercise the walk without credentials — otherwise use credentials, as above.

An access token instead of a password, which is the usual arrangement for a real
Artifactory:

The platform endpoint (`/access/api/v1/tokens`) answers `Unsupported authentication method
Basic`, and scope `applied-permissions/admin` is rejected as unaccepted, so use the
Artifactory endpoint with `applied-permissions/user`:

```bash
TOKEN=$(curl -s -u admin:password -X POST \
  http://localhost:8082/artifactory/api/security/token \
  -d 'username=admin&scope=applied-permissions/user&expires_in=3600' \
  | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')

./hop marketplace repo add --id artifactory \
  --url http://localhost:8082/artifactory/example-repo-local/ --browse \
  --auth-type token --password "${TOKEN}"
```

Set `--group-id-filter` to the groupId your plugins use. On the storage walk it
becomes the starting folder rather than a filter applied afterwards, which is what
keeps browsing a large repository affordable.

## Automated test

`JfrogArtifactoryBrowseIT` starts the same image through Testcontainers and covers
both browse paths, the token authentication and a download of a browsed plugin. It
is opt-in, since pulling and booting Artifactory is too slow for every build:

```bash
./mvnw -pl plugins/misc/marketplace verify -Dmarketplace.it=true
```

## Stop

```bash
docker compose -f docker/marketplace-artifactory/docker-compose.yml down
# Wipe data:
docker compose -f docker/marketplace-artifactory/docker-compose.yml down -v
```

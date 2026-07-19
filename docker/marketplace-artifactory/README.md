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

## Start

```bash
docker compose -f docker/marketplace-artifactory/docker-compose.yml up -d
```

Open http://localhost:8082 and complete first-time Artifactory setup (admin password).

Create a **local** Maven repository, for example:

| Field | Value |
|-------|--------|
| Package type | Maven |
| Repository Key | `hop-plugins-local` |
| Repository Layout | maven-2-default |

Public URL base (typical OSS default):

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

The script deploys each Wave 1 `*.zip` with Maven layout under `org/apache/hop/…`.

## Point Hop marketplace at local Artifactory

In `config/hop-config.json` (or via GUI later):

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

Then from the Hop install directory:

```bash
export HOP_HOME=/path/to/hop
./hop marketplace install hop-tech-parquet
./hop marketplace install hop-engines-beam
./hop marketplace list
```

Notes:

- `hop-engines-beam` marketplace zip is the **plugin only**; `lib/beam` stays in the default client.
- Restart Hop after install/uninstall so the plugin registry reloads.

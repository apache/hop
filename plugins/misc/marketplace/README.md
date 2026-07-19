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

# Hop Marketplace plugin

Bundled core plugin that installs optional Hop plugins from a Maven repository
(Maven Central, ASF Nexus, or a local Artifactory).

## CLI

From the Hop install directory (or invoke the `hop` script by path — it already
`cd`s to the install root):

```bash
./hop marketplace install hop-tech-parquet
./hop marketplace install hop-engines-spark:2.19.0
./hop marketplace install hop-engines-beam
./hop marketplace list
./hop marketplace uninstall hop-tech-parquet

# Declarative environment (CI/CD / Docker)
./hop marketplace apply -f hop-env.yaml
./hop marketplace apply -f hop-env.yaml --prune
./hop marketplace validate -f hop-env.yaml
```

Coordinates:

- `artifactId` (uses config `defaultVersion` / Hop version)
- `artifactId:version`
- `groupId:artifactId:version`

After install or uninstall, **restart Hop** so the plugin registry reloads.

### hop-env.yaml

See `config/projects/samples/marketplace/hop-env.example.yaml` (after install) or
`plugins/misc/marketplace/src/main/samples/hop-env.example.yaml` in source.

```yaml
version: "1.0"
hopVersion: "2.19.0"
enforceOnRun: false   # set true to hard-fail hop-run on drift
repositories:
  - id: central
    url: "https://repo1.maven.org/maven2/"
plugins:
  - artifactId: hop-tech-parquet
    version: "2.19.0"
dependencies:
  - groupId: org.postgresql
    artifactId: postgresql
    version: "42.7.3"
```

`hop-run` checks the env file only when `enforceOnRun: true` or `-Dhop.env.enforce=true`.
Discovery order: `-f` / `HOP_ENV_FILE` / `PROJECT_HOME/hop-env.yaml` / install-root `hop-env.yaml`.

## Config (`hop-config.json`)

Marketplace settings live under the `marketplace` key in **`${HOP_CONFIG_FOLDER}/hop-config.json`**
(always that path — not a separate file under the client zip). If `HOP_CONFIG_FOLDER` is
unset, Hop defaults it to `<working-dir>/config` (with the hop launcher, that is
`<install>/config`).

Local Sonatype Nexus (recommended for marketplace development — see
`docker/marketplace-nexus/README.md`):

```json
{
  "marketplace": {
    "enabled": true,
    "groupId": "org.apache.hop",
    "defaultVersion": "2.19.0-SNAPSHOT",
    "repositories": [
      {
        "id": "local-nexus",
        "url": "http://localhost:8081/repository/hop-plugins/"
      }
    ]
  }
}
```

After `./docker/marketplace-nexus/start.sh`, installs are **anonymous** — no password
in that hop-config.

```bash
cd /path/to/hop
./hop marketplace install hop-tech-parquet
```

Local Nexus after `docker/marketplace-nexus/start.sh` is **anonymous read** — leave
`username` / `password` out of config and do **not** set `HOP_MARKETPLACE_*` (wrong
Basic auth causes HTTP 401 even though anonymous would succeed).

Corporate private repos can use Basic auth (prefer a read-only user):

```bash
export HOP_MARKETPLACE_USERNAME=reader
export HOP_MARKETPLACE_PASSWORD='…'
```

## GUI

Hop GUI → **Tools → Marketplace…** lists Wave 1 optional plugins, installs from the
configured Maven repository, and uninstalls marketplace-managed installs. Restart
Hop after install/uninstall.

## Beam note

`hop-engines-beam` marketplace package is the **plugin** (`plugins/engines/beam`) only.
`lib/beam` remains part of the default Hop distribution and is never removed by
marketplace uninstall.

## Integration tests / full local install

Optional plugins are **not** in `hop-client.zip`. For ITs (or a full local install):

```bash
unzip assemblies/client/target/hop-client-*.zip -d assemblies/client/target/
./tools/install-wave1-plugins.sh assemblies/client/target/hop
```

`integration-tests/scripts/run-tests-docker.sh` and Jenkins run this automatically.

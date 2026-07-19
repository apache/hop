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

Bundled core plugin that installs optional Hop plugins from Maven repositories.
**Default primary:** [Apache Repository](https://repository.apache.org/content/groups/public/)
(`repository.apache.org`), with **Maven Central** as fallback.

## CLI

From the Hop install directory (or invoke the `hop` script by path — it already
`cd`s to the install root):

```bash
./hop marketplace install hop-tech-parquet
./hop marketplace install hop-engines-spark:2.19.0 --repo asf
./hop marketplace install hop-engines-beam
./hop marketplace list
./hop marketplace uninstall hop-tech-parquet

# Repositories (saved to ${HOP_CONFIG_FOLDER}/hop-config.json)
./hop marketplace repo list
./hop marketplace repo add --id local-nexus --url http://127.0.0.1:8081/repository/hop-plugins/ --primary
./hop marketplace repo set-primary asf
./hop marketplace repo enable central
./hop marketplace repo disable local-nexus
./hop marketplace repo remove local-nexus
./hop marketplace repo reset-defaults   # ASF primary + Central

# Declarative environment (CI/CD / Docker)
./hop marketplace apply -f hop-env.yaml
./hop marketplace apply -f hop-env.yaml --prune
./hop marketplace validate -f hop-env.yaml

# Restore all optional plugins that used to ship in the fat client
./hop marketplace apply -f full-client-env.yaml
```

Install tries the **primary** repository first, then other **enabled** repositories
in list order, until the zip is found (or all fail). Use `--repo <id>` to force a
single repository.

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
  - id: asf
    url: "https://repository.apache.org/content/groups/public/"
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
(always that path). If `HOP_CONFIG_FOLDER` is unset, Hop defaults it to
`<working-dir>/config` (with the hop launcher: `<install>/config`).

When the `marketplace` key is absent, defaults are:

1. **asf** (primary) — `https://repository.apache.org/content/groups/public/`
2. **central** — `https://repo1.maven.org/maven2/`

```json
{
  "marketplace": {
    "enabled": true,
    "groupId": "org.apache.hop",
    "defaultVersion": "2.19.0-SNAPSHOT",
    "repositories": [
      {
        "id": "asf",
        "name": "Apache Repository",
        "url": "https://repository.apache.org/content/groups/public/",
        "primary": true,
        "enabled": true
      },
      {
        "id": "central",
        "name": "Maven Central",
        "url": "https://repo1.maven.org/maven2/",
        "primary": false,
        "enabled": true
      }
    ]
  }
}
```

Local Sonatype Nexus for development (see `docker/marketplace-nexus/README.md`):

```bash
./hop marketplace repo add --id local-nexus \
  --url http://127.0.0.1:8081/repository/hop-plugins/ --primary
./hop marketplace install hop-tech-parquet
```

Anonymous local Nexus needs **no** username/password and **no** `HOP_MARKETPLACE_*`
env (wrong Basic auth causes HTTP 401 even when anonymous would work).

Corporate private repos can use Basic auth (prefer a read-only user):

```bash
export HOP_MARKETPLACE_USERNAME=reader
export HOP_MARKETPLACE_PASSWORD='…'
```

## GUI

Hop GUI → **Tools → Marketplace…** lists Wave 1 optional plugins. Choose the
**primary repository**, or **Manage…** to add/edit/remove/reorder repositories
(same hop-config.json as the CLI). Install uses the primary first, then fallbacks.
Restart after install or uninstall.

## Optional plugin registry & full-client-env.yaml

Source of truth for marketplace-optional plugins:

`src/main/resources/org/apache/hop/marketplace/optional-plugins.yaml`

- GUI catalog loads this file at runtime.
- `tools/generate-full-client-env.sh` builds `full-client-env.yaml` (Maven `generate-resources`).
- The marketplace plugin zip places `full-client-env.yaml` at the **Hop install root** (and under samples).

When moving another plugin out of the fat client: exclude it from assemblies, **add one entry**
to `optional-plugins.yaml`, rebuild. Then:

```bash
./hop marketplace apply -f full-client-env.yaml
```

## Packaging & size

Optional plugin modules already produce installable zips via the reactor `assembly`
profile (`appendAssemblyId=false` → `artifactId-version.zip` next to the jar). A
normal Maven deploy publishes that zip so marketplace can download it from ASF /
Central. For local SNAPSHOTs use `docker/marketplace-nexus/publish-wave1-plugins.sh`.

Lean client size (after Wave 1 optional split) is checked with:

```bash
./tools/check-assembly-size.sh
# typically hop-client ~590 MB (limit 850 MB)
```

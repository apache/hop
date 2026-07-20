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
./hop marketplace query parquet
./hop marketplace query engines
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

### Projects lifecycle environments

When the **Projects** plugin enables a lifecycle environment, Marketplace can
check the same hop-env file using settings stored as **namespaced attributes**
on that environment (`IAttributes` group `marketplace`). No compile-time
dependency exists between the two plugins; they share core `AttributesContext`.

| Attribute key (`marketplace` group) | Values | Meaning |
|-------------------------------------|--------|---------|
| `envFile` | path (variables OK) | hop-env / full-client-env file |
| `onEnable` | `off` / `warn` / `enforce` | Action when the environment is enabled |
| `strict` | `true` / `false` | Extra marketplace plugins count as drift |
| `autoApply` | `true` / `false` | Apply missing plugins on enable (default off) |

If `onEnable` is unset, purpose defaults apply: **Production** → `enforce`,
**Testing/Acceptance** → `warn`, **Development/CI/CB** → `off`.

Configure these on the environment dialog **Marketplace plugins** tab (only
visible when this plugin is installed). At enable time extension point
`HopProjectEnvironmentAfterEnabled` runs the check.

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

Hop GUI → **Tools → Marketplace…**, or the main toolbar icon after **Save As…**
(separator before it), opens the marketplace. The startup **Welcome** dialog also has a
**Marketplace** topic (docs link, open dialog, install-all via `full-client-env.yaml`).
The dialog lists optional plugins from the registry. Choose the **primary repository**,
or **Manage…** to add/edit/remove/reorder repositories (same hop-config.json as the CLI).
Install uses the primary first, then fallbacks.

**Environment file** path + **Browse…** / **Edit…** / **Validate** / **Apply** manage
declarative `hop-env.yaml` files. **Edit…** opens a tabbed editor (General,
Repositories, Plugins, Dependencies) with New/Open/Save/Save As, catalog multi-add,
and optional import of repositories from hop-config.

Restart after install, apply, or uninstall.

## Optional plugin registry (source of truth)

All marketplace-optional plugins are declared in **one file only**:

```text
plugins/misc/marketplace/src/main/resources/org/apache/hop/marketplace/optional-plugins.yaml
```

Do not hardcode plugin lists elsewhere. Everything below reads this registry:

| Consumer | How it uses the registry |
|----------|--------------------------|
| GUI catalog / `hop marketplace query` | Loads YAML at runtime |
| `full-client-env.yaml` | Generated by `tools/generate-full-client-env.sh` (Maven `generate-resources`) |
| Local publish to Nexus/Artifactory | `docker/marketplace-nexus/publish-marketplace-plugins.sh` |
| Unpack zips into an install (IT images) | `tools/install-wave1-plugins.sh` via `tools/list-marketplace-plugins.sh` |

Each entry needs at least:

```yaml
  - artifactId: hop-tech-parquet
    name: Parquet
    category: Technology
    description: Parquet file format support.
    installPath: plugins/tech/parquet   # path under Hop home after install
    modulePath: plugins/tech/parquet    # Maven module in this repo (for package/publish)
```

Top-level `groupId` (default `org.apache.hop`) is used when publishing.

### Making a plugin optional (checklist)

1. Exclude it from the fat hop-client assembly.
2. **Add one entry** to `optional-plugins.yaml` (`artifactId`, `installPath`, `modulePath`, …).
3. Rebuild marketplace / client so `full-client-env.yaml` regenerates.
4. For local testing: package + publish the zip (see next section), point Hop at that repo,
   then `./hop marketplace apply -f full-client-env.yaml` (or install a single GAV).
5. Restart Hop.

## Publishing plugin zips (local Nexus / Artifactory)

Marketplace installs **Maven GAV zips**, not jars alone. Layout:

```text
{repoBase}/org/apache/hop/{artifactId}/{version}/{artifactId}-{version}.zip
```

Plugin modules already produce that zip via the reactor assembly
(`appendAssemblyId=false` → `modulePath/target/{artifactId}-{version}.zip`).

### Script: `publish-marketplace-plugins.sh`

From the **Hop repo root**:

```bash
# List what the registry will publish (no network)
./tools/list-marketplace-plugins.sh
./tools/list-marketplace-plugins.sh --zips
```

#### Local Sonatype Nexus (Docker)

```bash
./docker/marketplace-nexus/start.sh
# UI http://localhost:8081/  — admin / hop-nexus-dev
# Repo http://127.0.0.1:8081/repository/hop-plugins/

export NEXUS_PASSWORD=hop-nexus-dev

# Build every registry modulePath, then deploy all zips:
./docker/marketplace-nexus/publish-marketplace-plugins.sh --package

# Or deploy only zips that already exist under target/:
./docker/marketplace-nexus/publish-marketplace-plugins.sh

# Preview without uploading:
./docker/marketplace-nexus/publish-marketplace-plugins.sh --dry-run
```

Prefer **`127.0.0.1`** over `localhost` in Hop config (IPv6 vs Docker IPv4).

More detail: `docker/marketplace-nexus/README.md`.

#### Your Artifactory (or any Maven 2 repo)

```bash
export ARTIFACTORY_URL='https://artifactory.example.com/artifactory/hop-plugins-local'
export ARTIFACTORY_USER='deploy-user'
export ARTIFACTORY_PASSWORD='…'
export NEXUS_REPO_ID=artifactory   # must match <server><id> used for deploy auth
export HOP_VERSION=2.19.0-SNAPSHOT # optional; default 2.19.0-SNAPSHOT

./docker/marketplace-nexus/publish-marketplace-plugins.sh --package
```

Equivalent env names also accepted: `NEXUS_REPO_URL`, `NEXUS_USER`, `NEXUS_PASSWORD`,
`NEXUS_ADMIN_PASSWORD`.

| Flag / env | Meaning |
|------------|---------|
| `--package` | `mvn -pl <all modulePaths> -am package -DskipTests`, then deploy |
| `--dry-run` | Print deploy targets; no Maven deploy (and no password required) |
| `HOP_VERSION` | Version coordinate and zip filename (default `2.19.0-SNAPSHOT`) |
| `GROUP_ID` | Override group (default: `groupId` from the YAML, else `org.apache.hop`) |
| `NEXUS_REPO_ID` | Maven `settings.xml` server id for credentials (default `hop-plugins`) |

Missing zips are **skipped** with a message (`SKIP (not built)`), not a hard failure—so you
can publish a subset after a partial package.

`publish-wave1-plugins.sh` is a **deprecated alias** of `publish-marketplace-plugins.sh`.

### Point Hop at the local repo and install

```bash
cd /path/to/hop-client   # install root (hop launcher cd's here)

./hop marketplace repo add --id local-nexus \
  --url http://127.0.0.1:8081/repository/hop-plugins/ --primary

./hop marketplace install hop-tech-parquet
# or restore everything listed in the registry / full-client env:
./hop marketplace apply -f full-client-env.yaml
```

Anonymous **read** on the repo is ideal for installs. Deploy credentials stay only on the
publish side (admin / CI user).

### Related helper scripts

```bash
# artifactId|modulePath  (or --zips / --modules)
./tools/list-marketplace-plugins.sh

# Unzip all built registry zips into a Hop install (no Maven repo)
./tools/install-wave1-plugins.sh [HOP_INSTALL_DIR]

# Regenerate full-client-env.yaml (also run by marketplace module generate-resources)
./tools/generate-full-client-env.sh [HOP_VERSION] [OUTPUT_PATH]
```

## Packaging & size

Optional plugins are **not** bundled in the default hop-client zip so the download stays
under the ASF packaging limit. Production releases publish the same GAV zips to
Apache Repository / Maven Central; day-to-day SNAPSHOT testing uses local Nexus or
Artifactory as above.

Lean client size check:

```bash
./tools/check-assembly-size.sh
# typically hop-client well under the 850 MB limit
```

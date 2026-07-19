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

From the Hop install directory (`HOP_HOME`):

```bash
./hop marketplace install hop-tech-parquet
./hop marketplace install hop-engines-spark:2.19.0
./hop marketplace install hop-engines-beam
./hop marketplace list
./hop marketplace uninstall hop-tech-parquet
```

Coordinates:

- `artifactId` (uses config `defaultVersion` / Hop version)
- `artifactId:version`
- `groupId:artifactId:version`

After install or uninstall, **restart Hop** so the plugin registry reloads.

## Config (`hop-config.json`)

```json
{
  "marketplace": {
    "enabled": true,
    "groupId": "org.apache.hop",
    "defaultVersion": "2.19.0-SNAPSHOT",
    "repositories": [
      { "id": "central", "url": "https://repo1.maven.org/maven2/" }
    ]
  }
}
```

For local testing see `docker/marketplace-artifactory/README.md`.

## Beam note

`hop-engines-beam` marketplace package is the **plugin** (`plugins/engines/beam`) only.
`lib/beam` remains part of the default Hop distribution and is never removed by
marketplace uninstall.

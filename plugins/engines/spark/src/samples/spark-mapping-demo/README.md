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

# spark-mapping-demo

Sample Hop project for the **Native Spark** cluster walk-through: prove that a
**Simple Mapping** child pipeline under `${PROJECT_HOME}` runs on Spark workers when
you submit a **project package** with `MainSpark`.

## What is in the box

| Path | Role |
|------|------|
| `pipelines/01-enrich-with-mapping.hpl` | Parent: Spark File Input → Simple Mapping → Spark File Output |
| `pipelines/mappings/upper-name.hpl` | Child mapping (must load via package `PROJECT_HOME`) |
| `data/customers-sample.csv` | Tiny input (copied to shared `/data/hop-data` on the cluster) |
| `metadata/.../spark-cluster.json` | Native Spark run configuration for `spark-submit` |
| `cluster-env.json` | Sets `HOP_DATA=file:///data/hop-data` for the cluster |
| `scripts/prepare-dist.sh` | Host: fat jar + package zip into `dist/spark-native-demo` |
| `scripts/spark-submit-demo.sh` | Inside master: `spark-submit` + MainSpark package args |

**Data vs definitions:** Dataset paths use **`HOP_DATA`**. The mapping path uses
**`${PROJECT_HOME}/pipelines/mappings/upper-name.hpl`**. Do not put bulk data under
package `PROJECT_HOME`.

## Five-command happy path

From the **repository root**, with a Hop install that includes the native Spark plugin
(`HOP_HOME` or `./hop-conf.sh`):

```bash
# 1) Fat jar + project package
./plugins/engines/spark/src/samples/spark-mapping-demo/scripts/prepare-dist.sh

# 2) Start Spark 4.1 master + workers (shared data volume)
HOP_DIST_DIR="$PWD/dist/spark-native-demo" \
  docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  up -d --build --scale spark-worker=2

# 3) Submit
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  exec spark /opt/hop-samples/spark-mapping-demo/scripts/spark-submit-demo.sh

# 4) Inspect output (on master; same volume as workers)
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  exec spark ls -la /data/hop-data/out/enriched

# 5) Tear down
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml down -v
```

Master UI: http://localhost:8080

Full narrative, troubleshooting, and design notes:
[Cluster mapping walk-through](../../../../../../docs/hop-user-manual/modules/ROOT/pages/pipeline/spark/cluster-mapping-walkthrough.adoc)
(or the published user manual page *Cluster mapping walk-through* under Native Spark).

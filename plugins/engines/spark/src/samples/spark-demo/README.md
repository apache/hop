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

# spark-demo

Sample Hop project for the **Native Spark** cluster walk-through: prove that nested
artifacts under `${PROJECT_HOME}` (mappings, workflows) run on a multi-node Spark
cluster when you submit a **project package** with `MainSpark`.

## What is in the box

| Path | Role |
|------|------|
| `pipelines/01-enrich-with-mapping.hpl` | Parent: Spark File Input → Simple Mapping → Spark File Output |
| `pipelines/mappings/upper-name.hpl` | Child mapping (must load via package `PROJECT_HOME`) |
| `pipelines/02-run-workflows.hpl` | Spark File Input → Workflow Executor (one workflow per country) |
| `workflows/create-country-folder.hwf` | Child workflow: create `${HOP_DATA}/out/countries/${COUNTRY_NAME}` + marker file |
| `pipelines/03-run-pipelines.hpl` | Orchestrator: Pipeline Executor (Force Driver Only) → nested **Native Spark** child per country |
| `pipelines/nested/enrich-by-country.hpl` | Child Spark Dataset job: customers → `out/by-country/${COUNTRY_NAME}/` |
| `data/customers-sample.csv` | Mapping / nested-pipeline demo input |
| `data/countries.csv` | Workflow + nested-pipeline control keys (Genovia, Wakanda, Latveria) |
| `scripts/spark-submit-pipelines-demo.sh` | Submit `03-run-pipelines.hpl` |
| `metadata/.../spark-cluster.json` | Native Spark run configuration for `spark-submit` |
| `cluster-env.json` | Sets `HOP_DATA=file:///data/hop-data` for the cluster |
| `scripts/prepare-dist.sh` | Host: fat jar + package zip into `/tmp/spark-demo-dist` (override with `DIST_DIR`) |
| `scripts/spark-submit-demo.sh` | Inside master: `spark-submit` + MainSpark package args |
| `scripts/spark-submit-workflows-demo.sh` | Same, for `pipelines/02-run-workflows.hpl` |

**Data vs definitions:** Dataset and side-effect paths use **`HOP_DATA`**. Mapping and
workflow definition paths use **`${PROJECT_HOME}/…`**. Do not put bulk data under
package `PROJECT_HOME`.

## Five-command happy path (mapping)

From the **repository root**, with a Hop install that includes the native Spark plugin
(`HOP_HOME` or `./hop-conf.sh`):

```bash
# 1) Fat jar + project package → /tmp/spark-demo-dist (no hop-config registration needed)
./plugins/engines/spark/src/samples/spark-demo/scripts/prepare-dist.sh

# 2) Start Spark 4.1 master + workers
#    /tmp/spark-demo-dist → /opt/hop-dist (artifacts)
#    /tmp/spark-demo-dist/hop-data → /data/hop-data (shared data plane, host-visible)
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  up -d --build --scale spark-worker=2

# 3) Submit mapping demo
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  exec spark /opt/hop-samples/spark-demo/scripts/spark-submit-demo.sh

# 4) Inspect on the host (same files as /data/hop-data in the containers)
ls -la /tmp/spark-demo-dist/hop-data/out/enriched
# Optional: execution information location JSON
ls -la /tmp/spark-demo-dist/hop-data/executions 2>/dev/null || true

# 5) Tear down
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml down
```

## Workflow Executor demo (countries)

Same cluster; submit the second pipeline:

```bash
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  exec spark /opt/hop-samples/spark-demo/scripts/spark-submit-workflows-demo.sh
```

Or:

```bash
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  exec spark env PIPELINE_PATH=pipelines/02-run-workflows.hpl \
  /opt/hop-samples/spark-demo/scripts/spark-submit-demo.sh
```

### What to inspect

On the **host** (bind mount):

```bash
find /tmp/spark-demo-dist/hop-data/out/countries -type f | sort
ls -la /tmp/spark-demo-dist/hop-data/executions 2>/dev/null || true
```

Or inside the container (`/data/hop-data` is the same directory):

```bash
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  exec spark find /data/hop-data/out/countries -type f | sort
```

Expected layout:

```text
/tmp/spark-demo-dist/hop-data/out/countries/   # host
  Genovia/<Internal.Pipeline.ID>.txt
  Wakanda/<Internal.Pipeline.ID>.txt
  Latveria/<Internal.Pipeline.ID>.txt
```

- **Three country folders** → Workflow Executor ran the child workflow once per input row.
- **Same marker basename** in all three folders → one parent mini-pipeline/transform instance
  processed all rows (common for a tiny CSV on one partition, or **DRIVER_ONLY**).
- **Different marker basenames** → multiple Spark partition instances of Workflow Executor
  (**DISTRIBUTED** fan-out).

### Nested Native Spark pipelines (heavy work per key)

Same cluster; one **full Native Spark child pipeline** per country (shared parent
`SparkSession`, not a new `spark-submit` app per key):

```bash
docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  exec spark /opt/hop-samples/spark-demo/scripts/spark-submit-pipelines-demo.sh
```

Inspect on the host:

```bash
find /tmp/spark-demo-dist/hop-data/out/by-country -type f | sort
```

Expect directories `Genovia/`, `Wakanda/`, `Latveria/` under `out/by-country/`. Logs should
mention **Nested Native Spark pipeline reusing parent SparkSession**.

**Force Driver Only** is required on the Pipeline Executor: nested
`SparkPipelineEngine` must start on the driver. The sample HPL sets attribute
`spark` / `run_mode` = `FORCE_DRIVER_ONLY`. Child run configuration is `spark-cluster`
(Native Spark).

### Generic transform run mode

On the Native Spark run configuration, **Generic transform run mode** defaults to
`DISTRIBUTED`. To force nested workflows / pipeline executors onto the Spark driver,
set the run config to `DRIVER_ONLY`, or right-click the transform → **Spark Run Mode** →
**Force Driver Only**.

A 3-row CSV may still use a single partition under `DISTRIBUTED`; folders still prove
package `PROJECT_HOME` resolution and shared-volume side effects.

Override the dist folder with `DIST_DIR=/somewhere/else` for prepare and the same path as
`HOP_DIST_DIR=…` for compose if needed. The shared data plane defaults to
`${HOP_DIST_DIR}/hop-data` (override with `HOP_DATA_HOST_DIR`).

Master UI: http://localhost:8080

Full narrative, troubleshooting, and design notes:
[Cluster mapping walk-through](../../../../../../docs/hop-user-manual/modules/ROOT/pages/pipeline/spark/cluster-mapping-walkthrough.adoc)
(or the published user manual page *Cluster mapping walk-through* under Native Spark).

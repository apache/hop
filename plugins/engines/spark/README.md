<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Native Apache Spark pipeline engine

Implements [issue #7486](https://github.com/apache/hop/issues/7486): a Hop
`IPipelineEngine` that runs batch pipelines on **Apache Spark 4.1.x** without
Apache Beam.

## Design notes (Phase 0 decisions)

| Topic | Choice |
|-------|--------|
| Spark version | `4.1.2` (`spark-sql_2.13`), independent of Beam’s Spark 3.x |
| Row model | Spark `Row` + `StructType` (`HopSparkRowConverter`) |
| Hop transforms | `mapPartitions` via `HopMapPartitionsFn` (init once per partition) |
| Metadata on the wire | Transform XML + `JsonRowMeta` + `SerializableMetadataProvider` JSON |
| Deployment v1 | `local[*]` and standalone `spark://`; fat-jar field for cluster |
| Streaming / windows | Out of scope — use Beam Spark |
| Stateful transforms | Native handlers: Memory Group By, Merge Join, Unique Rows, Sort Rows; sorted Group By remains banned |

## Classpath

- **Compile / local run:** Spark 4.1 is packaged under `plugins/engines/spark/lib`
  so Hop can start `local[*]` without a separate Spark install. The plugin must
  also ship **hadoop-client-api/runtime** and **log4j**: Spark 4 loads
  `org.apache.spark.sql.classic.SparkSession` reflectively and fails with
  *“Cannot find a SparkSession implementation on the Classpath”* when those
  classes are missing (the real `ClassNotFoundException` is swallowed).
- **Fat jar tokens (not real pack directories under `lib/spark-clients/`):**

  | Token | Use for | Spark runtime |
  |-------|---------|----------------|
  | `native` | hop-run client / embed Spark | Ships Spark 4 from `plugins/engines/spark/lib` |
  | **`native-provided`** | **`spark-submit` on a cluster** | **Cluster provides Spark** (do not embed) |

  **Do not** embed Spark inside the app jar for `spark-submit`. That mixes two
  Spark copies on the driver classpath and fails on Java 21 (e.g.
  `DirectByteBuffer` / wrong `Running Spark version`).

  ```bash
  # From a Hop install — fat jar for spark-submit:
  ./hop-conf.sh --generate-fat-jar=/tmp/hop-native-spark4-submit.jar \
    --spark-client-version=native-provided
  ```

- **spark-submit (cluster):** Use driver main class
  `org.apache.hop.spark.run.MainSpark` with a **native-provided** fat jar:

  ```bash
  # Critical: pin SPARK_HOME to this distro. If SPARK_HOME points at an older
  # install (e.g. /opt/spark -> 3.3.0), even "./bin/spark-submit" from a 4.1.x
  # tree re-executes ${SPARK_HOME}/bin/spark-class and loads the wrong Spark.
  export SPARK_HOME=~/Downloads/spark-4.1.3-bin-hadoop3

  # Export project metadata to JSON, then:
  "${SPARK_HOME}/bin/spark-submit" \
    --master spark://spark:7077 \
    --class org.apache.hop.spark.run.MainSpark \
    /tmp/hop-native-spark4-submit.jar \
    /path/to/pipeline.hpl \
    /path/to/metadata.json \
    YourNativeSparkRunConfig
  ```

  Positional args: `pipeline.hpl`, `metadata.json`, `runConfigName`, optional
  env config file. Named form also works: `--HopPipelinePath=...`,
  `--HopMetadataPath=...`, `--HopRunConfigurationName=...`,
  `--HopConfigFile=...`.

  The run configuration must use the **native** Spark pipeline engine (not
  Beam). Prefer leaving **master blank** so `spark-submit --master` wins, or set
  master to `spark://spark:7077`. Leave **fatJar empty** under submit.
  Paths must be readable on the driver; data paths must be reachable from workers.
- After rebuilding the plugin, reinstall into your Hop distro (or rebuild
  `assemblies/client`) so `plugins/engines/spark/lib` is updated.

## Execution information location

Attach an **execution information location** (and optionally an execution data
profile) on the native Spark **pipeline run configuration**, same as Local/Beam.

The driver will:

1. Load and initialize the location at prepare time  
2. `registerExecution` when the pipeline starts  
3. Periodically `updateExecutionState` for the pipeline and each transform
   component (metrics come from Spark accumulators / `EngineMetrics`)  
4. Write a final state and `close` the location when the pipeline finishes,
   fails, or is stopped  

Notes:

- **State updates** (`updateExecutionState`) run on the **driver** from
  `EngineMetrics` (all transform types).  
- **Row sampling** (`registerData`) runs on **executors** for generic
  mapPartitions transforms when the run configuration also names an
  **execution data profile** (First/Last/Random rows, etc.). Extra GUI
  samplers on the engine are serialized into the closure like Beam.  
- File locations used for sampling must be **writable from every executor**
  (shared FS / object store). Local-only paths only receive driver state.  
- For `spark-submit`, export location + data profile into the metadata JSON.

## Module layout

- `engines/` — `SparkPipelineEngine`, run configuration, capabilities
- `pipeline/` — Hop → Spark converter and transform handlers
- `pipeline/handler/` — native shuffle handlers + generic mapPartitions
- `core/` — `HopMapPartitionsFn`, row conversion, executor-side Hop init
- `run/` — `MainSpark` for spark-submit

## Native handlers (Phase 3)

| Plugin id | Spark API | Notes |
|-----------|-----------|--------|
| `MemoryGroupBy` | `groupBy` + agg | SUM, AVG, MIN, MAX, COUNTs, FIRST/LAST, STDDEV, percentile, concat variants |
| `MergeJoin` | `Dataset.join` | INNER / LEFT / RIGHT / FULL OUTER |
| `Unique` | `dropDuplicates` (+ count via groupBy) | Reject-to-error not supported |
| `SortRows` | `orderBy` | Optional unique-on-keys |
| `GroupBy` (sorted) | — | **Banned** — use Memory Group By |
| `SparkFileInput` | `spark.read.format(...).load` | CSV/Parquet/JSON/ORC/text |
| `SparkFileOutput` | `df.write.format(...).save` | Save modes, optional coalesce/partitionBy |

## Classic Hop I/O (POC)

`HopMapPartitionsFn` sets partition-scoped variables for classic writers:

| Variable | Value |
|----------|--------|
| `${Internal.Transform.ID}` | `{transformName}-{partitionId}` |
| `${Internal.Transform.CopyNr}` / `BundleNr` | partition id |

It also enables `beamContext` so Text File Output auto-appends `_id_bundle` like Beam.
`dependencies.xml` stages `textfile`, `getfilenames`, and `excel` for mapPartitions POCs
(Get File Names → Text File Input, small Excel lookups, etc.). Pure sources still run on
one partition so files are not listed/read N times.

## File I/O transforms

Dedicated **Spark File Input** / **Spark File Output** transforms (not Hop Text File Input)
provide a clean Dataset API mapping:

- **Input:** path, format, header/separator/quote (CSV), optional field schema or `inferSchema`
- **Output:** path, format, save mode (Overwrite/Append/Ignore/ErrorIfExists), header, coalesce,
  partition-by columns

**Paths are Spark/Hadoop URIs** (e.g. local path, `hdfs://…`, `s3a://…`), not Hop VFS schemes
(`s3://`, named MinIO `minio://`, …). Dataset `load`/`save` never call `HopVfs`. Classic
mapPartitions transforms still use Hop VFS. See user manual: *Paths and file systems on native
Spark*.

Optional **path scheme map** on the run configuration (`from=to` lines, e.g. `s3=s3a`) rewrites
URI schemes for Spark File / Lake PATH only via `SparkPathDialect.toSparkUri`. `SparkPathDialect`
also appends a hint when a known Hop-only scheme still appears in I/O errors after mapping.

## Project package (nested Simple Mapping / Pipeline Executor)

**Specifically for Native Spark execution** (not File → Export current project to zip).

```bash
# GUI: Tools → Export project package for Native Spark…
# CLI:
./hop-conf.sh -j my-project --export-spark-project=/tmp/my-project-spark.zip

# spark-submit
--class org.apache.hop.spark.run.MainSpark ... \
  --HopProjectPackage=/tmp/my-project-spark.zip \
  --HopPipelinePath=pipelines/run.hpl \
  --HopRunConfigurationName=YourNativeSparkRunConfig
```

`SparkProjectPackage` + `SparkPipelineEngine`:

1. Driver stages the zip and sets `HOP_SPARK_PROJECT_PACKAGE`
2. Session start: `SparkContext.addFile` ships the zip to **all executors** (`SparkFiles`)
3. Mini-pipeline init: `SparkFiles.get` → extract under `java.io.tmpdir` → `PROJECT_HOME`

Do **not** use package `PROJECT_HOME` as the root for Spark Dataset data paths.

Writes run as Spark actions during graph build; the output handler registers an empty leaf
Dataset so a later engine `count()` does not re-write files.

## Lakehouse connectors (Delta Lake / Apache Iceberg)

**Open table format** support on the native Spark engine (ACID tables, catalogs, time travel,
MERGE, maintenance). Connectors **ship in the default** `hop-engines-spark` plugin assembly:

| Path | Contents |
|------|----------|
| `plugins/engines/spark/lib/delta/` | Delta Lake 4.3.1 tree |
| `plugins/engines/spark/lib/iceberg/` | Iceberg Spark runtime 1.11.0 |

**User documentation (Antora):**
`docs/hop-user-manual/.../pipeline/spark/lakehouse.adoc` plus transform pages
`spark-lake-table-*.adoc` and `metadata-types/spark-catalog.adoc`.
**Integration tests:** `integration-tests/lakehouse` (Delta) and
`integration-tests/iceberg` (Iceberg) — PATH Input/Output, overwrite/time travel, MERGE.

| Pin | Coordinate |
|-----|------------|
| Spark | `spark-sql_2.13:4.1.2` (existing) |
| Delta Lake | `io.delta:delta-spark_4.1_2.13:4.3.1` |
| Apache Iceberg | `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0` |

The `_4.1_2.13` suffix means **Spark 4.1 / Scala 2.13**, not the product major version.

### Classloader (why not a sibling plugin)

`SparkPipelineEngine` sets TCCL to the **Spark engine plugin classloader**, which loads:

1. JARs under `plugins/engines/spark/lib/` (recursive — includes `lib/delta`, `lib/iceberg`)
2. Folders from `plugins/engines/spark/dependencies.xml`
3. The engine plugin jar

A sibling plugin under `plugins/engines/spark-delta` is a **separate** classloader and is
**not** visible to `SparkSession` / DataSource resolution. Connectors must live **inside**
the engine lib tree (as shipped) or on the spark-submit driver/executor classpath.

### hop-run / GUI `local[*]`

No manual overlay is required on a normal Hop install. After rebuilding the plugin:

```bash
./mvnw -pl plugins/engines/spark -am package
# zip / client assembly unpacks lib/delta and lib/iceberg under plugins/engines/spark/
```

`target/lakehouse-connectors/` is a frozen jar list for docs verification (generated every build).

### spark-submit

Use a **native-provided** fat jar so Spark is not embedded twice. Lakehouse jars under
`plugins/engines/spark/lib/{delta,iceberg}` are **included** in that fat jar (they are not
filtered out like Spark/Scala/Hadoop client). Optional `--packages` only if you strip them:

```bash
export SPARK_HOME=/path/to/spark-4.1.x-bin-hadoop3

"${SPARK_HOME}/bin/spark-submit" \
  --master spark://spark:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --class org.apache.hop.spark.run.MainSpark \
  /tmp/hop-native-provided.jar \
  /path/to/pipeline.hpl \
  /path/to/metadata.json \
  YourNativeSparkRunConfig
```

When both Delta and Iceberg are used, keep **Delta** on `spark_catalog` and register Iceberg
catalogs under **other** names (e.g. `lake`).

### Session conf matrix (spike / future transforms)

| When | Conf |
|------|------|
| Any Delta use | `spark.sql.extensions` includes `io.delta.sql.DeltaSparkSessionExtension` |
| Any Delta use | `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog` |
| Any Iceberg use | `spark.sql.extensions` includes `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` |
| Iceberg TABLE | `spark.sql.catalog.<name>=…` (+ type/warehouse/uri) |

### Spike findings (PR 1 — 2026-07, Spark 4.1.2 + Delta 4.3.1 + Iceberg 1.11.0)

Recorded by `SparkLakehouseConnectorTest` with `-Plakehouse`:

| Check | Result |
|-------|--------|
| `Class.forName(DeltaSparkSessionExtension)` with connector on CL | OK |
| PATH write/read with extension **and** DeltaCatalog | OK (required gate) |
| PATH write/read with extension **only** (no DeltaCatalog) | **Fails** with `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG` |
| Iceberg extension on CL | OK |
| Iceberg bare `format("iceberg").save/load(path)` (no catalog conf) | **Fails** — defaults toward `HiveCatalog` / needs HMS jars |
| Iceberg `writeTo(hop_iceberg.\`file:///path\`)` + SQL read (PR 3) | **OK** |
| Iceberg Hadoop warehouse + `writeTo("local.db.t")` + `spark.table` | OK (named-table style) |
| Iceberg TABLE `writeTo` vs `saveAsTable` | **writeTo for PATH**; formal TABLE lock still PR 5 / KD-24 |

**Tiered probe default (updated by spike + PR 3):**

- Always require connector classes on the classpath.
- **Any Delta use (including PATH-only)** requires session-registered
  `DeltaSparkSessionExtension` **and** `spark.sql.catalog.spark_catalog=…DeltaCatalog`.
- **Any Iceberg PATH use** requires Iceberg extensions + Hadoop catalog `hop_iceberg`
  (auto-registered on hop-run; can be set at runtime on active sessions).
- hop-run always applies full Delta/Iceberg conf when those formats are present.

### Frozen jar list (shipped under `lib/delta` + `lib/iceberg`)

Also staged into `target/lakehouse-connectors/` on every build for docs verification.

| Jar | Role |
|-----|------|
| `delta-spark_4.1_2.13-4.3.1.jar` | Delta Spark SQL / DataSource |
| `delta-storage-4.3.1.jar` | Delta storage layer |
| `delta-kernel-api-4.3.1.jar` | Delta Kernel API |
| `delta-kernel-defaults-4.3.1.jar` | Delta Kernel defaults |
| `delta-kernel-unitycatalog-4.3.1.jar` | Transitive from delta-spark 4.3.1 |
| `unitycatalog-client-0.5.0.jar` | Transitive (UC-related) |
| `unitycatalog-hadoop-0.5.0.jar` | Transitive (UC-related) |
| `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` | Shaded Iceberg Spark runtime |

### Probe API

`org.apache.hop.spark.table.SparkLakeConnectorProbe` verifies connector classes are loadable
and throws `HopException` with reinstall / `--packages` guidance when missing.
`LakeSessionPlan` scans active lake transforms before `SparkSession` build: probes the
classpath and applies Delta extension + `DeltaCatalog` on new hop-run sessions.

### Lake Table transforms (PR 2–3 — Delta + Iceberg PATH)

| Plugin id | Role |
|-----------|------|
| `SparkLakeTableInput` | Read Delta or Iceberg PATH tables |
| `SparkLakeTableOutput` | Write Delta or Iceberg PATH; **default save mode ErrorIfExists** |

| Format | PATH read | PATH write |
|--------|-----------|------------|
| **Delta** | `spark.read.format("delta").load(path)` | `format("delta").mode(…).save(path)` |
| **Iceberg** | SQL `SELECT * FROM hop_iceberg.\`file:///…\`` | `writeTo(hop_iceberg.\`uri\`).using("iceberg")` |

Iceberg PATH uses a built-in Hadoop catalog named **`hop_iceberg`** (not `spark_catalog`, so it
can co-exist with Delta’s `DeltaCatalog`). Bare `format("iceberg").load(path)` is **not** used —
on Spark 4.1 + Iceberg 1.11 it defaults toward HiveCatalog and fails without HMS jars.

hop-run auto-registers:

| When | Session conf |
|------|----------------|
| Any Delta | extensions + `spark.sql.catalog.spark_catalog=…DeltaCatalog` |
| Any Iceberg | extensions + `spark.sql.catalog.hop_iceberg=…SparkCatalog` (type=hadoop, warehouse under tmp) |

Classic **Spark File Input/Output** are unchanged (csv/parquet/json/orc/text only).

### Time travel (PR 4)

Lake Table Input supports:

| UI type | Delta option | Iceberg SQL |
|---------|--------------|-------------|
| `NONE` | (current) | (current) |
| `VERSION` | `versionAsOf` | `VERSION AS OF <snapshot_id>` |
| `TIMESTAMP` | `timestampAsOf` | `TIMESTAMP AS OF TIMESTAMP '…'` |

Example: write twice with Overwrite, then set Time travel = VERSION and Version = `0` (Delta)
or the first snapshot id from `{table}.snapshots` (Iceberg).

### Spark Catalog metadata + TABLE mode (PR 5)

**Metadata type:** `SparkCatalog` (category Connections) — Spark SQL catalog conf for Iceberg
Hadoop/REST (or custom). Fields: catalog name, type, warehouse/uri, optional credential, extra
`key=value` lines → `spark.sql.catalog.<name>.*`.

Lake Table transforms in **TABLE** mode:

| Field | Purpose |
|-------|---------|
| Identifier mode | `TABLE` |
| Table identifier | e.g. `lake.db.orders` |
| Spark Catalog metadata name | Hop metadata entry to register (optional if catalog already on session) |

| Format | TABLE read | TABLE write |
|--------|------------|-------------|
| **Iceberg** (primary) | `SELECT * FROM catalog.ns.table` (+ time travel clauses) | `writeTo(id).using("iceberg")` |
| **Delta** (advanced) | `format("delta").table(id)` | `format("delta").saveAsTable(id)` |

`LakeSessionPlan` loads referenced `SparkCatalog` entries and applies them when building the
session (hop-run) or registers them on an active session when possible (submit).

For spark-submit, export project metadata JSON including `SparkCatalog` definitions, or pass
equivalent `--conf spark.sql.catalog.<name>=…`.

### MERGE (PR 6)

**Spark Lake Table Merge** — single upstream hop supplies source rows; target is PATH or TABLE.

| Field | Notes |
|-------|--------|
| Merge condition | ON clause, aliases `t` (target) and `s` (source) |
| When matched | `UPDATE_ALL` / `DELETE` / `NONE` |
| When not matched | `INSERT_ALL` / `NONE` |
| Not matched by source | optional `DELETE` (Delta; Iceberg depends on version) |
| Raw MERGE SQL | advanced override (empty default) |

Target SQL ids: Delta PATH → `delta.\`path\``; Iceberg PATH → `hop_iceberg.\`uri\``; TABLE → multi-part id.

Metrics are log-only (merge completion), not Dataset row counts.

### Maintenance (PR 7)

**Spark Lake Table Maintenance** — zero-input action (no hop required). Destructive ops require
`acknowledgeDestructive`.

| Operation | Delta | Iceberg |
|-----------|-------|---------|
| OPTIMIZE | `OPTIMIZE delta.\`path\` [ZORDER BY …]` | `CALL cat.system.rewrite_data_files` |
| VACUUM | `VACUUM … RETAIN n HOURS` (**n required**) | n/a → use EXPIRE_SNAPSHOTS |
| EXPIRE_SNAPSHOTS | n/a | `CALL … expire_snapshots(retain_last => N)` |
| REWRITE_MANIFESTS | n/a | `CALL … rewrite_manifests` |
| DELETE_WHERE | `DELETE FROM … WHERE …` (**WHERE required**) | same |

Metrics are log-only (expect 0 in/out in the GUI).


### RAT / license

Delta Lake and Apache Iceberg are Apache-2.0 licensed and redistributed under
`plugins/engines/spark/lib/{delta,iceberg}/`. Re-check transitive licenses when pins change.
Unity Catalog client jars ship as Delta transitives (Apache-2.0).

## Build

```bash
# Unit + lakehouse ITs (connectors are default runtime deps)
./mvnw -pl plugins/engines/spark -am test

# Package plugin zip (includes lib/delta + lib/iceberg)
./mvnw -pl plugins/engines/spark -am package

# -Plakehouse is a no-op alias retained for older scripts
./mvnw -pl plugins/engines/spark -am test -Plakehouse
```

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

# Native Spark lakehouse samples

Walkthroughs for **Delta Lake** and **Apache Iceberg** on Hop’s native Spark
pipeline engine. Automated coverage lives in the engine module unit/IT suite
(`./mvnw -pl plugins/engines/spark -am test -Plakehouse`). This folder documents
**operator-oriented** pipelines you can rebuild in Hop Gui under the parent
`integration-tests/spark-native` project.

User manual: [Lakehouse tables on the native Spark engine](../../../docs/hop-user-manual/modules/ROOT/pages/pipeline/spark/lakehouse.adoc)
(rendered in the Hop docs site as *Pipeline → Lakehouse tables…*).

## Prerequisites

1. Hop with `plugins/engines/spark` (Spark 4.1.x). Default assemblies already include:
   ```text
   $HOP_HOME/plugins/engines/spark/lib/delta/      # Delta 4.3.1 tree
   $HOP_HOME/plugins/engines/spark/lib/iceberg/   # iceberg-spark-runtime-4.1_2.13:1.11.0
   ```
2. A **Native Spark** pipeline run configuration (e.g. `spark-local` with master
   `local[*]`), same as the other spark-native samples.

Pins: Delta `io.delta:delta-spark_4.1_2.13:4.3.1`, Iceberg
`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`.

## Sample A — Delta PATH round-trip

**Goal:** write a small Delta table, read it back.

| Step | Transform | Settings |
|------|-----------|----------|
| 1 | Row Generator (or Data Grid) | A few columns, e.g. `id` (Integer), `name` (String); 10 rows |
| 2 | Spark Lake Table Output | Format `delta`, mode `PATH`, path `${PROJECT_HOME}/output/lakehouse/delta-demo`, save mode **Overwrite** |
| 3 | (second pipeline or hop) Spark Lake Table Input | Same format/path; optional field list |
| 4 | Spark File Output or Dummy | Inspect results |

Run with the native Spark run configuration. First write should use **Overwrite**
because the default lake save mode is **ErrorIfExists**.

## Sample B — Iceberg PATH round-trip

Same as Sample A with format **`iceberg`**. PATH mode uses the built-in
`hop_iceberg` Hadoop catalog (registered by hop-run). Paths should be URIs Spark
can open (e.g. `file://${PROJECT_HOME}/output/lakehouse/iceberg-demo`).

## Sample C — Time travel (Delta)

1. Run Sample A twice with Overwrite (or write, change data, overwrite again).
2. Input: Time travel type **VERSION**, version **`0`** (first Delta commit).
3. Compare row counts / content to the current table (no time travel).

Iceberg: use snapshot ids from the table’s snapshot metadata instead of `0`.

## Sample D — MERGE upsert (Delta PATH)

1. Create a target table (Sample A).
2. Pipeline: source rows (new + changed keys) → **Spark Lake Table Merge**
   - Target: same PATH
   - Merge condition: `t.id = s.id`
   - When matched: `UPDATE_ALL`
   - When not matched: `INSERT_ALL`
3. Re-read with Lake Table Input; assert upsert semantics (idempotent re-run).

## Sample E — Maintenance OPTIMIZE (Delta)

Standalone pipeline with only **Spark Lake Table Maintenance**:

- Format `delta`, PATH to Sample A table
- Operation `OPTIMIZE`
- Optional Z-Order columns for demo

For `VACUUM`, set **Retention hours** and tick **Acknowledge destructive**.

## Sample F — Iceberg TABLE + Spark Catalog

1. Metadata → Connections → **Spark Catalog**
   - Catalog name: e.g. `lake`
   - Type: `hadoop`
   - Warehouse: e.g. `file://${PROJECT_HOME}/output/lakehouse/warehouse`
2. Lake Table Output: mode **TABLE**, identifier `lake.db.demo`, Spark Catalog
   metadata name = the entry above, format `iceberg`, save mode Overwrite.
3. Lake Table Input: same TABLE identifier + catalog metadata name.

On `spark-submit`, export metadata JSON including the Spark Catalog entry (or
pass matching `--conf spark.sql.catalog.lake=…`).

## Cluster notes

`native-provided` fat jars include `lib/delta` and `lib/iceberg` from the install (Spark
itself stays out). Optional `--packages` only if those folders were stripped.

```bash
export SPARK_HOME=/path/to/spark-4.1.x-bin-hadoop3
./hop-conf.sh --generate-fat-jar=/tmp/hop-native-provided.jar \
  --spark-client-version=native-provided

"${SPARK_HOME}/bin/spark-submit" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --class org.apache.hop.spark.run.MainSpark \
  /tmp/hop-native-provided.jar \
  /path/to/pipeline.hpl \
  /path/to/metadata.json \
  YourNativeSparkRunConfig
```

Keep Delta on `spark_catalog`; put Iceberg catalogs under other names.

## Metadata injection (MDI)

Lake Table Input / Output / Merge / Maintenance expose injection keys (see transform
docs). Pattern for multi-table work:

1. Template pipeline: Lake Table transforms with empty/variable placeholders.
2. Injecting pipeline: ETL Metadata Injection maps rows (path, table id, merge ON
   clause, maintenance op, …) into the template.
3. Run the template under a **Native Spark** run configuration.

Unit coverage: `SparkLakeTable*MetaInjectionTest` in `plugins/engines/spark`.

## Automated tests (developers)

```bash
# Unit + lakehouse ITs (connectors are default runtime deps)
./mvnw -pl plugins/engines/spark -am test
```

See also `plugins/engines/spark/README.md` (frozen jar list, spike findings,
transform matrices).

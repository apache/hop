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

## Module layout

- `engines/` — `SparkPipelineEngine`, run configuration, capabilities
- `pipeline/` — Hop → Spark converter and transform handlers
- `pipeline/handler/` — native shuffle handlers + generic mapPartitions
- `core/` — `HopMapPartitionsFn`, row conversion, executor-side Hop init

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

## File I/O transforms

Dedicated **Spark File Input** / **Spark File Output** transforms (not Hop Text File Input)
provide a clean Dataset API mapping:

- **Input:** path, format, header/separator/quote (CSV), optional field schema or `inferSchema`
- **Output:** path, format, save mode (Overwrite/Append/Ignore/ErrorIfExists), header, coalesce,
  partition-by columns

Writes run as Spark actions during graph build; the output handler registers an empty leaf
Dataset so a later engine `count()` does not re-write files.

## Build

```bash
./mvnw -pl plugins/engines/spark -am test
```

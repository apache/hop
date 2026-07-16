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

# iceberg integration tests (iceberg)

End-to-end Hop IT for **Spark Lake Table** transforms on the native Spark engine
using **iceberg** PATH mode.

## Prerequisites

- Hop with `plugins/engines/spark` and connectors under `lib/delta` / `lib/iceberg`
  (default assembly since lakehouse packaging PR)
- Same environment as `integration-tests/spark-native` (Java 21)

## Scenarios

| Workflow | What |
|----------|------|
| `main-0001-input-output` | Data Grid → Lake Output → Lake Input → CSV → golden (10 rows) |
| `main-0002-overwrite-timetravel` | Write 5, overwrite 20, read current (and time travel where applicable) |
| `main-0003-merge` | Seed (1,a)(2,b), MERGE update/insert → golden 3 rows |

0002 validates double Overwrite + current read. Iceberg snapshot-id time travel is covered in Java unit tests (`SparkLakeTableTimeTravelTest`).

## Run

```bash
export HOP_LOCATION=/path/to/hop
./integration-tests/scripts/run-tests.sh iceberg
```

## Table paths

PATH mode tables under `${PROJECT_HOME}/output/` (deleted by each main workflow).
Do not commit `_delta_log` / Iceberg metadata directories.

## Work directory

All table and extract paths use **`/tmp/hop-it-iceberg/`** (not the project `output/` folder).
That avoids Docker volume permission issues when deleting Spark `.crc` files between runs.

Cleanup is a Shell action (`rm -rf` + `mkdir`) that always exits 0.

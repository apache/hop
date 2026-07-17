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

# dist/spark-native-demo

Staging directory for the Native Spark Docker walk-through. Populate with:

```bash
./plugins/engines/spark/src/samples/spark-mapping-demo/scripts/prepare-dist.sh
```

Expected artifacts (mounted into the Spark master as `/opt/hop-dist`):

- `hop-native-spark4-submit.jar` — fat jar (`native-provided`)
- `spark-mapping-demo.zip` — Native Spark project package
- `cluster-env.json` — `HOP_DATA=file:///data/hop-data`
- `data/customers-sample.csv` — optional copy for inspection

Do not commit large jars. Keep this folder gitignored for `*.jar` / `*.zip` if desired.

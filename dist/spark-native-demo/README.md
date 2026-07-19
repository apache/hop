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

# Staging location moved

Native Spark demo artifacts default to **`/tmp/spark-demo-dist`** (not under the git tree).

```bash
./plugins/engines/spark/src/samples/spark-demo/scripts/prepare-dist.sh
# → /tmp/spark-demo-dist/{hop-native-spark4-submit.jar,spark-demo.zip,hop-data/,…}

docker compose -f docker/integration-tests/integration-tests-spark-native-cluster.yaml \
  up -d --build --scale spark-worker=2
# hop-data is bind-mounted to /data/hop-data in the cluster (outputs + executions on the host)
```

Override with `DIST_DIR` / `HOP_DIST_DIR` / `HOP_DATA_HOST_DIR` if you prefer other paths.

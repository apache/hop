/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.DriverManager;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.tableinput.TableInputMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Regression: a Table Input that consumes incoming parameter rows must be routed to the generic
 * (union) handler, not the native partitioned JDBC handler, even though the plugin id is registered
 * as an explicit native handler. The converter decides per transform, so two main predecessors are
 * unioned and the sink Table Input reads each parameter row once (2 result rows here).
 */
class TableInputParametersToSparkIT {

  private static SparkSession spark;

  @BeforeAll
  static void start() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    try (var connection =
        DriverManager.getConnection("jdbc:h2:mem:table_input_params_it;DB_CLOSE_DELAY=-1")) {
      // nothing to create: sources select literals directly
    }
    spark =
        SparkSession.builder()
            .appName("hop-spark-table-input-params-it")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stop() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  private MemoryMetadataProvider provider() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider
        .getSerializer(DatabaseMeta.class)
        .save(
            new DatabaseMeta(
                "h2",
                "H2",
                "Native",
                "",
                "mem:table_input_params_it;DB_CLOSE_DELAY=-1",
                "",
                "",
                ""));
    return provider;
  }

  private static TransformMeta source(String name, String sql) {
    TableInputMeta meta = new TableInputMeta();
    meta.setConnection("h2");
    meta.setSql(sql);
    meta.setUseNamedParameters(false);
    TransformMeta tm = new TransformMeta(name, meta);
    tm.setTransformPluginId("TableInput");
    return tm;
  }

  @Test
  void twoParameterSourcesUnionIntoNonPartitionedTableInput() throws Exception {
    MemoryMetadataProvider provider = provider();

    PipelineMeta pm = new PipelineMeta();
    TransformMeta left = source("left", "SELECT 'a' AS PARAM FROM DUAL");
    TransformMeta right = source("right", "SELECT 'b' AS PARAM FROM DUAL");

    TableInputMeta sinkMeta = new TableInputMeta();
    sinkMeta.setConnection("h2");
    sinkMeta.setSql("SELECT ? AS V");
    sinkMeta.setUseNamedParameters(false);
    sinkMeta.setExecuteEachInputRow(true);
    TransformMeta sink = new TransformMeta("sink", sinkMeta);
    sink.setTransformPluginId("TableInput");

    pm.setMetadataProvider(provider);
    pm.addTransform(left);
    pm.addTransform(right);
    pm.addTransform(sink);
    pm.addPipelineHop(new PipelineHopMeta(left, sink));
    pm.addPipelineHop(new PipelineHopMeta(right, sink));

    HopPipelineMetaToSparkConverter converter =
        new HopPipelineMetaToSparkConverter(
            new Variables(), pm, provider, "local-spark", new SparkPipelineRunConfiguration());

    Dataset<Row> result = converter.createDataset(spark);
    List<Row> rows = result.collectAsList();
    assertEquals(
        2,
        rows.size(),
        "two parameter rows must be read once each, through the generic (union) handler");
    assertEquals(Set.of("a", "b"), Set.of(rows.get(0).getString(0), rows.get(1).getString(0)));
  }
}

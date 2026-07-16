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

package org.apache.hop.spark.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.pipeline.handler.SparkLakeTableMaintenanceHandler;
import org.apache.hop.spark.pipeline.handler.SparkLakeTableOutputHandler;
import org.apache.hop.spark.transforms.io.SparkFileOutputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableInputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableMaintenanceMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableOutputMeta;
import org.apache.hop.spark.util.SparkConst;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Delta OPTIMIZE smoke + destructive ack guard. Requires {@code -Plakehouse} for OPTIMIZE. */
class SparkLakeTableMaintenanceTest {

  @TempDir Path tempDir;

  private SparkSession spark;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @AfterEach
  void stopSpark() {
    if (spark != null) {
      try {
        spark.stop();
      } catch (Exception ignored) {
        // best effort
      }
      spark = null;
    }
    try {
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    } catch (Exception ignored) {
      // ignore
    }
  }

  @Test
  void vacuumRequiresAcknowledge() {
    SparkLakeTableMaintenanceMeta meta = new SparkLakeTableMaintenanceMeta();
    meta.setFormat(SparkLakeFormats.FORMAT_DELTA);
    meta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    meta.setTablePath("/tmp/t");
    meta.setOperation(SparkMaintenanceSqlBuilder.OP_VACUUM);
    meta.setRetentionHours("168");
    meta.setAcknowledgeDestructive(false);

    TransformMeta tm = new TransformMeta("maint", meta);
    tm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_MAINTENANCE_PLUGIN_ID);
    PipelineMeta pm = new PipelineMeta();
    pm.addTransform(tm);

    HopException ex =
        assertThrows(
            HopException.class,
            () ->
                new SparkLakeTableMaintenanceHandler()
                    .handleTransform(
                        LogChannel.GENERAL,
                        new Variables(),
                        "spark",
                        new SparkPipelineRunConfiguration(),
                        new MemoryMetadataProvider(),
                        "{}",
                        pm,
                        tm,
                        new HashMap<>(),
                        null,
                        new RowMeta(),
                        List.of(),
                        null));
    assertTrue(ex.getMessage().contains("destructive") || ex.getMessage().contains("acknowledge"));
  }

  @Test
  void deltaOptimizeSmoke() throws Exception {
    assumeTrue(
        SparkLakeConnectorProbe.isDeltaPresent(SparkLakeConnectorProbe.class.getClassLoader()),
        "Delta connector not on classpath; enable with -Plakehouse");

    Path tablePath = tempDir.resolve("opt_table");
    spark =
        SparkSession.builder()
            .appName("hop-delta-opt")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.shuffle.partitions", "2")
            .config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, SparkLakeFormats.DELTA_EXTENSION)
            .config(SparkLakeFormats.SPARK_CONF_SPARK_CATALOG, SparkLakeFormats.DELTA_CATALOG)
            .getOrCreate();

    // Seed small table
    writeDelta(tablePath.toString(), spark.range(0, 50).toDF("id"));

    SparkLakeTableMaintenanceMeta meta = new SparkLakeTableMaintenanceMeta();
    meta.setFormat(SparkLakeFormats.FORMAT_DELTA);
    meta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    meta.setTablePath(tablePath.toString());
    meta.setOperation(SparkMaintenanceSqlBuilder.OP_OPTIMIZE);
    meta.setAcknowledgeDestructive(false);

    TransformMeta tm = new TransformMeta("opt", meta);
    tm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_MAINTENANCE_PLUGIN_ID);
    PipelineMeta pm = new PipelineMeta();
    pm.addTransform(tm);

    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkLakeTableMaintenanceHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pm,
            tm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            null);

    assertEquals(0, map.get("opt").count());
    assertEquals(50L, spark.read().format("delta").load(tablePath.toString()).count());
  }

  private static void assertTrue(boolean cond) {
    org.junit.jupiter.api.Assertions.assertTrue(cond);
  }

  private void writeDelta(String path, Dataset<Row> data) throws Exception {
    SparkLakeTableOutputMeta outMeta = new SparkLakeTableOutputMeta();
    outMeta.setFormat(SparkLakeFormats.FORMAT_DELTA);
    outMeta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    outMeta.setTablePath(path);
    outMeta.setSaveMode(SparkFileOutputMeta.MODE_OVERWRITE);
    TransformMeta outTm = new TransformMeta("seed", outMeta);
    outTm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_OUTPUT_PLUGIN_ID);
    PipelineMeta pm = new PipelineMeta();
    pm.addTransform(outTm);
    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkLakeTableOutputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pm,
            outTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            data);
  }
}

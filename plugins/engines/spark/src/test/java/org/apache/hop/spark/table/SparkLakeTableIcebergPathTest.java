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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.pipeline.handler.SparkLakeTableInputHandler;
import org.apache.hop.spark.pipeline.handler.SparkLakeTableOutputHandler;
import org.apache.hop.spark.transforms.io.SparkFileOutputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableInputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableOutputMeta;
import org.apache.hop.spark.util.SparkConst;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Iceberg PATH Input/Output (skipped if connectors missing). */
class SparkLakeTableIcebergPathTest {

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
  void icebergPathOutputThenInputRoundTrip() throws Exception {
    assumeTrue(
        SparkLakeConnectorProbe.isIcebergPresent(SparkLakeConnectorProbe.class.getClassLoader()),
        "Iceberg connector not on classpath; connectors missing from test classpath");

    Path tablePath = tempDir.resolve("orders_iceberg");
    Path warehouse = tempDir.resolve("iceberg_wh");
    spark =
        SparkSession.builder()
            .appName("hop-lake-iceberg-path")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.host", "localhost")
            .config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, SparkLakeFormats.ICEBERG_EXTENSIONS)
            .config(
                SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG, SparkLakeFormats.ICEBERG_CATALOG)
            .config(SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG_TYPE, "hadoop")
            .config(
                SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG_WAREHOUSE,
                warehouse.toUri().toString())
            .getOrCreate();

    Dataset<Row> source = spark.range(0, 18).toDF("id");

    SparkLakeTableOutputMeta outMeta = new SparkLakeTableOutputMeta();
    outMeta.setFormat(SparkLakeFormats.FORMAT_ICEBERG);
    outMeta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    outMeta.setTablePath(tablePath.toString());
    outMeta.setSaveMode(SparkFileOutputMeta.MODE_OVERWRITE);

    TransformMeta outTm = new TransformMeta("ice_out", outMeta);
    outTm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_OUTPUT_PLUGIN_ID);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(outTm);

    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkLakeTableOutputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            outTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            source);

    assertEquals(0, map.get("ice_out").count());

    SparkLakeTableInputMeta inMeta = new SparkLakeTableInputMeta();
    inMeta.setFormat(SparkLakeFormats.FORMAT_ICEBERG);
    inMeta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    inMeta.setTablePath(tablePath.toString());

    TransformMeta inTm = new TransformMeta("ice_in", inMeta);
    inTm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID);
    pipelineMeta.addTransform(inTm);

    new SparkLakeTableInputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            inTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            null);

    assertEquals(18L, map.get("ice_in").count());
  }

  @Test
  void lakeSessionPlanCollectsIcebergFormat() throws Exception {
    SparkLakeTableInputMeta inMeta = new SparkLakeTableInputMeta();
    inMeta.setFormat(SparkLakeFormats.FORMAT_ICEBERG);
    inMeta.setTablePath("/tmp/x");
    TransformMeta inTm = new TransformMeta("in", inMeta);
    inTm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID);

    PipelineMeta pm = new PipelineMeta();
    pm.addTransform(inTm);

    LakeSessionPlan plan = LakeSessionPlan.from(pm, new MemoryMetadataProvider());
    assertEquals(true, plan.needsIceberg());
    assertEquals(false, plan.needsDelta());
  }
}

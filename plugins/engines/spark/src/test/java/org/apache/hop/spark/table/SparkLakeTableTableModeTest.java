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
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import org.apache.hop.spark.metadata.SparkCatalog;
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

/** Iceberg TABLE mode via SparkCatalog (Hadoop warehouse; skipped if connectors missing). */
class SparkLakeTableTableModeTest {

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
  void icebergTableModeRoundTripWithSparkCatalog() throws Exception {
    assumeTrue(
        SparkLakeConnectorProbe.isIcebergPresent(SparkLakeConnectorProbe.class.getClassLoader()),
        "Iceberg connector not on classpath; connectors missing from test classpath");

    Path warehouse = tempDir.resolve("wh");
    SparkCatalog catalogMeta = new SparkCatalog();
    catalogMeta.setName("lake-meta");
    catalogMeta.setCatalogName("lake");
    catalogMeta.setCatalogType(SparkCatalog.TYPE_HADOOP);
    catalogMeta.setWarehouse(warehouse.toUri().toString());

    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(SparkCatalog.class).save(catalogMeta);

    // Build session as LakeSessionPlan would for hop-run
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("hop-iceberg-table")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.shuffle.partitions", "2")
            .config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, SparkLakeFormats.ICEBERG_EXTENSIONS);
    SparkCatalogApplier.applyToBuilder(builder, catalogMeta, new Variables());
    spark = builder.getOrCreate();

    String tableId = "lake.db.orders";

    SparkLakeTableOutputMeta outMeta = new SparkLakeTableOutputMeta();
    outMeta.setFormat(SparkLakeFormats.FORMAT_ICEBERG);
    outMeta.setIdentifierMode(SparkLakeTableInputMeta.MODE_TABLE);
    outMeta.setTableIdentifier(tableId);
    outMeta.setCatalogMetadataName("lake-meta");
    outMeta.setSaveMode(SparkFileOutputMeta.MODE_OVERWRITE);

    TransformMeta outTm = new TransformMeta("out", outMeta);
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
            provider,
            "{}",
            pm,
            outTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            spark.range(0, 12).toDF("id"));

    assertEquals(0, map.get("out").count());
    assertEquals(12L, spark.table(tableId).count());

    SparkLakeTableInputMeta inMeta = new SparkLakeTableInputMeta();
    inMeta.setFormat(SparkLakeFormats.FORMAT_ICEBERG);
    inMeta.setIdentifierMode(SparkLakeTableInputMeta.MODE_TABLE);
    inMeta.setTableIdentifier(tableId);
    inMeta.setCatalogMetadataName("lake-meta");

    TransformMeta inTm = new TransformMeta("in", inMeta);
    inTm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID);
    pm.addTransform(inTm);

    new SparkLakeTableInputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            provider,
            "{}",
            pm,
            inTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            null);

    assertEquals(12L, map.get("in").count());
  }

  @Test
  void lakeSessionPlanLoadsSparkCatalog() throws Exception {
    SparkCatalog catalogMeta = new SparkCatalog();
    catalogMeta.setName("lake-meta");
    catalogMeta.setCatalogName("lake");
    catalogMeta.setCatalogType(SparkCatalog.TYPE_HADOOP);
    catalogMeta.setWarehouse("/tmp/wh");

    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(SparkCatalog.class).save(catalogMeta);

    SparkLakeTableInputMeta inMeta = new SparkLakeTableInputMeta();
    inMeta.setFormat(SparkLakeFormats.FORMAT_ICEBERG);
    inMeta.setIdentifierMode(SparkLakeTableInputMeta.MODE_TABLE);
    inMeta.setTableIdentifier("lake.db.t");
    inMeta.setCatalogMetadataName("lake-meta");

    TransformMeta inTm = new TransformMeta("in", inMeta);
    inTm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID);
    PipelineMeta pm = new PipelineMeta();
    pm.addTransform(inTm);

    LakeSessionPlan plan = LakeSessionPlan.from(pm, provider, new Variables());
    assertTrue(plan.needsIceberg());
    assertTrue(plan.needsTableMode());
    assertEquals(1, plan.getCatalogsByMetaName().size());
    assertTrue(plan.getCatalogsByMetaName().containsKey("lake-meta"));
  }
}

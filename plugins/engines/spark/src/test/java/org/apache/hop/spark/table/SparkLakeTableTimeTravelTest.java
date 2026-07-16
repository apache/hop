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

/** Time travel: write twice, read as-of first version/snapshot. Requires {@code -Plakehouse}. */
class SparkLakeTableTimeTravelTest {

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
  void deltaVersionAsOfFirstWrite() throws Exception {
    assumeTrue(
        SparkLakeConnectorProbe.isDeltaPresent(SparkLakeConnectorProbe.class.getClassLoader()),
        "Delta connector not on classpath; enable with -Plakehouse");

    Path tablePath = tempDir.resolve("delta_tt");
    spark =
        SparkSession.builder()
            .appName("hop-delta-tt")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.shuffle.partitions", "2")
            .config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, SparkLakeFormats.DELTA_EXTENSION)
            .config(SparkLakeFormats.SPARK_CONF_SPARK_CATALOG, SparkLakeFormats.DELTA_CATALOG)
            .getOrCreate();

    writeLake(tablePath.toString(), SparkLakeFormats.FORMAT_DELTA, spark.range(0, 5).toDF("id"));
    writeLake(tablePath.toString(), SparkLakeFormats.FORMAT_DELTA, spark.range(0, 20).toDF("id"));

    // Current = 20
    assertEquals(20L, readLake(tablePath.toString(), SparkLakeFormats.FORMAT_DELTA, null, null));

    // version 0 is first write (5 rows) on a new path table after overwrite creates version 0,1
    assertEquals(
        5L,
        readLake(
            tablePath.toString(),
            SparkLakeFormats.FORMAT_DELTA,
            SparkLakeTableInputMeta.TIME_TRAVEL_VERSION,
            "0"));
  }

  @Test
  void icebergSnapshotAsOfFirstWrite() throws Exception {
    assumeTrue(
        SparkLakeConnectorProbe.isIcebergPresent(SparkLakeConnectorProbe.class.getClassLoader()),
        "Iceberg connector not on classpath; enable with -Plakehouse");

    Path tablePath = tempDir.resolve("iceberg_tt");
    Path warehouse = tempDir.resolve("wh");
    spark =
        SparkSession.builder()
            .appName("hop-iceberg-tt")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.shuffle.partitions", "2")
            .config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, SparkLakeFormats.ICEBERG_EXTENSIONS)
            .config(
                SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG, SparkLakeFormats.ICEBERG_CATALOG)
            .config(SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG_TYPE, "hadoop")
            .config(
                SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG_WAREHOUSE,
                warehouse.toUri().toString())
            .getOrCreate();

    writeLake(tablePath.toString(), SparkLakeFormats.FORMAT_ICEBERG, spark.range(0, 7).toDF("id"));
    writeLake(tablePath.toString(), SparkLakeFormats.FORMAT_ICEBERG, spark.range(0, 15).toDF("id"));

    assertEquals(15L, readLake(tablePath.toString(), SparkLakeFormats.FORMAT_ICEBERG, null, null));

    String sqlId = SparkLakeTableSupport.icebergPathSqlIdentifier(tablePath.toString());
    List<Row> snaps = spark.sql("SELECT snapshot_id FROM " + sqlId + ".snapshots").collectAsList();
    // First committed snapshot corresponds to first write
    long firstSnap = snaps.get(0).getLong(0);

    assertEquals(
        7L,
        readLake(
            tablePath.toString(),
            SparkLakeFormats.FORMAT_ICEBERG,
            SparkLakeTableInputMeta.TIME_TRAVEL_VERSION,
            Long.toString(firstSnap)));
  }

  private void writeLake(String path, String format, Dataset<Row> data) throws Exception {
    SparkLakeTableOutputMeta outMeta = new SparkLakeTableOutputMeta();
    outMeta.setFormat(format);
    outMeta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    outMeta.setTablePath(path);
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

  private long readLake(String path, String format, String ttType, String version)
      throws Exception {
    SparkLakeTableInputMeta inMeta = new SparkLakeTableInputMeta();
    inMeta.setFormat(format);
    inMeta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    inMeta.setTablePath(path);
    if (ttType != null) {
      inMeta.setTimeTravelType(ttType);
      inMeta.setTimeTravelVersion(version);
    }

    TransformMeta inTm = new TransformMeta("in", inMeta);
    inTm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID);
    PipelineMeta pm = new PipelineMeta();
    pm.addTransform(inTm);

    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkLakeTableInputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pm,
            inTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            null);
    return map.get("in").count();
  }
}

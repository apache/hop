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
import org.apache.hop.spark.pipeline.handler.SparkLakeTableMergeHandler;
import org.apache.hop.spark.pipeline.handler.SparkLakeTableOutputHandler;
import org.apache.hop.spark.transforms.io.SparkFileOutputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableInputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableMergeMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableOutputMeta;
import org.apache.hop.spark.util.SparkConst;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Delta PATH MERGE upsert. Requires {@code -Plakehouse}. */
class SparkLakeTableMergeTest {

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
  void deltaPathMergeUpsert() throws Exception {
    assumeTrue(
        SparkLakeConnectorProbe.isDeltaPresent(SparkLakeConnectorProbe.class.getClassLoader()),
        "Delta connector not on classpath; enable with -Plakehouse");

    Path tablePath = tempDir.resolve("merge_orders");
    spark =
        SparkSession.builder()
            .appName("hop-delta-merge")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.shuffle.partitions", "2")
            .config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, SparkLakeFormats.DELTA_EXTENSION)
            .config(SparkLakeFormats.SPARK_CONF_SPARK_CATALOG, SparkLakeFormats.DELTA_CATALOG)
            .getOrCreate();

    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("val", DataTypes.StringType, true)
            });

    // Seed target: id 1,2
    Dataset<Row> seed =
        spark.createDataFrame(
            List.of(RowFactory.create(1L, "a"), RowFactory.create(2L, "b")), schema);
    writeDelta(tablePath.toString(), seed);

    // Source: update id=1, insert id=3
    Dataset<Row> source =
        spark.createDataFrame(
            List.of(RowFactory.create(1L, "a-updated"), RowFactory.create(3L, "c")), schema);

    SparkLakeTableMergeMeta mergeMeta = new SparkLakeTableMergeMeta();
    mergeMeta.setFormat(SparkLakeFormats.FORMAT_DELTA);
    mergeMeta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    mergeMeta.setTablePath(tablePath.toString());
    mergeMeta.setMergeCondition("t.id = s.id");
    mergeMeta.setMatchedAction(SparkMergeSqlBuilder.MATCHED_UPDATE_ALL);
    mergeMeta.setNotMatchedAction(SparkMergeSqlBuilder.NOT_MATCHED_INSERT_ALL);

    TransformMeta mergeTm = new TransformMeta("merge", mergeMeta);
    mergeTm.setTransformPluginId(SparkConst.SPARK_LAKE_TABLE_MERGE_PLUGIN_ID);
    PipelineMeta pm = new PipelineMeta();
    pm.addTransform(mergeTm);

    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkLakeTableMergeHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pm,
            mergeTm,
            map,
            spark,
            new RowMeta(),
            List.of(),
            source);

    assertEquals(0, map.get("merge").count());

    Dataset<Row> result =
        spark.read().format(SparkLakeFormats.FORMAT_DELTA).load(tablePath.toString()).orderBy("id");
    assertEquals(3L, result.count());
    List<Row> rows = result.collectAsList();
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals("a-updated", rows.get(0).getString(1));
    assertEquals(2L, rows.get(1).getLong(0));
    assertEquals("b", rows.get(1).getString(1));
    assertEquals(3L, rows.get(2).getLong(0));
    assertEquals("c", rows.get(2).getString(1));
  }

  @Test
  void resolveMergeTargetDeltaPath() throws Exception {
    SparkLakeTableMergeMeta meta = new SparkLakeTableMergeMeta();
    meta.setFormat(SparkLakeFormats.FORMAT_DELTA);
    meta.setIdentifierMode(SparkLakeTableInputMeta.MODE_PATH);
    meta.setTablePath("/tmp/orders");
    String id = SparkLakeTableSupport.resolveMergeTargetSqlId(null, new Variables(), meta, "m");
    assertEquals("delta.`/tmp/orders`", id);
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

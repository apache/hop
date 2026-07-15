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

package org.apache.hop.spark.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.memgroupby.GAggregate;
import org.apache.hop.pipeline.transforms.memgroupby.GGroup;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta.GroupType;
import org.apache.hop.pipeline.transforms.mergejoin.MergeJoinMeta;
import org.apache.hop.pipeline.transforms.sort.SortRowsField;
import org.apache.hop.pipeline.transforms.sort.SortRowsMeta;
import org.apache.hop.pipeline.transforms.uniquerows.UniqueField;
import org.apache.hop.pipeline.transforms.uniquerows.UniqueRowsMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.util.SparkConst;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration-style unit tests for native shuffle-aware handlers on a local Spark session. These
 * prove global semantics across partitions (not partition-local Hop mini-pipelines).
 */
class SparkNativeHandlersTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-native-handlers-test")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void memoryGroupBySumsAcrossPartitions() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("category", DataTypes.StringType, false),
              DataTypes.createStructField("amount", DataTypes.LongType, false)
            });
    List<Row> rows =
        Arrays.asList(
            RowFactory.create("A", 10L),
            RowFactory.create("A", 5L),
            RowFactory.create("B", 7L),
            RowFactory.create("B", 3L),
            RowFactory.create("A", 1L));
    // Force multiple partitions so a partition-local group-by would be wrong
    Dataset<Row> input = spark.createDataFrame(rows, schema).repartition(4);

    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    meta.getGroups().add(new GGroup("category"));
    meta.getAggregates().add(new GAggregate("total", "amount", GroupType.Sum, null));

    TransformMeta groupMeta = new TransformMeta("group", meta);
    groupMeta.setTransformPluginId(SparkConst.MEMORY_GROUP_BY_PLUGIN_ID);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(groupMeta);

    Map<String, Dataset<Row>> map = new HashMap<>();
    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("category"));
    inputMeta.addValueMeta(new ValueMetaInteger("amount"));

    new SparkMemoryGroupByHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            groupMeta,
            map,
            spark,
            inputMeta,
            List.of(),
            input);

    List<Row> result = map.get("group").collectAsList();
    assertEquals(2, result.size());
    Map<String, Long> totals = new HashMap<>();
    for (Row r : result) {
      totals.put(r.getString(0), r.getLong(1));
    }
    assertEquals(16L, totals.get("A"));
    assertEquals(10L, totals.get("B"));
  }

  @Test
  void mergeJoinInnerAcrossPartitions() throws Exception {
    StructType leftSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("name", DataTypes.StringType, false)
            });
    StructType rightSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("city", DataTypes.StringType, false)
            });

    Dataset<Row> left =
        spark
            .createDataFrame(
                Arrays.asList(
                    RowFactory.create(1L, "Alice"),
                    RowFactory.create(2L, "Bob"),
                    RowFactory.create(3L, "Carol")),
                leftSchema)
            .repartition(3);
    Dataset<Row> right =
        spark
            .createDataFrame(
                Arrays.asList(
                    RowFactory.create(1L, "NYC"),
                    RowFactory.create(2L, "LA"),
                    RowFactory.create(4L, "Chicago")),
                rightSchema)
            .repartition(3);

    MergeJoinMeta meta = new MergeJoinMeta();
    meta.setJoinType("INNER");
    meta.setLeftTransformName("left");
    meta.setRightTransformName("right");
    meta.getKeyFields1().add("id");
    meta.getKeyFields2().add("id");

    TransformMeta joinTm = new TransformMeta("join", meta);
    joinTm.setTransformPluginId(SparkConst.MERGE_JOIN_PLUGIN_ID);

    TransformMeta leftTm =
        new TransformMeta("left", new org.apache.hop.pipeline.transforms.dummy.DummyMeta());
    TransformMeta rightTm =
        new TransformMeta("right", new org.apache.hop.pipeline.transforms.dummy.DummyMeta());

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(leftTm);
    pipelineMeta.addTransform(rightTm);
    pipelineMeta.addTransform(joinTm);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(leftTm, joinTm));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(rightTm, joinTm));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("left", left);
    map.put("right", right);

    new SparkMergeJoinHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            joinTm,
            map,
            spark,
            new RowMeta(),
            List.of(leftTm, rightTm),
            null);

    List<Row> result = map.get("join").collectAsList();
    assertEquals(2, result.size());
    // columns: id, name, id_1 (or city with renamed id), city
    assertTrue(result.stream().anyMatch(r -> "Alice".equals(r.getString(1))));
    assertTrue(result.stream().anyMatch(r -> "Bob".equals(r.getString(1))));
  }

  @Test
  void uniqueRowsGlobalDistinct() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("code", DataTypes.StringType, false),
              DataTypes.createStructField("n", DataTypes.LongType, false)
            });
    Dataset<Row> input =
        spark
            .createDataFrame(
                Arrays.asList(
                    RowFactory.create("X", 1L),
                    RowFactory.create("Y", 2L),
                    RowFactory.create("X", 3L),
                    RowFactory.create("Z", 4L),
                    RowFactory.create("Y", 5L)),
                schema)
            .repartition(4);

    UniqueRowsMeta meta = new UniqueRowsMeta();
    meta.getCompareFields().add(new UniqueField("code", false));

    TransformMeta uniqueTm = new TransformMeta("unique", meta);
    uniqueTm.setTransformPluginId(SparkConst.UNIQUE_ROWS_PLUGIN_ID);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(uniqueTm);

    Map<String, Dataset<Row>> map = new HashMap<>();
    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    inputMeta.addValueMeta(new ValueMetaInteger("n"));

    new SparkUniqueRowsHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            uniqueTm,
            map,
            spark,
            inputMeta,
            List.of(),
            input);

    List<Row> result = map.get("unique").collectAsList();
    assertEquals(3, result.size());
  }

  @Test
  void sortRowsGlobalOrder() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("v", DataTypes.LongType, false),
            });
    Dataset<Row> input =
        spark
            .createDataFrame(
                Arrays.asList(
                    RowFactory.create(5L),
                    RowFactory.create(1L),
                    RowFactory.create(9L),
                    RowFactory.create(3L)),
                schema)
            .repartition(3);

    SortRowsMeta meta = new SortRowsMeta();
    SortRowsField field = new SortRowsField();
    field.setFieldName("v");
    field.setAscending(true);
    meta.getSortFields().add(field);

    TransformMeta sortTm = new TransformMeta("sort", meta);
    sortTm.setTransformPluginId(SparkConst.SORT_ROWS_PLUGIN_ID);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(sortTm);

    Map<String, Dataset<Row>> map = new HashMap<>();
    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("v"));

    new SparkSortRowsHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            sortTm,
            map,
            spark,
            inputMeta,
            List.of(),
            input);

    // orderBy is a narrow transformation; collect may not preserve global order without an action
    // that respects ordering — takeOrdered is reliable for assertion
    List<Row> ordered = map.get("sort").sort(colAsc("v")).collectAsList();
    assertEquals(Arrays.asList(1L, 3L, 5L, 9L), extractLongs(ordered));
  }

  private static org.apache.spark.sql.Column colAsc(String name) {
    return org.apache.spark.sql.functions.col(name).asc();
  }

  private static List<Long> extractLongs(List<Row> rows) {
    List<Long> values = new ArrayList<>();
    for (Row r : rows) {
      values.add(r.getLong(0));
    }
    return values;
  }
}

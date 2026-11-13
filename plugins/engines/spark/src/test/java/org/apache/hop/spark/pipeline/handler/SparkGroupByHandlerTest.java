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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.groupby.Aggregation;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupingField;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.spark.core.HopSparkRowConverter;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Group By on 4 partitions gives the rows of the local engine, for every aggregation type and
 * option. The input is sorted on the grouping field, as Group By expects, and spread over the
 * partitions in order, so most groups span two partitions: the handler has to bring them together.
 */
class SparkGroupByHandlerTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-group-by-test")
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
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

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("key"));
    rowMeta.addValueMeta(new ValueMetaInteger("int_v"));
    rowMeta.addValueMeta(new ValueMetaNumber("num_v"));
    rowMeta.addValueMeta(new ValueMetaString("str_v"));
    rowMeta.addValueMeta(new ValueMetaBoolean("skip"));
    return rowMeta;
  }

  /** 24 rows, sorted on key (groups of 7, 5, 7 and 5 rows), nulls in every field. */
  private static List<Object[]> inputRows() {
    List<Object[]> rows = new ArrayList<>();
    String[] keys = {"a", "b", "c", null};
    int[] sizes = {7, 5, 7, 5};
    String[] words = {"red", "blue", "red", null, "green", "blue", "amber"};
    int n = 0;
    for (int k = 0; k < keys.length; k++) {
      for (int i = 0; i < sizes[k]; i++) {
        n++;
        rows.add(
            new Object[] {
              keys[k],
              n % 6 == 0 ? null : (long) ((n * 37) % 23 - 7),
              n % 5 == 0 ? null : ((n * 13) % 17) / 4.0,
              words[(n * 3) % words.length],
              n % 4 == 0
            });
      }
    }
    return rows;
  }

  private static Aggregation aggregation(String field, String subject, int type, String value) {
    return new Aggregation(field, subject, Aggregation.getTypeDescLongFromCode(type), value);
  }

  private static GroupByMeta groupBy(List<String> keys, List<Aggregation> aggregations) {
    GroupByMeta meta = new GroupByMeta();
    meta.setDefault();
    List<GroupingField> grouping = new ArrayList<>();
    for (String key : keys) {
      grouping.add(new GroupingField(key));
    }
    meta.setGroupingFields(grouping);
    meta.setAggregations(new ArrayList<>(aggregations));
    return meta;
  }

  /** Every aggregation type of Group By, on a subject that suits it. */
  private static List<Aggregation> everyAggregation() {
    List<Aggregation> all = new ArrayList<>();
    all.add(aggregation("sum", "int_v", Aggregation.TYPE_GROUP_SUM, null));
    all.add(aggregation("avg", "num_v", Aggregation.TYPE_GROUP_AVERAGE, null));
    all.add(aggregation("avg_int", "int_v", Aggregation.TYPE_GROUP_AVERAGE, null));
    all.add(aggregation("median", "num_v", Aggregation.TYPE_GROUP_MEDIAN, null));
    all.add(aggregation("p90", "num_v", Aggregation.TYPE_GROUP_PERCENTILE, "90"));
    all.add(aggregation("min", "str_v", Aggregation.TYPE_GROUP_MIN, null));
    all.add(aggregation("max", "int_v", Aggregation.TYPE_GROUP_MAX, null));
    all.add(aggregation("count_all", "int_v", Aggregation.TYPE_GROUP_COUNT_ALL, null));
    all.add(aggregation("concat_comma", "str_v", Aggregation.TYPE_GROUP_CONCAT_COMMA, null));
    all.add(aggregation("first", "str_v", Aggregation.TYPE_GROUP_FIRST, null));
    all.add(aggregation("last", "str_v", Aggregation.TYPE_GROUP_LAST, null));
    all.add(aggregation("first_null", "int_v", Aggregation.TYPE_GROUP_FIRST_INCL_NULL, null));
    all.add(aggregation("last_null", "int_v", Aggregation.TYPE_GROUP_LAST_INCL_NULL, null));
    all.add(aggregation("cum_sum", "int_v", Aggregation.TYPE_GROUP_CUMULATIVE_SUM, null));
    all.add(aggregation("cum_avg", "num_v", Aggregation.TYPE_GROUP_CUMULATIVE_AVERAGE, null));
    all.add(aggregation("stddev", "num_v", Aggregation.TYPE_GROUP_STANDARD_DEVIATION, null));
    all.add(aggregation("concat", "str_v", Aggregation.TYPE_GROUP_CONCAT_STRING, "|"));
    all.add(aggregation("count_distinct", "str_v", Aggregation.TYPE_GROUP_COUNT_DISTINCT, null));
    all.add(aggregation("count_any", "int_v", Aggregation.TYPE_GROUP_COUNT_ANY, null));
    all.add(
        aggregation(
            "stddev_sample", "num_v", Aggregation.TYPE_GROUP_STANDARD_DEVIATION_SAMPLE, null));
    all.add(aggregation("p75_rank", "num_v", Aggregation.TYPE_GROUP_PERCENTILE_NEAREST_RANK, "75"));
    all.add(aggregation("concat_crlf", "str_v", Aggregation.TYPE_GROUP_CONCAT_STRING_CRLF, null));
    all.add(aggregation("concat_distinct", "str_v", Aggregation.TYPE_GROUP_CONCAT_DISTINCT, ";"));
    all.add(aggregation("moving_avg", "num_v", Aggregation.TYPE_GROUP_MOVING_AVERAGE, "2"));
    return all;
  }

  /** src (Injector with the input fields) -> group. */
  private static PipelineMeta pipeline(GroupByMeta groupBy) {
    InjectorMeta injector = new InjectorMeta();
    for (var valueMeta : inputRowMeta().getValueMetaList()) {
      injector
          .getInjectorFields()
          .add(new InjectorField(valueMeta.getName(), valueMeta.getTypeDesc(), "-1", "-1"));
    }
    TransformMeta src = new TransformMeta("src", injector);
    src.setTransformPluginId("Injector");
    TransformMeta group = new TransformMeta("group", groupBy);
    group.setTransformPluginId("GroupBy");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(src);
    pipelineMeta.addTransform(group);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(src, group));
    return pipelineMeta;
  }

  /** The rows the local engine's Group By writes, as Spark rows of the declared layout. */
  private static List<String> onTheLocalEngine(PipelineMeta pipelineMeta, List<Object[]> rows)
      throws Exception {
    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta, new Variables(), null);
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    pipeline.prepareExecution();
    List<Object[]> written = new ArrayList<>();
    pipeline
        .getTransform("group", 0)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                written.add(row.clone());
              }
            });
    RowProducer producer = pipeline.addRowProducer("src", 0);
    pipeline.startThreads();
    for (Object[] row : rows) {
      producer.putRow(inputRowMeta(), row.clone());
    }
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors(), "errors on the local engine");
    IRowMeta outputRowMeta = pipelineMeta.getTransformFields(new Variables(), "group");
    HopSparkRowConverter.RowCodec codec = HopSparkRowConverter.RowCodec.of(outputRowMeta);
    List<String> result = new ArrayList<>();
    for (Object[] row : written) {
      result.add(codec.toSpark(row).toString());
    }
    Collections.sort(result);
    return result;
  }

  /** The rows of the native handler on 4 partitions. */
  private static List<String> onSpark(PipelineMeta pipelineMeta, List<Object[]> rows)
      throws Exception {
    IRowMeta rowMeta = inputRowMeta();
    HopSparkRowConverter.RowCodec codec = HopSparkRowConverter.RowCodec.of(rowMeta);
    List<Row> sparkRows = new ArrayList<>();
    for (Object[] row : rows) {
      sparkRows.add(codec.toSpark(row));
    }
    Dataset<Row> input =
        SparkGenericTransformHandler.spreadInOrder(
            spark.createDataFrame(sparkRows, HopSparkRowConverter.toStructType(rowMeta)), 4);
    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkGroupByHandler(new SparkGenericTransformHandler())
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            pipelineMeta.findTransform("group"),
            map,
            spark,
            rowMeta,
            List.of(pipelineMeta.findTransform("src")),
            input);
    List<String> result = new ArrayList<>();
    for (Row row : map.get("group").collectAsList()) {
      result.add(row.toString());
    }
    Collections.sort(result);
    return result;
  }

  private static void assertSameRows(GroupByMeta groupBy, List<Object[]> rows) throws Exception {
    PipelineMeta pipelineMeta = pipeline(groupBy);
    List<String> local = onTheLocalEngine(pipelineMeta, rows);
    List<String> spark = onSpark(pipeline(groupBy), rows);
    assertEquals(local, spark);
  }

  @Test
  void everyAggregationTypeGivesTheLocalEngineRows() throws Exception {
    GroupByMeta groupBy = groupBy(List.of("key"), everyAggregation());
    List<String> local = onTheLocalEngine(pipeline(groupBy), inputRows());
    assertEquals(4, local.size(), "one row per key, the null key included");
    assertEquals(local, onSpark(pipeline(groupBy), inputRows()));
  }

  @Test
  void everyAggregationTypeWithoutGroupingFields() throws Exception {
    assertSameRows(groupBy(List.of(), everyAggregation()), inputRows());
  }

  @Test
  void allRowsWithLineNumbersCumulativeAndMovingAggregates() throws Exception {
    GroupByMeta groupBy = groupBy(List.of("key"), everyAggregation());
    groupBy.setPassAllRows(true);
    groupBy.setAddingLineNrInGroup(true);
    groupBy.setLineNrInGroupField("line_nr");
    List<String> local = onTheLocalEngine(pipeline(groupBy), inputRows());
    assertEquals(inputRows().size(), local.size(), "every row comes back");
    assertEquals(local, onSpark(pipeline(groupBy), inputRows()));
  }

  @Test
  void ignoredRowsAreLeftOutOfTheAggregates() throws Exception {
    GroupByMeta groupBy = groupBy(List.of("key"), everyAggregation());
    groupBy.setAggregateIgnored(true);
    groupBy.setAggregateIgnoredField("skip");
    assertSameRows(groupBy, inputRows());
  }

  @Test
  void anEmptyInputStillGivesOneRowWhenAsked() throws Exception {
    GroupByMeta groupBy = groupBy(List.of(), everyAggregation());
    groupBy.setAlwaysGivingBackOneRow(true);
    List<String> local = onTheLocalEngine(pipeline(groupBy), List.of());
    assertEquals(1, local.size());
    assertEquals(local, onSpark(pipeline(groupBy), List.of()));
  }

  @Test
  void anEmptyInputGivesNothingOtherwise() throws Exception {
    GroupByMeta groupBy = groupBy(List.of("key"), everyAggregation());
    assertEquals(List.of(), onSpark(pipeline(groupBy), List.of()));
  }

  @Test
  void twoGroupingFields() throws Exception {
    List<Object[]> rows = new ArrayList<>(inputRows());
    rows.sort(
        (a, b) -> {
          int c = String.valueOf(a[0]).compareTo(String.valueOf(b[0]));
          return c != 0 ? c : String.valueOf(a[3]).compareTo(String.valueOf(b[3]));
        });
    GroupByMeta groupBy = groupBy(List.of("key", "str_v"), everyAggregation());
    List<String> local = onTheLocalEngine(pipeline(groupBy), rows);
    assertFalse(local.isEmpty());
    assertEquals(local, onSpark(pipeline(groupBy), rows));
  }
}

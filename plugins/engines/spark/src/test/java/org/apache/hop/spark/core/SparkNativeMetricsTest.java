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

package org.apache.hop.spark.core;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

class SparkNativeMetricsTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("hop-spark-native-metrics-test")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.shuffle.partitions", "3")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static Dataset<Row> ids(int n, int partitions) {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
            });
    List<Row> rows = new ArrayList<>();
    for (long i = 0; i < n; i++) {
      rows.add(RowFactory.create(i));
    }
    return spark.createDataFrame(rows, schema).repartition(partitions);
  }

  private static SparkTransformMetricsAccumulator accumulator(String name) {
    SparkTransformMetricsAccumulator acc = new SparkTransformMetricsAccumulator();
    spark.sparkContext().register(acc, name);
    return acc;
  }

  private static SparkNativeMetricsListener listen(SparkTransformMetricsAccumulator acc) {
    SparkNativeMetricsListener listener = new SparkNativeMetricsListener(acc);
    listener.addTo(spark.sparkContext());
    return listener;
  }

  private static Map<String, SparkTransformMetricSlice> settle(
      SparkNativeMetricsListener listener, SparkTransformMetricsAccumulator acc) {
    listener.flush(spark.sparkContext(), 10_000L);
    listener.removeFrom(spark.sparkContext());
    return acc.value();
  }

  private static long sum(
      Map<String, SparkTransformMetricSlice> slices,
      String transform,
      java.util.function.ToLongFunction<SparkTransformMetricSlice> f) {
    return slices.values().stream()
        .filter(s -> transform.equals(s.getTransformName()))
        .mapToLong(f)
        .sum();
  }

  @Test
  void trackReportsInputRoleAcrossPartitionsWithoutRddBarrier() {
    Dataset<Row> input = ids(40, 4);
    SparkTransformMetricsAccumulator acc = accumulator("native-metrics-input");
    SparkNativeMetricsListener listener = listen(acc);

    Dataset<Row> tracked =
        SparkNativeMetrics.track(input, "file-in", listener, SparkNativeMetrics.Role.INPUT);
    // Anchor only: the lineage stays a Dataset plan (no LogicalRDD from createDataFrame(rdd))
    assertTrue(tracked.queryExecution().optimizedPlan().toString().contains("CollectMetrics"));
    assertFalse(tracked.queryExecution().optimizedPlan().toString().contains("LogicalRDD"));
    assertEquals(40L, tracked.count());

    Map<String, SparkTransformMetricSlice> slices = settle(listener, acc);
    assertFalse(slices.isEmpty());
    Set<Integer> copies = new HashSet<>();
    for (SparkTransformMetricSlice slice : slices.values()) {
      assertEquals("file-in", slice.getTransformName());
      assertTrue(slice.isFinished());
      assertTrue(slice.getStartTimeMs() > 0, "partition should record start time");
      assertTrue(slice.getEndTimeMs() >= slice.getStartTimeMs(), "end should be >= start");
      copies.add(slice.getCopyNr());
    }
    assertEquals(40L, sum(slices, "file-in", SparkTransformMetricSlice::getLinesInput));
    assertEquals(40L, sum(slices, "file-in", SparkTransformMetricSlice::getLinesWritten));
    assertTrue(copies.size() >= 2, "expected multi-partition copies, got " + copies);
  }

  @Test
  void trackReportsOutputAndTransformRoles() {
    Dataset<Row> input = ids(3, 2);
    SparkTransformMetricsAccumulator acc = accumulator("native-metrics-roles");
    SparkNativeMetricsListener listener = listen(acc);

    Dataset<Row> outTracked =
        SparkNativeMetrics.track(input, "file-out", listener, SparkNativeMetrics.Role.OUTPUT);
    assertEquals(3L, outTracked.count());
    Dataset<Row> txTracked =
        SparkNativeMetrics.track(input, "sort", listener, SparkNativeMetrics.Role.TRANSFORM);
    assertEquals(3L, txTracked.count());

    Map<String, SparkTransformMetricSlice> slices = settle(listener, acc);
    assertEquals(3L, sum(slices, "file-out", SparkTransformMetricSlice::getLinesOutput));
    assertEquals(3L, sum(slices, "file-out", SparkTransformMetricSlice::getLinesRead));
    assertEquals(0L, sum(slices, "file-out", SparkTransformMetricSlice::getLinesInput));
    assertEquals(3L, sum(slices, "sort", SparkTransformMetricSlice::getLinesWritten));
    assertEquals(3L, sum(slices, "sort", SparkTransformMetricSlice::getLinesRead));
  }

  @Test
  void countsAfterShuffleAreNotDoubledBySortSampling() {
    // group by → sort: range partitioning samples the aggregate once before the real pass
    Dataset<Row> input = ids(100, 4).withColumn("k", col("id").mod(5));
    SparkTransformMetricsAccumulator acc = accumulator("native-metrics-shuffle");
    SparkNativeMetricsListener listener = listen(acc);

    Dataset<Row> in =
        SparkNativeMetrics.track(input, "in", listener, SparkNativeMetrics.Role.INPUT);
    Dataset<Row> grouped = in.groupBy(col("k")).agg(count(col("id")).alias("n"));
    grouped =
        SparkNativeMetrics.track(grouped, "group", listener, SparkNativeMetrics.Role.TRANSFORM);
    Dataset<Row> sorted = grouped.orderBy(col("k"));
    sorted = SparkNativeMetrics.track(sorted, "sort", listener, SparkNativeMetrics.Role.TRANSFORM);
    assertEquals(5L, sorted.count());

    Map<String, SparkTransformMetricSlice> slices = settle(listener, acc);
    slices
        .values()
        .forEach(
            sl ->
                System.out.println(
                    "SLICE "
                        + sl.getTransformName()
                        + " copy="
                        + sl.getCopyNr()
                        + " in="
                        + sl.getLinesInput()
                        + " written="
                        + sl.getLinesWritten()
                        + " start="
                        + sl.getStartTimeMs()
                        + " end="
                        + sl.getEndTimeMs()));
    System.out.println(sorted.queryExecution().executedPlan().toString());
    assertEquals(100L, sum(slices, "in", SparkTransformMetricSlice::getLinesInput));
    assertEquals(5L, sum(slices, "group", SparkTransformMetricSlice::getLinesWritten));
    assertEquals(5L, sum(slices, "sort", SparkTransformMetricSlice::getLinesWritten));
  }

  @Test
  void tokensAreUniquePerListener() {
    SparkTransformMetricsAccumulator acc = new SparkTransformMetricsAccumulator();
    SparkNativeMetricsListener a = new SparkNativeMetricsListener(acc);
    SparkNativeMetricsListener b = new SparkNativeMetricsListener(acc);
    String t1 = a.register("x", SparkNativeMetrics.Role.TRANSFORM);
    String t2 = a.register("x", SparkNativeMetrics.Role.TRANSFORM);
    assertNotEquals(t1, t2);
    assertNotEquals(t1, b.register("x", SparkNativeMetrics.Role.TRANSFORM));
    assertTrue(t1.startsWith(SparkNativeMetricsListener.TOKEN_PREFIX));
  }

  @Test
  void trackIsNoOpWithoutListener() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("v", DataTypes.IntegerType, false),
            });
    Dataset<Row> input = spark.createDataFrame(List.of(RowFactory.create(1)), schema);
    Dataset<Row> same =
        SparkNativeMetrics.track(input, "x", null, SparkNativeMetrics.Role.TRANSFORM);
    assertEquals(input, same);
  }
}

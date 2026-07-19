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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void trackReportsInputRoleAcrossPartitions() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
            });
    List<Row> rows = new ArrayList<>();
    for (long i = 0; i < 40; i++) {
      rows.add(RowFactory.create(i));
    }
    Dataset<Row> input = spark.createDataFrame(rows, schema).repartition(4);

    SparkTransformMetricsAccumulator acc = new SparkTransformMetricsAccumulator();
    spark.sparkContext().register(acc, "native-metrics-input");

    Dataset<Row> tracked =
        SparkNativeMetrics.track(input, "file-in", acc, SparkNativeMetrics.Role.INPUT);
    assertEquals(40L, tracked.count());

    Map<String, SparkTransformMetricSlice> slices = acc.value();
    assertFalse(slices.isEmpty());

    long totalInput = 0;
    long totalWritten = 0;
    Set<Integer> copies = new HashSet<>();
    for (SparkTransformMetricSlice slice : slices.values()) {
      assertEquals("file-in", slice.getTransformName());
      assertTrue(slice.isFinished());
      assertTrue(slice.getStartTimeMs() > 0, "partition should record start time");
      assertTrue(slice.getEndTimeMs() >= slice.getStartTimeMs(), "end should be >= start");
      totalInput += slice.getLinesInput();
      totalWritten += slice.getLinesWritten();
      copies.add(slice.getCopyNr());
    }
    assertEquals(40L, totalInput);
    assertEquals(40L, totalWritten);
    assertTrue(copies.size() >= 2, "expected multi-partition copies, got " + copies);
  }

  @Test
  void trackReportsOutputAndTransformRoles() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("v", DataTypes.StringType, false),
            });
    List<Row> rows =
        List.of(RowFactory.create("a"), RowFactory.create("b"), RowFactory.create("c"));
    Dataset<Row> input = spark.createDataFrame(rows, schema).repartition(2);

    SparkTransformMetricsAccumulator outAcc = new SparkTransformMetricsAccumulator();
    spark.sparkContext().register(outAcc, "native-metrics-output");
    Dataset<Row> outTracked =
        SparkNativeMetrics.track(input, "file-out", outAcc, SparkNativeMetrics.Role.OUTPUT);
    assertEquals(3L, outTracked.count());
    long outSum =
        outAcc.value().values().stream().mapToLong(SparkTransformMetricSlice::getLinesOutput).sum();
    long readSum =
        outAcc.value().values().stream().mapToLong(SparkTransformMetricSlice::getLinesRead).sum();
    assertEquals(3L, outSum);
    assertEquals(3L, readSum);

    SparkTransformMetricsAccumulator txAcc = new SparkTransformMetricsAccumulator();
    spark.sparkContext().register(txAcc, "native-metrics-tx");
    Dataset<Row> txTracked =
        SparkNativeMetrics.track(input, "sort", txAcc, SparkNativeMetrics.Role.TRANSFORM);
    assertEquals(3L, txTracked.count());
    long written =
        txAcc.value().values().stream().mapToLong(SparkTransformMetricSlice::getLinesWritten).sum();
    long read =
        txAcc.value().values().stream().mapToLong(SparkTransformMetricSlice::getLinesRead).sum();
    assertEquals(3L, written);
    assertEquals(3L, read);
  }

  @Test
  void trackIsNoOpWithoutAccumulator() {
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

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

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.spark.core.SparkTransformMetricSlice;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.pipeline.handler.SparkGenericTransformHandler;
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
 * Verifies mapPartitions transforms publish per-partition Hop counters into a Spark accumulator,
 * and that {@link SparkPipelineEngine#populateEngineMetrics()} maps them to engine components.
 */
class SparkPipelineEngineMetricsTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-engine-metrics-test")
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
  void mapPartitionsReportsPerPartitionMetrics() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, false),
            });
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      rows.add(RowFactory.create("r" + i));
    }
    // Multiple partitions → multiple engine component copies for the same transform
    Dataset<Row> input = spark.createDataFrame(rows, schema).repartition(4);

    SparkTransformMetricsAccumulator acc = new SparkTransformMetricsAccumulator();
    spark.sparkContext().register(acc, "hop-metrics-test");

    DummyMeta dummyMeta = new DummyMeta();
    TransformMeta dummyTm = new TransformMeta("pass", dummyMeta);
    dummyTm.setTransformPluginId("Dummy");

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(dummyTm);

    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("name"));

    SparkGenericTransformHandler handler = new SparkGenericTransformHandler();
    handler.setMetricsAccumulator(acc);

    Map<String, Dataset<Row>> map = new HashMap<>();
    handler.handleTransform(
        LogChannel.GENERAL,
        new Variables(),
        "spark",
        new SparkPipelineRunConfiguration(),
        new MemoryMetadataProvider(),
        "{}",
        pipelineMeta,
        dummyTm,
        map,
        spark,
        inputMeta,
        List.of(),
        input);

    long count = map.get("pass").count();
    assertEquals(20L, count);

    Map<String, SparkTransformMetricSlice> slices = acc.value();
    assertFalse(slices.isEmpty(), "expected metric slices from mapPartitions");

    long totalRead = 0;
    long totalWritten = 0;
    Set<Integer> copyNrs = new HashSet<>();
    for (SparkTransformMetricSlice slice : slices.values()) {
      assertEquals("pass", slice.getTransformName());
      assertTrue(slice.isFinished());
      assertFalse(slice.isRunning());
      totalRead += slice.getLinesRead();
      totalWritten += slice.getLinesWritten();
      copyNrs.add(slice.getCopyNr());
    }
    // Dummy reads and writes each row once across all partitions
    assertEquals(20L, totalRead);
    assertEquals(20L, totalWritten);
    assertTrue(
        copyNrs.size() >= 2,
        "expected multiple partition copies, got copyNrs=" + copyNrs + " slices=" + slices.size());
  }

  @Test
  void populateEngineMetricsMapsSlicesToComponents() throws Exception {
    SparkPipelineEngine engine = new SparkPipelineEngine();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("metrics-seed");
    TransformMeta nativeLike = new TransformMeta("file-in", new DummyMeta());
    nativeLike.setTransformPluginId("Dummy");
    TransformMeta mapPart = new TransformMeta("pass", new DummyMeta());
    mapPart.setTransformPluginId("Dummy");
    pipelineMeta.addTransform(nativeLike);
    pipelineMeta.addTransform(mapPart);
    engine.setPipelineMeta(pipelineMeta);

    long t0 = 1_700_000_000_000L;
    SparkTransformMetricsAccumulator acc = new SparkTransformMetricsAccumulator();
    acc.add(
        new SparkTransformMetricSlice(
            "pass", 0, "host-a", 10, 10, 0, 0, 0, false, true, t0, t0 + 2_000L));
    acc.add(
        new SparkTransformMetricSlice(
            "pass", 1, "host-b", 5, 5, 0, 0, 0, false, true, t0 + 100L, t0 + 1_100L));

    // Reflect package-visible wiring via package-private-ish: set through start path is heavy;
    // call populate after injecting via setMetrics using reflection-free package access —
    // populateEngineMetrics is protected; test is same package.
    setMetricsFields(engine, acc);
    engine.setFinished(true);
    engine.setRunning(false);
    engine.setExecutionStartDate(new java.util.Date(t0 - 1000));
    engine.setExecutionEndDate(new java.util.Date(t0 + 3000));
    // status finished
    engine.populateEngineMetrics();

    EngineMetrics metrics = engine.getEngineMetrics();
    assertNotNull(metrics);
    List<IEngineComponent> passCopies = engine.getComponentCopies("pass");
    assertEquals(2, passCopies.size());

    IEngineComponent c0 = engine.findComponent("pass", 0);
    IEngineComponent c1 = engine.findComponent("pass", 1);
    assertNotNull(c0);
    assertNotNull(c1);
    assertEquals(10L, c0.getLinesRead());
    assertEquals(5L, c1.getLinesRead());
    assertEquals(ComponentExecutionStatus.STATUS_FINISHED, c0.getStatus());

    // Duration: GUI uses firstRowReadDate / lastRowWrittenDate per component copy
    assertNotNull(c0.getFirstRowReadDate());
    assertNotNull(c0.getLastRowWrittenDate());
    assertEquals(t0, c0.getFirstRowReadDate().getTime());
    assertEquals(t0 + 2_000L, c0.getLastRowWrittenDate().getTime());
    assertEquals(2_000L, c0.getExecutionDuration());
    assertEquals(1_000L, c1.getExecutionDuration());

    // Native/placeholder transform without slices still listed
    IEngineComponent fileIn = engine.findComponent("file-in", 0);
    assertNotNull(fileIn);

    Long read0 = metrics.getComponentMetric(c0, Pipeline.METRIC_READ);
    assertEquals(10L, read0);

    EngineMetrics filtered = engine.getEngineMetrics("pass", 1);
    assertEquals(1, filtered.getComponents().size());
    assertEquals(1, filtered.getComponents().get(0).getCopyNr());
  }

  /** Package-level field injection for unit testing populateEngineMetrics. */
  private static void setMetricsFields(
      SparkPipelineEngine engine, SparkTransformMetricsAccumulator acc) throws Exception {
    var accField = SparkPipelineEngine.class.getDeclaredField("metricsAccumulator");
    accField.setAccessible(true);
    accField.set(engine, acc);
  }
}

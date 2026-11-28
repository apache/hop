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

package org.apache.hop.spark.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Condition;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.filterrows.FilterRowsMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.spark.core.SparkTransformMetricSlice;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
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

/** Neighbouring generic transforms run in one mini-pipeline and give the same rows. */
class SparkFusedChainTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-fused-chain-test")
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

  /** src (Dummy) -> above (n > 2) -> below (n < 18) -> out (Dummy). */
  private static PipelineMeta filterChain() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta src = source();
    TransformMeta above = filter("above", Condition.Function.LARGER, 2L);
    TransformMeta below = filter("below", Condition.Function.SMALLER, 18L);
    TransformMeta out = dummy("out");
    for (TransformMeta t : List.of(src, above, below, out)) {
      pipelineMeta.addTransform(t);
    }
    pipelineMeta.addPipelineHop(new PipelineHopMeta(src, above));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(above, below));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(below, out));
    return pipelineMeta;
  }

  /** A source that declares the fields n (Integer) and name (String). */
  private static TransformMeta source() {
    InjectorMeta meta = new InjectorMeta();
    meta.getInjectorFields().add(new InjectorField("n", "Integer", "-1", "-1"));
    meta.getInjectorFields().add(new InjectorField("name", "String", "-1", "-1"));
    TransformMeta transformMeta = new TransformMeta("src", meta);
    transformMeta.setTransformPluginId("Injector");
    return transformMeta;
  }

  private static TransformMeta dummy(String name) {
    TransformMeta meta = new TransformMeta(name, new DummyMeta());
    meta.setTransformPluginId("Dummy");
    return meta;
  }

  private static TransformMeta filter(String name, Condition.Function function, long value)
      throws Exception {
    FilterRowsMeta meta = new FilterRowsMeta();
    meta.setCondition(
        new Condition(
            "n", function, null, new ValueMetaAndData(new ValueMetaInteger("constant"), value)));
    TransformMeta transformMeta = new TransformMeta(name, meta);
    transformMeta.setTransformPluginId("FilterRows");
    return transformMeta;
  }

  private static HopPipelineMetaToSparkConverter converter(PipelineMeta pipelineMeta)
      throws Exception {
    return new HopPipelineMetaToSparkConverter(
        new Variables(),
        pipelineMeta,
        new MemoryMetadataProvider(),
        "spark",
        new SparkPipelineRunConfiguration());
  }

  private static Dataset<Row> numbers() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("n", DataTypes.LongType, false),
              DataTypes.createStructField("name", DataTypes.StringType, true),
            });
    List<Row> rows = new ArrayList<>();
    for (long i = 0; i < 20; i++) {
      rows.add(RowFactory.create(i, "r" + i));
    }
    return spark.createDataFrame(rows, schema).repartition(4);
  }

  private static IRowMeta numbersRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("n"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  @Test
  void neighbouringGenericTransformsFormOneChain() throws Exception {
    PipelineMeta pipelineMeta = filterChain();
    HopPipelineMetaToSparkConverter converter = converter(pipelineMeta);
    Map<String, List<TransformMeta>> chains =
        converter.findFusedChains(pipelineMeta.getTransforms());
    // The source has no input and the Dummy is native (no row conversion): only the filters fuse
    assertEquals(1, chains.size());
    assertEquals(
        List.of("below"), chains.get("above").stream().map(TransformMeta::getName).toList());
  }

  @Test
  void aTransformThatCopiesToSeveralHopsEndsTheChain() throws Exception {
    PipelineMeta pipelineMeta = filterChain();
    TransformMeta above = pipelineMeta.findTransform("above");
    TransformMeta extra = dummy("extra");
    pipelineMeta.addTransform(extra);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(above, extra));
    assertTrue(converter(pipelineMeta).findFusedChains(pipelineMeta.getTransforms()).isEmpty());
  }

  @Test
  void aFusedChainGivesTheRowsOfSeparateTransforms() throws Exception {
    PipelineMeta pipelineMeta = filterChain();
    TransformMeta above = pipelineMeta.findTransform("above");
    TransformMeta below = pipelineMeta.findTransform("below");
    SparkPipelineRunConfiguration runConfiguration = new SparkPipelineRunConfiguration();

    SparkTransformMetricsAccumulator metrics = new SparkTransformMetricsAccumulator();
    spark.sparkContext().register(metrics, "hop-fused-chain-test");
    SparkGenericTransformHandler handler = new SparkGenericTransformHandler();
    handler.setMetricsAccumulator(metrics);

    Map<String, Dataset<Row>> fused = new HashMap<>();
    handler.handleFusedTransforms(
        LogChannel.GENERAL,
        new Variables(),
        "spark",
        runConfiguration,
        "{}",
        pipelineMeta,
        above,
        List.of(below),
        fused,
        spark,
        numbersRowMeta(),
        numbers());
    List<Long> fusedRows = sortedNumbers(fused.get("below"));

    SparkGenericTransformHandler plain = new SparkGenericTransformHandler();
    Map<String, Dataset<Row>> separate = new HashMap<>();
    plain.handleTransform(
        LogChannel.GENERAL,
        new Variables(),
        "spark",
        runConfiguration,
        new MemoryMetadataProvider(),
        "{}",
        pipelineMeta,
        above,
        separate,
        spark,
        numbersRowMeta(),
        List.of(pipelineMeta.findTransform("src")),
        numbers());
    plain.handleTransform(
        LogChannel.GENERAL,
        new Variables(),
        "spark",
        runConfiguration,
        new MemoryMetadataProvider(),
        "{}",
        pipelineMeta,
        below,
        separate,
        spark,
        numbersRowMeta(),
        List.of(above),
        separate.get("above"));

    List<Long> expected = new ArrayList<>();
    for (long i = 3; i < 18; i++) {
      expected.add(i);
    }
    assertEquals(expected, fusedRows);
    assertEquals(expected, sortedNumbers(separate.get("below")));

    // Each transform of the chain still reports its own counters
    long aboveRead = 0;
    long aboveWritten = 0;
    long belowRead = 0;
    long belowWritten = 0;
    for (SparkTransformMetricSlice slice : metrics.value().values()) {
      if ("above".equals(slice.getTransformName())) {
        aboveRead += slice.getLinesRead();
        aboveWritten += slice.getLinesWritten();
      } else if ("below".equals(slice.getTransformName())) {
        belowRead += slice.getLinesRead();
        belowWritten += slice.getLinesWritten();
      }
    }
    assertEquals(20, aboveRead);
    assertEquals(17, aboveWritten);
    assertEquals(17, belowRead);
    assertEquals(15, belowWritten);
  }

  private static List<Long> sortedNumbers(Dataset<Row> dataset) {
    List<Long> numbers = new ArrayList<>();
    for (Row row : dataset.collectAsList()) {
      numbers.add(row.getLong(0));
    }
    numbers.sort(Long::compare);
    return numbers;
  }
}

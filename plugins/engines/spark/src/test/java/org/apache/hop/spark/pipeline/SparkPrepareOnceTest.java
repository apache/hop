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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformPrepareOnce;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.spark.core.PrepareCountingTransform;
import org.apache.hop.spark.core.PrepareCountingTransformMeta;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The once-per-run work of a transform (Table output's truncate, ...) runs once on the driver, and
 * the partitions skip it, instead of every partition doing it.
 */
class SparkPrepareOnceTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            PrepareCountingTransformMeta.class.getName(),
            TransformPluginType.class,
            Transform.class);
    spark =
        SparkSession.builder()
            .appName("hop-spark-prepare-once-test")
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @BeforeEach
  void reset() {
    PrepareCountingTransform.reset();
  }

  private static PipelineMeta pipeline() {
    InjectorMeta injector = new InjectorMeta();
    injector.getInjectorFields().add(new InjectorField("n", "Integer", "-1", "-1"));
    TransformMeta src = new TransformMeta("src", injector);
    src.setTransformPluginId("Injector");
    TransformMeta counting = new TransformMeta("counting", new PrepareCountingTransformMeta());
    counting.setTransformPluginId(PrepareCountingTransformMeta.ID);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(src);
    pipelineMeta.addTransform(counting);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(src, counting));
    return pipelineMeta;
  }

  private static long runOnFourPartitions(PipelineMeta pipelineMeta, Variables variables)
      throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {DataTypes.createStructField("n", DataTypes.LongType, false)});
    List<Row> rows = new ArrayList<>();
    for (long i = 0; i < 40; i++) {
      rows.add(RowFactory.create(i));
    }
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("n"));
    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkGenericTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            variables,
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            pipelineMeta.findTransform("counting"),
            map,
            spark,
            rowMeta,
            List.of(pipelineMeta.findTransform("src")),
            spark.createDataFrame(rows, schema).repartition(4));
    return map.get("counting").count();
  }

  @Test
  void theDriverPreparesOnceAndThePartitionsSkipIt() throws Exception {
    PipelineMeta pipelineMeta = pipeline();
    Variables variables = new Variables();
    List<String> prepared =
        TransformPrepareOnce.prepareOnDriver(
            pipelineMeta,
            variables,
            new MemoryMetadataProvider(),
            new LoggingObject("spark-prepare-once-test"));
    variables.setVariable(
        Const.INTERNAL_VARIABLE_PIPELINE_PREPARED_ONCE_TRANSFORMS,
        TransformPrepareOnce.preparedOnDriverValue(prepared));

    assertEquals(40, runOnFourPartitions(pipelineMeta, variables));
    assertEquals(1, PrepareCountingTransform.PREPARED.get(), "prepared once, on the driver");
    assertEquals(4, PrepareCountingTransform.FLAGGED.size());
    assertTrue(PrepareCountingTransform.FLAGGED.stream().allMatch(f -> f));
  }

  @Test
  void aPartitionNeverPreparesOnItsOwn() throws Exception {
    // Without the driver's list the transform falls back to its own first-row work, as before;
    // the partitions never run the preparation themselves
    assertEquals(40, runOnFourPartitions(pipeline(), new Variables()));
    assertEquals(0, PrepareCountingTransform.PREPARED.get());
    assertFalse(PrepareCountingTransform.FLAGGED.stream().anyMatch(f -> f));
  }
}

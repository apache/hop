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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.spark.core.BufferingTransformMeta;
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

/**
 * A transform that writes its last batch in batchComplete() (Table output, Update, Insert / update,
 * the Neo4j outputs ...) still delivers those rows on the direct row path and inside a fused chain,
 * where no executor calls batchComplete() after every iteration.
 */
class SparkBatchCompleteTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            BufferingTransformMeta.class.getName(), TransformPluginType.class, Transform.class);
    spark =
        SparkSession.builder()
            .appName("hop-spark-batch-complete-test")
            .master("local[2]")
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

  /** src (Injector, field n) -> first -> second, both buffering until batchComplete(). */
  private static PipelineMeta pipeline() {
    InjectorMeta injector = new InjectorMeta();
    injector.getInjectorFields().add(new InjectorField("n", "Integer", "-1", "-1"));
    TransformMeta src = new TransformMeta("src", injector);
    src.setTransformPluginId("Injector");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(src);
    TransformMeta previous = src;
    for (String name : List.of("first", "second")) {
      TransformMeta buffering = new TransformMeta(name, new BufferingTransformMeta());
      buffering.setTransformPluginId(BufferingTransformMeta.ID);
      pipelineMeta.addTransform(buffering);
      pipelineMeta.addPipelineHop(new PipelineHopMeta(previous, buffering));
      previous = buffering;
    }
    return pipelineMeta;
  }

  private static Dataset<Row> numbers() {
    StructType schema =
        new StructType(
            new StructField[] {DataTypes.createStructField("n", DataTypes.LongType, false)});
    List<Row> rows = new ArrayList<>();
    for (long i = 0; i < 20; i++) {
      rows.add(RowFactory.create(i));
    }
    return spark.createDataFrame(rows, schema).repartition(4);
  }

  private static IRowMeta numbersRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("n"));
    return rowMeta;
  }

  @Test
  void aTransformOnItsOwnDeliversItsLastBatch() throws Exception {
    PipelineMeta pipelineMeta = pipeline();
    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkGenericTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            pipelineMeta.findTransform("first"),
            map,
            spark,
            numbersRowMeta(),
            List.of(pipelineMeta.findTransform("src")),
            numbers());
    assertEquals(20, map.get("first").count());
  }

  @Test
  void everyTransformOfAFusedChainDeliversItsLastBatch() throws Exception {
    PipelineMeta pipelineMeta = pipeline();
    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkGenericTransformHandler()
        .handleFusedTransforms(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            "{}",
            pipelineMeta,
            pipelineMeta.findTransform("first"),
            List.of(pipelineMeta.findTransform("second")),
            map,
            spark,
            numbersRowMeta(),
            numbers());
    assertEquals(20, map.get("second").count());
  }
}

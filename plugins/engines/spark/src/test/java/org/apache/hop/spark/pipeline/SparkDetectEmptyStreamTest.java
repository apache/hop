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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.detectemptystream.DetectEmptyStreamMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.pipeline.handler.SparkGenericTransformHandler;
import org.apache.spark.api.java.JavaSparkContext;
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

/** Detect empty stream looks at the whole stream, not at one partition of it. */
class SparkDetectEmptyStreamTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-detect-empty-stream-test")
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

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {DataTypes.createStructField("n", DataTypes.LongType, true)});

  /** Runs src -> empty? and gives the rows it hands on. */
  private static List<Row> detect(Dataset<Row> input) throws Exception {
    InjectorMeta injector = new InjectorMeta();
    injector.getInjectorFields().add(new InjectorField("n", "Integer", "-1", "-1"));
    TransformMeta src = new TransformMeta("src", injector);
    src.setTransformPluginId("Injector");
    TransformMeta empty = new TransformMeta("empty?", new DetectEmptyStreamMeta());
    empty.setTransformPluginId("DetectEmptyStream");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(src);
    pipelineMeta.addTransform(empty);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(src, empty));
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("n"));

    Map<String, Dataset<Row>> datasets = new HashMap<>();
    new SparkGenericTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            empty,
            datasets,
            spark,
            rowMeta,
            List.of(src),
            input);
    return datasets.get("empty?").collectAsList();
  }

  @Test
  void rowsOnOnePartitionOfFourAreNoEmptyStream() throws Exception {
    // One row, three empty partitions: each of those used to report an empty stream
    Dataset<Row> input =
        spark.createDataFrame(List.of(RowFactory.create(7L)), SCHEMA).repartition(4);
    assertEquals(4, input.toJavaRDD().getNumPartitions());

    assertEquals(0, detect(input).size());
  }

  @Test
  void anEmptyInputWithoutPartitionsGivesOneRow() throws Exception {
    // As a shuffle of nothing under adaptive execution: no partition at all
    Dataset<Row> input =
        spark.createDataFrame(new JavaSparkContext(spark.sparkContext()).<Row>emptyRDD(), SCHEMA);
    assertEquals(0, input.toJavaRDD().getNumPartitions());

    List<Row> rows = detect(input);

    assertEquals(1, rows.size());
    assertTrue(rows.get(0).isNullAt(0));
  }
}

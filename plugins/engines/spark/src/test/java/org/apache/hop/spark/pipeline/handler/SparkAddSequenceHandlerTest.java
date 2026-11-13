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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Counter;
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
import org.apache.hop.pipeline.transforms.addsequence.AddSequenceMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Add sequence over 4 partitions gives the n-th row the value the local engine's counter gives it,
 * so no value repeats per partition, also when the counter wraps at its maximum.
 */
class SparkAddSequenceHandlerTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-add-sequence-test")
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

  @ParameterizedTest
  @CsvSource({
    // start, increment, maximum
    "1, 1, 999999999",
    "100, 5, 999999999",
    "1, 1, 7",
    "0, 3, 10",
    "50, -2, 41",
    "1, 1, 0"
  })
  void everyRowGetsTheLocalEnginesValue(long start, long increment, long maximum) throws Exception {
    AddSequenceMeta meta = new AddSequenceMeta();
    meta.setDefault();
    meta.setValueName("seq");
    meta.setCounterUsed(true);
    meta.setDatabaseUsed(false);
    meta.setStartAt(Long.toString(start));
    meta.setIncrementBy(Long.toString(increment));
    meta.setMaxValue(Long.toString(maximum));
    InjectorMeta injector = new InjectorMeta();
    injector.getInjectorFields().add(new InjectorField("n", "Integer", "-1", "-1"));
    TransformMeta src = new TransformMeta("src", injector);
    src.setTransformPluginId("Injector");
    TransformMeta sequence = new TransformMeta("sequence", meta);
    sequence.setTransformPluginId("Sequence");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(src);
    pipelineMeta.addTransform(sequence);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(src, sequence));

    StructType schema =
        new StructType(
            new StructField[] {DataTypes.createStructField("n", DataTypes.LongType, false)});
    List<Row> rows = new ArrayList<>();
    for (long i = 0; i < 103; i++) {
      rows.add(RowFactory.create(i));
    }
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("n"));
    Map<String, Dataset<Row>> map = new HashMap<>();
    new SparkAddSequenceHandler(new SparkGenericTransformHandler())
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            sequence,
            map,
            spark,
            rowMeta,
            List.of(src),
            spark.createDataFrame(rows, schema).repartition(4));

    // collectAsList keeps the partition order the rows were numbered in
    List<Row> out = map.get("sequence").collectAsList();
    assertEquals(103, out.size());
    Counter counter = new Counter(start, increment, maximum);
    for (Row row : out) {
      assertEquals(counter.getAndNext(), row.getLong(1), "row " + row);
    }
  }
}

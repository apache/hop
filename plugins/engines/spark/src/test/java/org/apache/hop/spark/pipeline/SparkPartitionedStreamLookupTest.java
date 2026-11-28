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
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.HashPartitionField;
import org.apache.hop.pipeline.HashPartitioner;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta.Lookup;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta.MatchKey;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta.ReturnValue;
import org.apache.hop.spark.core.SparkHopPartitioning;
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
 * A partitioned Stream lookup: every partition gets the whole info stream (it is broadcast), so
 * each main row finds its value whichever side is partitioned, as long as the rows of a key stay
 * together on the main side.
 */
class SparkPartitionedStreamLookupTest {

  private static final int KEYS = 100;
  private static final int ROWS = 1000;

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-partitioned-stream-lookup-test")
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static TransformMeta injector(String name, String... fieldsAndTypes) {
    InjectorMeta meta = new InjectorMeta();
    for (int i = 0; i < fieldsAndTypes.length; i += 2) {
      meta.getInjectorFields()
          .add(new InjectorField(fieldsAndTypes[i], fieldsAndTypes[i + 1], "-1", "-1"));
    }
    TransformMeta transformMeta = new TransformMeta(name, meta);
    transformMeta.setTransformPluginId("Injector");
    return transformMeta;
  }

  private static void hashOn(TransformMeta transformMeta, String field) throws Exception {
    TransformPartitioningMeta partitioning =
        new TransformPartitioningMeta(
            HashPartitioner.ID, new PartitionSchema("three", List.of("P1", "P2", "P3")));
    ((HashPartitioner) partitioning.getPartitioner())
        .getFields()
        .add(new HashPartitionField(field));
    transformMeta.setTransformPartitioningMeta(partitioning);
  }

  /** main, main2 -> lookup value <- lookup (info) */
  private static PipelineMeta pipeline(boolean infoPartitioned, String mainKey) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta main = injector("main", mainKey, "Integer", "n", "Integer");
    TransformMeta main2 = injector("main2", mainKey, "Integer", "n", "Integer");
    TransformMeta info = injector("lookup", "id", "Integer", "value", "String");
    if (infoPartitioned) {
      hashOn(info, "id");
    }

    StreamLookupMeta lookupMeta = new StreamLookupMeta();
    lookupMeta.setSourceTransformName("lookup");
    Lookup lookup = new Lookup();
    MatchKey key = new MatchKey();
    key.setKeyStream(mainKey);
    key.setKeyLookup("id");
    lookup.getMatchKeys().add(key);
    ReturnValue value = new ReturnValue();
    value.setValue("value");
    value.setValueName("value");
    value.setValueDefaultType(IValueMeta.TYPE_STRING);
    lookup.getReturnValues().add(value);
    lookupMeta.setLookup(lookup);
    TransformMeta lookupValue = new TransformMeta("lookup value", lookupMeta);
    lookupValue.setTransformPluginId("StreamLookup");
    hashOn(lookupValue, mainKey);

    for (TransformMeta t : List.of(main, main2, info, lookupValue)) {
      pipelineMeta.addTransform(t);
    }
    pipelineMeta.addPipelineHop(new PipelineHopMeta(main, lookupValue));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(main2, lookupValue));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(info, lookupValue));
    lookupMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
    return pipelineMeta;
  }

  /** ids 0..99 over 1000 rows, n = the row number */
  private static Dataset<Row> mainRows(String mainKey, int from, int to) {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(mainKey, DataTypes.LongType, false),
              DataTypes.createStructField("n", DataTypes.LongType, false)
            });
    List<Row> rows = new ArrayList<>();
    for (long n = from; n < to; n++) {
      rows.add(RowFactory.create(n % KEYS, n));
    }
    return spark.createDataFrame(rows, schema).repartition(3);
  }

  private static Dataset<Row> lookupRows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("value", DataTypes.StringType, true)
            });
    List<Row> rows = new ArrayList<>();
    for (long id = 0; id < KEYS; id++) {
      rows.add(RowFactory.create(id, "v" + id));
    }
    return spark.createDataFrame(rows, schema).repartition(2);
  }

  /** Runs the lookup the way the converter does: inputs unioned, then shuffled on id. */
  private static Dataset<Row> runLookup(boolean infoPartitioned, String mainKey) throws Exception {
    PipelineMeta pipelineMeta = pipeline(infoPartitioned, mainKey);
    TransformMeta lookupValue = pipelineMeta.findTransform("lookup value");
    TransformMeta info = pipelineMeta.findTransform("lookup");

    Map<String, Dataset<Row>> datasets = new HashMap<>();
    datasets.put(
        "lookup",
        infoPartitioned ? SparkHopPartitioning.repartition(lookupRows(), info) : lookupRows());
    // Two main inputs: the second half of the rows comes in over the second hop
    Dataset<Row> input =
        SparkHopPartitioning.repartition(
            mainRows(mainKey, 0, ROWS / 2).union(mainRows(mainKey, ROWS / 2, ROWS)), lookupValue);
    IRowMeta rowMeta =
        pipelineMeta.getTransformFields(
            new Variables(), pipelineMeta.findTransform("main"), lookupValue, null);

    new SparkGenericTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            lookupValue,
            datasets,
            spark,
            rowMeta,
            List.of(pipelineMeta.findTransform("main"), pipelineMeta.findTransform("main2")),
            input);
    return datasets.get("lookup value");
  }

  private static void assertEveryRowFoundItsValue(Dataset<Row> output) {
    List<Row> rows = output.collectAsList();
    assertEquals(ROWS, rows.size());
    for (Row row : rows) {
      assertEquals("v" + row.getLong(0), row.getString(2), "row n=" + row.getLong(1));
    }
  }

  @Test
  void bothSidesPartitionedOnTheKey() throws Exception {
    Dataset<Row> output = runLookup(true, "id");

    assertEquals(4, output.toJavaRDD().getNumPartitions());
    assertEveryRowFoundItsValue(output);
    // Every partition has rows: the lookup really ran on several partitions
    for (List<Row> partition : output.toJavaRDD().glom().collect()) {
      assertTrue(!partition.isEmpty());
    }
  }

  /** Refused on the local engine (the info source must be partitioned too); fine on Spark. */
  @Test
  void onlyTheLookupPartitioned() throws Exception {
    assertEveryRowFoundItsValue(runLookup(false, "id"));
  }

  /**
   * The keys need not have the same name: the lookup is partitioned on its main field customer_id,
   * the info source on its own field id.
   */
  @Test
  void keysWithDifferentNames() throws Exception {
    Dataset<Row> output = runLookup(true, "customer_id");

    assertEquals(List.of("customer_id", "n", "value"), List.of(output.columns()));
    assertEveryRowFoundItsValue(output);
  }
}

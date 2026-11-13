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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.HashPartitionField;
import org.apache.hop.pipeline.HashPartitioner;
import org.apache.hop.pipeline.ModPartitioner;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RebalancePartitioner;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.pipeline.handler.SparkGenericTransformHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A partitioned transform gets the rows of its Hop partition on its Spark partition. */
class SparkHopPartitioningTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-partitioning-test")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.shuffle.partitions", "5")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static TransformMeta partitioned(String name, String method, PartitionSchema schema)
      throws Exception {
    TransformMeta transformMeta = new TransformMeta(name, new DummyMeta());
    transformMeta.setTransformPluginId("Dummy");
    TransformPartitioningMeta partitioning = new TransformPartitioningMeta(method, schema);
    if (partitioning.getPartitioner() instanceof ModPartitioner mod) {
      mod.setFieldName("id");
    }
    transformMeta.setTransformPartitioningMeta(partitioning);
    return transformMeta;
  }

  private static PartitionSchema static3() {
    return new PartitionSchema("static3", List.of("P1", "P2", "P3"));
  }

  private static Dataset<Row> keyed() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("day", DataTypes.LongType, false)
            });
    List<Row> rows = new ArrayList<>();
    for (long i = 0; i < 200; i++) {
      rows.add(RowFactory.create(i % 20, i % 3));
    }
    return spark.createDataFrame(rows, schema).repartition(2);
  }

  /** Every key on exactly one Spark partition. */
  private static void assertEachKeyOnOnePartition(Dataset<Row> placed, int column) {
    Map<Object, Integer> partitionOfKey = new HashMap<>();
    List<List<Row>> partitions = placed.toJavaRDD().glom().collect();
    for (int p = 0; p < partitions.size(); p++) {
      for (Row row : partitions.get(p)) {
        Integer earlier = partitionOfKey.putIfAbsent(row.get(column), p);
        if (earlier != null) {
          assertEquals(earlier, p, "key " + row.get(column));
        }
      }
    }
  }

  @Test
  void theRowsOfAKeyAreOnOnePartitionAndSparkPicksTheCount() throws Exception {
    TransformMeta transformMeta = partitioned("partitioned", "ModPartitioner", static3());

    Dataset<Row> placed = SparkHopPartitioning.repartition(keyed(), transformMeta);

    // Not the 3 of the schema: Spark's shuffle partitions
    assertEquals(5, placed.toJavaRDD().getNumPartitions());
    assertEquals(200L, placed.count());
    assertEachKeyOnOnePartition(placed, 0);
  }

  @Test
  void hashOnFieldsKeepsEachCombinationTogether() throws Exception {
    TransformMeta transformMeta = partitioned("partitioned", HashPartitioner.ID, static3());
    HashPartitioner hash =
        (HashPartitioner) transformMeta.getTransformPartitioningMeta().getPartitioner();
    hash.getFields().add(new HashPartitionField("id"));
    hash.getFields().add(new HashPartitionField("day"));

    Dataset<Row> placed =
        SparkHopPartitioning.repartition(
            keyed()
                .withColumn(
                    "key", functions.concat_ws("/", functions.col("id"), functions.col("day"))),
            transformMeta);

    assertEquals("hash[id, day]", SparkHopPartitioning.placement(transformMeta));
    assertEachKeyOnOnePartition(placed, 2);
  }

  @Test
  void rebalanceSpreadsTheRows() throws Exception {
    TransformMeta transformMeta = partitioned("partitioned", RebalancePartitioner.ID, static3());
    // All rows on one partition to start with
    Dataset<Row> lopsided = keyed().coalesce(1);

    Dataset<Row> placed = SparkHopPartitioning.repartition(lopsided, transformMeta);

    List<List<Row>> partitions = placed.toJavaRDD().glom().collect();
    assertTrue(partitions.size() > 1, "partitions: " + partitions.size());
    assertEquals(200L, placed.count());
  }

  @Test
  void rebalanceSpreadsTheRowsWithAdaptiveExecution() throws Exception {
    TransformMeta transformMeta = partitioned("partitioned", RebalancePartitioner.ID, static3());
    spark.conf().set("spark.sql.adaptive.enabled", "true");
    spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "false");
    try {
      Dataset<Row> placed = SparkHopPartitioning.repartition(keyed().coalesce(1), transformMeta);

      List<List<Row>> partitions = placed.toJavaRDD().glom().collect();
      assertTrue(partitions.size() > 1, "partitions: " + partitions.size());
      assertEquals(200, partitions.stream().mapToInt(List::size).sum());
    } finally {
      spark.conf().set("spark.sql.adaptive.enabled", "false");
      spark.conf().unset("spark.sql.adaptive.coalescePartitions.enabled");
    }
  }

  @Test
  void aFieldThatIsNotInTheInputIsAnError() throws Exception {
    TransformMeta transformMeta = partitioned("partitioned", "ModPartitioner", static3());
    ((ModPartitioner) transformMeta.getTransformPartitioningMeta().getPartitioner())
        .setFieldName("nope");

    assertThrows(
        HopException.class, () -> SparkHopPartitioning.repartition(keyed(), transformMeta));
  }

  @Test
  void mirrorPartitioningIsRefused() throws Exception {
    TransformMeta transformMeta = partitioned("mirrored", "Mirror", static3());

    assertThrows(HopException.class, () -> SparkHopPartitioning.placement(transformMeta));
  }

  @Test
  void aPartitionedMiniPipelineGetsItsSparkPartitionAsPartition() {
    IVariables variables = new Variables();
    variables.setVariable(SparkHopPartitioning.VAR_PARTITIONED, "Y");

    SparkHopPartitioning.applyPartitionVariables(variables, 7);

    assertEquals("7", variables.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_PARTITION_ID));
    assertEquals("007", variables.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_PARTITION_NR));
  }

  @Test
  void aTransformThatIsNotPartitionedGetsNoPartitionVariables() {
    IVariables variables = new Variables();

    SparkHopPartitioning.applyPartitionVariables(variables, 1);

    assertNull(variables.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_PARTITION_ID));
  }

  @Test
  void aPartitionedTransformIsNotFused() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta source = new TransformMeta("source", new DummyMeta());
    source.setTransformPluginId("Dummy");
    TransformMeta transformMeta = partitioned("partitioned", "ModPartitioner", static3());
    transformMeta.setTransformPluginId("FilterRows");
    pipelineMeta.addTransform(source);
    pipelineMeta.addTransform(transformMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(source, transformMeta));

    assertFalse(
        SparkGenericTransformHandler.canFuse(
            pipelineMeta, transformMeta, new SparkPipelineRunConfiguration()));
  }
}

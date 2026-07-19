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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Multi-previous Dataset union (same layout) for native Spark. */
class MultiInputUnionTest {

  private static SparkSession spark;

  @BeforeAll
  static void start() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-multi-input-test")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stop() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void unionSameLayoutMergesRows() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, true, Metadata.empty()),
              new StructField("name", DataTypes.StringType, true, Metadata.empty())
            });
    Dataset<Row> a =
        spark.createDataFrame(
            List.of(RowFactory.create(1L, "a"), RowFactory.create(2L, "b")), schema);
    Dataset<Row> b = spark.createDataFrame(List.of(RowFactory.create(3L, "c")), schema);

    PipelineMeta pm = new PipelineMeta();
    TransformMeta left = dummy("left");
    TransformMeta right = dummy("right");
    TransformMeta sink = dummy("sink");
    pm.addTransform(left);
    pm.addTransform(right);
    pm.addTransform(sink);
    pm.addPipelineHop(new PipelineHopMeta(left, sink));
    pm.addPipelineHop(new PipelineHopMeta(right, sink));

    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("left", a);
    map.put("right", b);

    // Dummy has no getFields change; attach fields via a spy of getTransformFields is hard —
    // use real metas and override by putting identical schemas on both Datasets only.
    // getTransformFields on Dummy returns previous fields; with no previous on left/right empty.
    // So inject via SelectValues-less approach: call resolve with row meta from Datasets
    // indirectly.
    // We need pipelineMeta.getTransformFields to return matching metas — Dummy with no previous
    // returns empty. Build Constant-less chain: use RowMeta-empty Dummy won't work for check.
    // Instead unit-test union path by temporarily using empty layout exclude: Dummy does not
    // exclude. Workaround: use pipeline where left/right have injector-like fields via Dummy
    // only when we don't check layout (excludeFromRowLayoutVerification) — Dummy returns false.
    //
    // Practical approach: construct IRowMeta via getTransformFields after adding fields through
    // a thin transform is heavy. Validate lookup + union with exclude via StreamLookup exclude?
    // StreamLookup excludeFromRowLayoutVerification is true.
    // Simplest: only assert layout mismatch throws with real fields from Constant.

    // For merge without layout from Dummy: skip safeMode when both empty.
    HopPipelineMetaToSparkConverter.ResolvedInputs resolved =
        HopPipelineMetaToSparkConverter.resolveAndUnionInputs(
            LogChannel.GENERAL, new Variables(), pm, sink, List.of(left, right), map);

    assertEquals(3, resolved.dataset().count());
  }

  @Test
  void layoutMismatchThrows() throws Exception {
    StructType schemaA =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, true, Metadata.empty()),
              new StructField("name", DataTypes.StringType, true, Metadata.empty())
            });
    StructType schemaB =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, true, Metadata.empty()),
              new StructField("other", DataTypes.StringType, true, Metadata.empty())
            });
    Dataset<Row> a = spark.createDataFrame(List.of(RowFactory.create(1L, "a")), schemaA);
    Dataset<Row> b = spark.createDataFrame(List.of(RowFactory.create(2L, "x")), schemaB);

    // Build pipeline with Constant-like field definitions using Dummy won't set fields.
    // Use a custom approach: TransformMeta with metas that implement getFields.
    PipelineMeta pm = pipelineWithTwoStringFields("left", "right", "sink", "name", "other");
    Map<String, Dataset<Row>> map = new HashMap<>();
    map.put("left", a);
    map.put("right", b);

    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                HopPipelineMetaToSparkConverter.resolveAndUnionInputs(
                    LogChannel.GENERAL,
                    new Variables(),
                    pm,
                    pm.findTransform("sink"),
                    List.of(pm.findTransform("left"), pm.findTransform("right")),
                    map));
    assertTrue(
        ex.getMessage().contains("same row layout") || ex.getMessage().contains("Mixing"),
        () -> "unexpected: " + ex.getMessage());
  }

  @Test
  void lookupPrefersTargetStreamKey() {
    Map<String, Dataset<Row>> map = new HashMap<>();
    StructType schema =
        new StructType(
            new StructField[] {new StructField("id", DataTypes.LongType, true, Metadata.empty())});
    Dataset<Row> main = spark.createDataFrame(List.of(RowFactory.create(1L)), schema);
    Dataset<Row> target = spark.createDataFrame(List.of(RowFactory.create(9L)), schema);
    map.put("Filter", main);
    map.put("Filter - TARGET - Next", target);

    TransformMeta filter = dummy("Filter");
    TransformMeta next = dummy("Next");
    Dataset<Row> found =
        HopPipelineMetaToSparkConverter.lookupPreviousDataset(
            map, filter, next, LogChannel.GENERAL);
    assertEquals(9L, found.collectAsList().get(0).getLong(0));
  }

  private static TransformMeta dummy(String name) {
    TransformMeta tm = new TransformMeta(name, new DummyMeta());
    tm.setTransformPluginId("Dummy");
    return tm;
  }

  /**
   * Two source Dummy transforms are not enough for getFields; attach SelectValues-like row meta by
   * using a small anonymous ITransformMeta is heavy. Instead wire Constant fields via Dummy and
   * manually call getFields is wrong. Use pipeline with Calculator? Easiest: RowMeta for
   * verification via a custom TransformMeta that only implements getFields.
   *
   * <p>Here we use Dummy and pre-seed nothing — for layoutMismatch we need getTransformFields to
   * return different layouts. Add Injector metas as previous with fixed fields.
   */
  private static PipelineMeta pipelineWithTwoStringFields(
      String left, String right, String sink, String leftExtra, String rightExtra)
      throws Exception {
    PipelineMeta pm = new PipelineMeta();
    // Sources with different second field names via Injector
    org.apache.hop.pipeline.transforms.injector.InjectorMeta injL =
        new org.apache.hop.pipeline.transforms.injector.InjectorMeta();
    injL.getInjectorFields()
        .add(
            new org.apache.hop.pipeline.transforms.injector.InjectorField(
                "id", "Integer", "9", "0"));
    injL.getInjectorFields()
        .add(
            new org.apache.hop.pipeline.transforms.injector.InjectorField(
                leftExtra, "String", "50", "-1"));
    org.apache.hop.pipeline.transforms.injector.InjectorMeta injR =
        new org.apache.hop.pipeline.transforms.injector.InjectorMeta();
    injR.getInjectorFields()
        .add(
            new org.apache.hop.pipeline.transforms.injector.InjectorField(
                "id", "Integer", "9", "0"));
    injR.getInjectorFields()
        .add(
            new org.apache.hop.pipeline.transforms.injector.InjectorField(
                rightExtra, "String", "50", "-1"));

    TransformMeta l = new TransformMeta(left, injL);
    l.setTransformPluginId("Injector");
    TransformMeta r = new TransformMeta(right, injR);
    r.setTransformPluginId("Injector");
    TransformMeta s = dummy(sink);
    pm.addTransform(l);
    pm.addTransform(r);
    pm.addTransform(s);
    pm.addPipelineHop(new PipelineHopMeta(l, s));
    pm.addPipelineHop(new PipelineHopMeta(r, s));
    return pm;
  }
}

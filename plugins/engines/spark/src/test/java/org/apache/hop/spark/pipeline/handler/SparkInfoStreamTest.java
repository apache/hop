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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta.Lookup;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta.MatchKey;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta.ReturnValue;
import org.apache.hop.spark.core.SparkInfoStreamSupport;
import org.apache.spark.broadcast.Broadcast;
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

/**
 * Unit tests for info/side-stream discovery and broadcast helper. End-to-end Stream Lookup is
 * covered by integration-tests/spark-native.
 */
class SparkInfoStreamTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-info-stream-test")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void resolveInfoTransformsExcludesMainPrevious() {
    PipelineMeta pm = new PipelineMeta();
    TransformMeta mainSrc = new TransformMeta("mainSrc", new DummyMeta());
    mainSrc.setTransformPluginId("Dummy");
    TransformMeta infoSrc = new TransformMeta("infoSrc", new DummyMeta());
    infoSrc.setTransformPluginId("Dummy");
    StreamLookupMeta slMeta = new StreamLookupMeta();
    slMeta.setSourceTransformName("infoSrc");
    Lookup lookup = new Lookup();
    MatchKey mk = new MatchKey();
    mk.setKeyStream("k");
    mk.setKeyLookup("k");
    lookup.getMatchKeys().add(mk);
    ReturnValue rv = new ReturnValue();
    rv.setValue("v");
    rv.setValueName("v");
    lookup.getReturnValues().add(rv);
    slMeta.setLookup(lookup);
    TransformMeta lookupT = new TransformMeta("lookup", slMeta);
    lookupT.setTransformPluginId("StreamLookup");
    pm.addTransform(mainSrc);
    pm.addTransform(infoSrc);
    pm.addTransform(lookupT);
    pm.addPipelineHop(new PipelineHopMeta(mainSrc, lookupT));
    pm.addPipelineHop(new PipelineHopMeta(infoSrc, lookupT));
    slMeta.searchInfoAndTargetTransforms(pm.getTransforms());

    List<TransformMeta> mainPrev = pm.findPreviousTransforms(lookupT, false);
    List<TransformMeta> info =
        SparkGenericTransformHandler.resolveInfoTransforms(pm, lookupT, mainPrev);

    assertEquals(1, mainPrev.size());
    assertEquals("mainSrc", mainPrev.get(0).getName());
    assertEquals(1, info.size());
    assertEquals("infoSrc", info.get(0).getName());
  }

  @Test
  void broadcastInfoRowsCollectsHopRows() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("stateCode", DataTypes.StringType, true, Metadata.empty()),
              new StructField("countPerState", DataTypes.LongType, true, Metadata.empty())
            });
    Dataset<Row> infoDs =
        spark.createDataFrame(
            List.of(RowFactory.create("AK", 10L), RowFactory.create("GA", 20L)), schema);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("stateCode"));
    rowMeta.addValueMeta(new ValueMetaInteger("countPerState"));

    Broadcast<List<Object[]>> broadcast =
        SparkInfoStreamSupport.broadcastInfoRows(spark, infoDs, rowMeta, "infoSrc");
    List<Object[]> rows = broadcast.value();
    assertEquals(2, rows.size());
    assertEquals("AK", rows.get(0)[0]);
    assertTrue(((Number) rows.get(0)[1]).longValue() == 10L);
  }
}

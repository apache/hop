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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.pipeline.transforms.uniquerowsbyhashset.UniqueRowsByHashSetMeta;
import org.apache.hop.pipeline.transforms.uniquerowsbyhashset.UniqueRowsByHashSetMeta.CompareField;
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
import org.junit.jupiter.api.Test;

/** Unique Rows By Hashset over several partitions gives the local engine's rows. */
class SparkUniqueRowsByHashSetHandlerTest {

  private static final int KEYS = 50;
  private static final int ROWS = 400;

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-unique-rows-by-hash-set-test")
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
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

  /** src -> unique -> out, and unique -(error hop)-> duplicates when rejecting. */
  private static PipelineMeta pipeline(boolean reject) {
    InjectorMeta injector = new InjectorMeta();
    injector.getInjectorFields().add(new InjectorField("key", "String", "-1", "-1"));
    injector.getInjectorFields().add(new InjectorField("n", "Integer", "-1", "-1"));
    TransformMeta src = new TransformMeta("src", injector);
    src.setTransformPluginId("Injector");

    UniqueRowsByHashSetMeta meta = new UniqueRowsByHashSetMeta();
    meta.setCompareFields(new ArrayList<>(List.of(new CompareField("key"))));
    meta.setStoreValues(true);
    meta.setRejectDuplicateRow(reject);
    meta.setErrorDescription("duplicate");
    TransformMeta unique = new TransformMeta("unique", meta);
    unique.setTransformPluginId("UniqueRowsByHashSet");

    TransformMeta out = new TransformMeta("out", new DummyMeta());
    out.setTransformPluginId("Dummy");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(src);
    pipelineMeta.addTransform(unique);
    pipelineMeta.addTransform(out);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(src, unique));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(unique, out));
    if (reject) {
      TransformMeta duplicates = new TransformMeta("duplicates", new DummyMeta());
      duplicates.setTransformPluginId("Dummy");
      pipelineMeta.addTransform(duplicates);
      pipelineMeta.addPipelineHop(new PipelineHopMeta(unique, duplicates));
      TransformErrorMeta errorMeta =
          new TransformErrorMeta(unique, duplicates, "nr_errors", "error_description", null, null);
      errorMeta.setEnabled(true);
      unique.setTransformErrorMeta(errorMeta);
    }
    return pipelineMeta;
  }

  /** key k0..k49 repeating, n = the row number; in that order over 4 partitions. */
  private static Dataset<Row> rows() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("key", DataTypes.StringType, false),
              DataTypes.createStructField("n", DataTypes.LongType, false)
            });
    List<Row> rows = new ArrayList<>();
    for (long n = 0; n < ROWS; n++) {
      rows.add(RowFactory.create("k" + (n % KEYS), n));
    }
    Dataset<Row> dataset = spark.createDataFrame(rows, schema);
    assertTrue(dataset.toJavaRDD().getNumPartitions() > 1);
    return dataset;
  }

  private static Map<String, Dataset<Row>> run(boolean reject) throws Exception {
    PipelineMeta pipelineMeta = pipeline(reject);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("key"));
    rowMeta.addValueMeta(new ValueMetaInteger("n"));
    Map<String, Dataset<Row>> datasets = new HashMap<>();
    new SparkUniqueRowsByHashSetHandler(new SparkGenericTransformHandler())
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            new MemoryMetadataProvider(),
            "{}",
            pipelineMeta,
            pipelineMeta.findTransform("unique"),
            datasets,
            spark,
            rowMeta,
            List.of(pipelineMeta.findTransform("src")),
            rows());
    return datasets;
  }

  @Test
  void everyKeyOnceTheFirstOfItsRows() throws Exception {
    List<Row> unique = run(false).get("unique").orderBy("n").collectAsList();

    assertEquals(KEYS, unique.size());
    for (int i = 0; i < KEYS; i++) {
      // The first row of key k<i> is row i, as on the local engine
      assertEquals("k" + i, unique.get(i).getString(0));
      assertEquals(i, unique.get(i).getLong(1));
    }
  }
}

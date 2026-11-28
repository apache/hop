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

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.detectlastrow.DetectLastRowMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.rowgenerator.GeneratorField;
import org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A run releases every Dataset it keeps, also those its transform handlers keep: Hop GUI and Hop
 * Server reuse the Spark session, which would otherwise hold a copy of the rows of every run.
 */
class SparkPersistedDatasetsTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-persisted-datasets-test")
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

  @Test
  void identifyLastRowReleasesTheRowsItKeeps() throws Exception {
    DetectLastRowMeta lastRow = new DetectLastRowMeta();
    lastRow.setResultFieldName("last");
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta generator = generator();
    TransformMeta flag = transform("flag", lastRow, "DetectLastRow");
    TransformMeta out = transform("out", new DummyMeta(), "Dummy");
    chain(pipelineMeta, generator, flag, out);

    assertReleasesWhatItKeeps(pipelineMeta, new SparkPipelineRunConfiguration());
  }

  @Test
  void aSourceSpreadOverPartitionsReleasesTheRowsItKeeps() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    chain(pipelineMeta, generator(), transform("out", new DummyMeta(), "Dummy"));
    SparkPipelineRunConfiguration runConfiguration = new SparkPipelineRunConfiguration();
    runConfiguration.setSourcePartitions("4");

    assertReleasesWhatItKeeps(pipelineMeta, runConfiguration);
  }

  @Test
  void aTransformReadByTwoTransformsReleasesTheRowsItKeeps() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta generator = generator();
    // Copies its rows to both hops
    generator.setDistributes(false);
    TransformMeta first = transform("first", new DummyMeta(), "Dummy");
    TransformMeta second = transform("second", new DummyMeta(), "Dummy");
    chain(pipelineMeta, generator, first);
    pipelineMeta.addTransform(second);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(generator, second));

    assertReleasesWhatItKeeps(pipelineMeta, new SparkPipelineRunConfiguration());
  }

  /** Runs the pipeline as the engine does: an action on every leaf, then the release. */
  private static void assertReleasesWhatItKeeps(
      PipelineMeta pipelineMeta, SparkPipelineRunConfiguration runConfiguration) throws Exception {
    int before = spark.sharedState().cacheManager().numCachedEntries();
    HopPipelineMetaToSparkConverter converter =
        new HopPipelineMetaToSparkConverter(
            new Variables(), pipelineMeta, new MemoryMetadataProvider(), "spark", runConfiguration);
    converter.createDataset(spark);
    long rows = 0;
    for (Dataset<Row> leaf : converter.getLeafDatasets().values()) {
      rows += leaf.count();
    }
    assertTrue(rows > 0, "the pipeline gave no rows");
    assertTrue(
        spark.sharedState().cacheManager().numCachedEntries() > before,
        "the run kept no rows: the test doesn't test the release");

    converter.unpersistAll();

    assertEquals(before, spark.sharedState().cacheManager().numCachedEntries());
  }

  /** Ten rows with one Integer field. */
  private static TransformMeta generator() {
    RowGeneratorMeta meta = new RowGeneratorMeta();
    meta.setRowLimit("10");
    meta.getFields()
        .add(new GeneratorField("n", "Integer", null, -1, -1, null, null, null, "1", false));
    return transform("generator", meta, "RowGenerator");
  }

  private static TransformMeta transform(String name, ITransformMeta meta, String pluginId) {
    TransformMeta transformMeta = new TransformMeta(name, meta);
    transformMeta.setTransformPluginId(pluginId);
    return transformMeta;
  }

  private static void chain(PipelineMeta pipelineMeta, TransformMeta... transforms) {
    for (TransformMeta transform : transforms) {
      pipelineMeta.addTransform(transform);
    }
    for (int i = 1; i < transforms.length; i++) {
      pipelineMeta.addPipelineHop(new PipelineHopMeta(transforms[i - 1], transforms[i]));
    }
  }
}

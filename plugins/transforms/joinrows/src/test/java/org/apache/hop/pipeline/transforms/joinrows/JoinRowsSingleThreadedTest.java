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

package org.apache.hop.pipeline.transforms.joinrows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Runs Join Rows through the {@link SingleThreadedPipelineExecutor}: the executor behind the single
 * threaded pipeline engine and single threaded sub-pipelines (#2353).
 *
 * <p>The executor calls processRow() once per row waiting on the input row sets, and for as long as
 * the main stream (an info stream of Join Rows) holds rows. The rows on the input row sets are the
 * complete input, but the row sets are not flagged as done, so any read beyond them would wait
 * forever.
 */
class JoinRowsSingleThreadedTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (ITransformMeta meta : List.of(new JoinRowsMeta(), new InjectorMeta(), new DummyMeta())) {
      if (registry.getPluginId(TransformPluginType.class, meta) == null) {
        registry.registerPluginClass(
            meta.getClass().getName(), TransformPluginType.class, Transform.class);
      }
    }
  }

  @Test
  void cartesianProductOfTwoInputs() throws Exception {
    List<RowMetaAndData> rows = assertTimeoutPreemptively(TIMEOUT, () -> runJoin(3, 2, 500, false));

    assertEquals(6, rows.size());
    assertProduct(rows, 3, 2);
  }

  @Test
  void cartesianProductWhenInputRowSetsAreFinished() throws Exception {
    List<RowMetaAndData> rows = assertTimeoutPreemptively(TIMEOUT, () -> runJoin(3, 2, 500, true));

    assertEquals(6, rows.size());
    assertProduct(rows, 3, 2);
  }

  @Test
  void cartesianProductSpillingToTemporaryFile() throws Exception {
    // A cache size below the number of rows makes the transform read the rows back from disk.
    List<RowMetaAndData> rows = assertTimeoutPreemptively(TIMEOUT, () -> runJoin(4, 5, 2, false));

    assertEquals(20, rows.size());
    assertProduct(rows, 4, 5);
  }

  @Test
  void noOutputWhenOneInputIsEmpty() throws Exception {
    List<RowMetaAndData> rows = assertTimeoutPreemptively(TIMEOUT, () -> runJoin(3, 0, 500, false));

    assertTrue(rows.isEmpty());
  }

  @Test
  void singleInputPassesRowsThrough() throws Exception {
    // This is how the Beam engine used to feed the transform: all inputs flattened into one row
    // set, one row per iteration. The old batchComplete() hung on the row that was left behind.
    List<RowMetaAndData> rows =
        assertTimeoutPreemptively(TIMEOUT, () -> runJoin(3, -1, 500, false));

    assertEquals(3, rows.size());
  }

  private static void assertProduct(List<RowMetaAndData> rows, int nrMain, int nrOther)
      throws Exception {
    List<String> expected = new ArrayList<>();
    for (long id = 1; id <= nrMain; id++) {
      for (int n = 1; n <= nrOther; n++) {
        expected.add(id + "-name" + n);
      }
    }
    List<String> actual = new ArrayList<>();
    for (RowMetaAndData row : rows) {
      assertEquals(2, row.getRowMeta().size());
      actual.add(row.getInteger("id") + "-" + row.getString("name", null));
    }
    assertEquals(expected, actual);
  }

  private static List<RowMetaAndData> runJoin(
      int nrMain, int nrOther, int cacheSize, boolean finishInputs) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("join-rows-single-threaded");

    TransformMeta main = addTransform(pipelineMeta, "main", new InjectorMeta());
    // A negative number of rows leaves the second input out of the pipeline
    TransformMeta other =
        nrOther < 0 ? null : addTransform(pipelineMeta, "other", new InjectorMeta());

    JoinRowsMeta joinRowsMeta = new JoinRowsMeta();
    joinRowsMeta.setDefault();
    joinRowsMeta.setCacheSize(cacheSize);
    joinRowsMeta.setMainTransformName("main");
    joinRowsMeta.setDirectory(System.getProperty("java.io.tmpdir"));
    TransformMeta join = addTransform(pipelineMeta, "join", joinRowsMeta);

    TransformMeta output = addTransform(pipelineMeta, "output", new DummyMeta());

    pipelineMeta.addPipelineHop(new PipelineHopMeta(main, join));
    if (other != null) {
      pipelineMeta.addPipelineHop(new PipelineHopMeta(other, join));
    }
    pipelineMeta.addPipelineHop(new PipelineHopMeta(join, output));

    // Like loading the pipeline from a file: the main transform is an info stream of Join Rows,
    // which the executor feeds with processRow() calls for as long as it holds rows.
    joinRowsMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    pipeline.prepareExecution();

    RowProducer mainProducer = pipeline.addRowProducer("main", 0);
    RowProducer otherProducer = other == null ? null : pipeline.addRowProducer("other", 0);

    ITransform joinTransform = pipeline.getTransform("join", 0);
    TransformRowsCollector collector = new TransformRowsCollector();
    joinTransform.addRowListener(collector);

    pipeline.startThreads();

    IRowMeta mainRowMeta = new RowMeta();
    mainRowMeta.addValueMeta(new ValueMetaInteger("id"));
    for (long id = 1; id <= nrMain; id++) {
      mainProducer.putRow(mainRowMeta, new Object[] {id});
    }
    IRowMeta otherRowMeta = new RowMeta();
    otherRowMeta.addValueMeta(new ValueMetaString("name"));
    for (int n = 1; n <= nrOther; n++) {
      otherProducer.putRow(otherRowMeta, new Object[] {"name" + n});
    }
    if (finishInputs) {
      mainProducer.finished();
      if (otherProducer != null) {
        otherProducer.finished();
      }
    }

    SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor(pipeline);
    assertTrue(executor.init());
    try {
      executor.oneIteration();
    } finally {
      executor.dispose();
    }
    assertEquals(0, joinTransform.getErrors());

    return collector.getRowsWritten();
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }
}

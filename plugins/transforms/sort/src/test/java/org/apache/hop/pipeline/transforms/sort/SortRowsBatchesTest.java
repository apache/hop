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

package org.apache.hop.pipeline.transforms.sort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Kafka consumer and the Event Hubs listener run their sub-pipeline in batches: rows are put on the
 * input, then one iteration of the single-threaded executor runs. Sort rows marks its output done
 * after every batch, but more batches follow, so the transform after it reads every batch.
 */
class SortRowsBatchesTest {

  private static final int NR_BATCHES = 4;
  private static final int ROWS_PER_BATCH = 3;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (ITransformMeta meta : List.of(new SortRowsMeta(), new InjectorMeta(), new DummyMeta())) {
      if (registry.getPluginId(TransformPluginType.class, meta) == null) {
        registry.registerPluginClass(
            meta.getClass().getName(), TransformPluginType.class, Transform.class);
      }
    }
  }

  @Test
  void everyBatchReachesTheTransformAfterSortRows() {
    List<Integer> rowsAfterEachBatch =
        assertTimeoutPreemptively(Duration.ofSeconds(20), SortRowsBatchesTest::runBatches);

    assertEquals(List.of(3, 6, 9, 12), rowsAfterEachBatch);
  }

  private static List<Integer> runBatches() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("sort-rows-batches");

    SortRowsMeta sortRowsMeta = new SortRowsMeta();
    sortRowsMeta.setDefault();
    sortRowsMeta.getSortFields().add(new SortRowsField("nr", true, true, false, 0, false));
    sortRowsMeta.setDirectory(System.getProperty("java.io.tmpdir"));

    TransformMeta input = addTransform(pipelineMeta, "input", new InjectorMeta());
    TransformMeta sort = addTransform(pipelineMeta, "sort", sortRowsMeta);
    TransformMeta output = addTransform(pipelineMeta, "output", new DummyMeta());
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, sort));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(sort, output));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    pipeline.prepareExecution();
    RowProducer producer = pipeline.addRowProducer("input", 0);

    List<Object[]> rows = new ArrayList<>();
    pipeline
        .getTransform("output", 0)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
                rows.add(row);
              }
            });
    pipeline.startThreads();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("nr"));

    SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor(pipeline);
    assertTrue(executor.init());
    List<Integer> rowsAfterEachBatch = new ArrayList<>();
    try {
      long nr = 0;
      for (int batch = 0; batch < NR_BATCHES; batch++) {
        for (int r = 0; r < ROWS_PER_BATCH; r++) {
          producer.putRow(rowMeta, new Object[] {nr++});
        }
        executor.oneIteration();
        rowsAfterEachBatch.add(rows.size());
      }
    } finally {
      executor.dispose();
    }
    return rowsAfterEachBatch;
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }
}

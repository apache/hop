/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.rowstoresult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
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
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Copy rows to result in a real pipeline: Injector, then Copy rows to result, then Dummy. */
class RowsToResultTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final String INPUT = "input";
  private static final String TO_RESULT = "to result";
  private static final String OUTPUT = "output";

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (ITransformMeta meta :
        List.of(new RowsToResultMeta(), new InjectorMeta(), new DummyMeta())) {
      if (registry.getPluginId(TransformPluginType.class, meta) == null) {
        registry.registerPluginClass(
            meta.getClass().getName(), TransformPluginType.class, Transform.class);
      }
    }
  }

  /** A transform after Copy rows to result that changes the row in place, as If null does. */
  @Test
  void laterInPlaceChangeDoesNotReachTheResultRows() {
    List<RowMetaAndData> resultRows =
        assertTimeoutPreemptively(
            TIMEOUT,
            () -> {
              Pipeline pipeline = preparePipeline(1, PipelineMeta.PipelineType.Normal);
              pipeline
                  .getTransform(OUTPUT, 0)
                  .addRowListener(
                      new RowAdapter() {
                        @Override
                        public void rowReadEvent(IRowMeta rowMeta, Object[] row)
                            throws HopTransformException {
                          row[1] = "changed downstream";
                        }
                      });
              return runMultiThreaded(pipeline, 3);
            });

    assertEquals(List.of("value-0", "value-1", "value-2"), names(resultRows));
  }

  @Test
  void resultRowsKeepTheFieldsAndShareOneRowMeta() {
    List<RowMetaAndData> resultRows =
        assertTimeoutPreemptively(
            TIMEOUT,
            () -> runMultiThreaded(preparePipeline(1, PipelineMeta.PipelineType.Normal), 3));

    assertEquals(3, resultRows.size());
    IRowMeta first = resultRows.get(0).getRowMeta();
    assertEquals(List.of("id", "name"), List.of(first.getFieldNames()));
    for (RowMetaAndData row : resultRows) {
      assertSame(first, row.getRowMeta());
    }
  }

  /**
   * Several copies hand over their rows when their input ends, at about the same moment, into one
   * list. Repeated because a lost update depends on thread timing.
   */
  @Test
  void severalCopiesLoseNoRows() {
    int copies = 4;
    int nrRows = 40_000;
    for (int attempt = 0; attempt < 10; attempt++) {
      List<RowMetaAndData> resultRows =
          assertTimeoutPreemptively(
              TIMEOUT,
              () ->
                  runMultiThreaded(
                      preparePipeline(copies, PipelineMeta.PipelineType.Normal), nrRows));

      assertEquals(nrRows, resultRows.size(), "attempt " + attempt);
      boolean[] seen = new boolean[nrRows];
      for (RowMetaAndData row : resultRows) {
        int id = (int) (long) (Long) row.getData()[0];
        assertTrue(!seen[id], "row " + id + " added twice");
        seen[id] = true;
      }
    }
  }

  /**
   * The single threaded executor calls processRow() only for the rows waiting on the input, then
   * batchComplete(). The rows of every batch reach the result rows, once.
   */
  @Test
  void singleThreadedExecutorReturnsEveryRowOnce() {
    List<RowMetaAndData> resultRows =
        assertTimeoutPreemptively(
            TIMEOUT,
            () -> {
              Pipeline pipeline = preparePipeline(1, PipelineMeta.PipelineType.SingleThreaded);
              RowProducer producer = pipeline.addRowProducer(INPUT, 0);
              pipeline.startThreads();

              SingleThreadedPipelineExecutor executor =
                  new SingleThreadedPipelineExecutor(pipeline);
              assertTrue(executor.init());
              try {
                putRows(producer, 0, 3);
                executor.oneIteration();
                putRows(producer, 3, 2);
                executor.oneIteration();
                producer.finished();
                executor.oneIteration();
              } finally {
                executor.dispose();
              }
              assertEquals(0, pipeline.getTransform(TO_RESULT, 0).getErrors());
              return pipeline.getResultRows();
            });

    assertEquals(List.of("value-0", "value-1", "value-2", "value-3", "value-4"), names(resultRows));
  }

  @Test
  void emptyInputGivesNoResultRows() {
    List<RowMetaAndData> resultRows =
        assertTimeoutPreemptively(
            TIMEOUT,
            () -> runMultiThreaded(preparePipeline(1, PipelineMeta.PipelineType.Normal), 0));

    assertTrue(resultRows.isEmpty());
  }

  private static Pipeline preparePipeline(int copies, PipelineMeta.PipelineType type)
      throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("rows-to-result");

    TransformMeta input = addTransform(pipelineMeta, INPUT, new InjectorMeta());
    TransformMeta toResult = addTransform(pipelineMeta, TO_RESULT, new RowsToResultMeta());
    toResult.setCopies(copies);
    TransformMeta output = addTransform(pipelineMeta, OUTPUT, new DummyMeta());
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, toResult));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(toResult, output));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setPipelineType(type);
    pipeline.prepareExecution();
    return pipeline;
  }

  private static List<RowMetaAndData> runMultiThreaded(Pipeline pipeline, int nrRows)
      throws Exception {
    RowProducer producer = pipeline.addRowProducer(INPUT, 0);
    pipeline.startThreads();
    putRows(producer, 0, nrRows);
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors());
    return pipeline.getResultRows();
  }

  private static void putRows(RowProducer producer, int firstId, int nrRows) {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    for (int id = firstId; id < firstId + nrRows; id++) {
      producer.putRow(rowMeta, new Object[] {(long) id, "value-" + id});
    }
  }

  private static List<String> names(List<RowMetaAndData> rows) {
    List<String> names = new ArrayList<>();
    for (RowMetaAndData row : rows) {
      names.add((String) row.getData()[1]);
    }
    return names;
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }
}

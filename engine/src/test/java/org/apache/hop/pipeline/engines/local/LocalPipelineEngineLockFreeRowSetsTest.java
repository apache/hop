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

package org.apache.hop.pipeline.engines.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.LockFreeRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/** The experimental lock-free row sets option of the local run configuration. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class LocalPipelineEngineLockFreeRowSetsTest {

  private static final int ROWS_PER_INPUT = 50_000;

  @BeforeAll
  static void beforeClass() throws HopException {
    HopEnvironment.init();
  }

  /** Two transforms A and C feeding into transform B, so B reads from several inputs. */
  private static PipelineMeta createPipelineMeta() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("lock-free row sets test");
    TransformMeta a = new TransformMeta("A", new DummyMeta());
    TransformMeta c = new TransformMeta("C", new DummyMeta());
    TransformMeta b = new TransformMeta("B", new DummyMeta());
    pipelineMeta.addTransform(a);
    pipelineMeta.addTransform(c);
    pipelineMeta.addTransform(b);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(a, b));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(c, b));
    return pipelineMeta;
  }

  private static LocalPipelineEngine createPipeline(boolean lockFreeRowSets) {
    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(createPipelineMeta(), new Variables(), new LoggingObject("test"));
    LocalPipelineRunConfiguration config =
        (LocalPipelineRunConfiguration)
            pipeline.getPipelineRunConfiguration().getEngineRunConfiguration();
    // a small buffer, so producers and consumers fill it and wait on each other
    config.setRowSetSize("100");
    config.setLockFreeRowSets(lockFreeRowSets);
    return pipeline;
  }

  @Test
  void lockFreeRowSetsAreOffByDefault() throws Exception {
    assertFalse(new LocalPipelineRunConfiguration().isLockFreeRowSets());

    LocalPipelineEngine pipeline = createPipeline(false);
    pipeline.prepareExecution();
    assertFalse(pipeline.isLockFreeRowSets());
    for (IRowSet rowSet : pipeline.getRowsets()) {
      assertInstanceOf(BlockingRowSet.class, rowSet);
    }
    pipeline.addRowProducer("A", 0).finished();
    pipeline.addRowProducer("C", 0).finished();
    pipeline.startThreads();
    pipeline.waitUntilFinished();
  }

  @Test
  void everyRowArrivesThroughLockFreeRowSets() throws Exception {
    LocalPipelineEngine pipeline = createPipeline(true);
    pipeline.prepareExecution();

    assertTrue(pipeline.isLockFreeRowSets());
    assertEquals(2, pipeline.getRowsets().size());
    for (IRowSet rowSet : pipeline.getRowsets()) {
      assertInstanceOf(LockFreeRowSet.class, rowSet);
    }

    RowProducer producerA = pipeline.addRowProducer("A", 0);
    RowProducer producerC = pipeline.addRowProducer("C", 0);
    assertInstanceOf(LockFreeRowSet.class, producerA.getRowSet());

    pipeline.startThreads();
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    Thread feedC =
        new Thread(
            () -> {
              for (long i = 0; i < ROWS_PER_INPUT; i++) {
                producerC.putRow(rowMeta, new Object[] {i});
              }
              producerC.finished();
            });
    feedC.start();
    for (long i = 0; i < ROWS_PER_INPUT; i++) {
      producerA.putRow(rowMeta, new Object[] {i});
    }
    producerA.finished();
    feedC.join();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getErrors());
    assertEquals(
        2L * ROWS_PER_INPUT, pipeline.getTransform("B", 0).getLinesRead(), "rows read by B");
    assertEquals(
        2L * ROWS_PER_INPUT, pipeline.getTransform("B", 0).getLinesWritten(), "rows written by B");
  }

  /**
   * One input finishes without rows, the other one sends bursts with idle gaps: the reader parks on
   * its inputs in between and is woken by each burst and at the end.
   */
  @Test
  @Timeout(60)
  void readsBurstsWhileAnotherInputIsAlreadyDone() throws Exception {
    LocalPipelineEngine pipeline = createPipeline(true);
    pipeline.prepareExecution();
    RowProducer producerA = pipeline.addRowProducer("A", 0);
    RowProducer producerC = pipeline.addRowProducer("C", 0);
    pipeline.startThreads();

    producerC.finished();
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    int bursts = 5;
    int rowsPerBurst = 1_000;
    for (int b = 0; b < bursts; b++) {
      for (long i = 0; i < rowsPerBurst; i++) {
        producerA.putRow(rowMeta, new Object[] {i});
      }
      Thread.sleep(20);
    }
    producerA.finished();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getErrors());
    assertEquals(
        (long) bursts * rowsPerBurst,
        pipeline.getTransform("B", 0).getLinesRead(),
        "rows read by B");
  }

  @Test
  void copyKeepsTheOption() {
    LocalPipelineRunConfiguration config = new LocalPipelineRunConfiguration();
    config.setLockFreeRowSets(true);
    assertTrue(config.clone().isLockFreeRowSets());
  }
}

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

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class PipelineTest {
  @Mock private ITransform transformMock, transformMock2;
  @Mock private ITransformData data, data2;
  @Mock private TransformMeta transformMeta, transformMeta2;
  @Mock private PipelineMeta pipelineMeta;

  int count = 10000;
  IPipelineEngine<PipelineMeta> pipeline;
  PipelineMeta meta;

  @BeforeAll
  static void beforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void beforeTest() throws HopException {
    meta = new PipelineMeta();
    pipeline = new LocalPipelineEngine(meta);
    pipeline.setLogChannel(Mockito.mock(ILogChannel.class));
    pipeline.prepareExecution();
    pipeline.startThreads();
  }

  /** Execution of pipeline with no transforms never ends */
  @Test
  void pipelineWithNoTransformsIsNotEndless() throws Exception {
    Pipeline pipelineWithNoTransforms = new LocalPipelineEngine(new PipelineMeta());
    pipelineWithNoTransforms = spy(pipelineWithNoTransforms);

    pipelineWithNoTransforms.prepareExecution();

    pipelineWithNoTransforms.startThreads();

    // check pipeline lifecycle is not corrupted
    verify(pipelineWithNoTransforms).fireExecutionStartedListeners();
    verify(pipelineWithNoTransforms).fireExecutionFinishedListeners();
  }

  /**
   * ConcurrentModificationException when restarting pipeline Test that listeners can be accessed
   * concurrently during pipeline finish
   */
  @Test
  void testPipelineFinishListenersConcurrentModification() throws InterruptedException {
    CountDownLatch start = new CountDownLatch(1);
    PipelineFinishListenerAdder add = new PipelineFinishListenerAdder(pipeline, start);
    PipelineFinishListenerFirer firer = new PipelineFinishListenerFirer(pipeline, start);
    startThreads(add, firer, start);
    assertEquals(count, add.c, "All listeners are added: no ConcurrentModificationException");
    assertEquals(
        count,
        firer.c,
        "All Finish listeners are iterated over: no ConcurrentModificationException");
  }

  /** Test that listeners can be accessed concurrently during pipeline start */
  @Test
  void testPipelineStartListenersConcurrentModification() throws InterruptedException {
    CountDownLatch start = new CountDownLatch(1);
    PipelineFinishListenerAdder add = new PipelineFinishListenerAdder(pipeline, start);
    PipelineStartListenerFirer starter = new PipelineStartListenerFirer(pipeline, start);
    startThreads(add, starter, start);
    assertEquals(count, add.c, "All listeners are added: no ConcurrentModificationException");
    assertEquals(
        count,
        starter.c,
        "All Start listeners are iterated over: no ConcurrentModificationException");
  }

  /** Test that pipeline stop listeners can be accessed concurrently */
  @Test
  void testPipelineStoppedListenersConcurrentModification() throws InterruptedException {
    CountDownLatch start = new CountDownLatch(1);
    PipelineStoppedCaller stopper = new PipelineStoppedCaller(pipeline, start);
    PipelineStopListenerAdder adder = new PipelineStopListenerAdder(pipeline, start);
    startThreads(stopper, adder, start);
    assertEquals(count, adder.c, "All pipeline stop listeners is added");
    assertEquals(count, stopper.c, "All stop call success");
  }

  @Test
  void testFirePipelineFinishedListeners() throws Exception {
    Pipeline line = new LocalPipelineEngine();
    IExecutionFinishedListener mockListener = mock(IExecutionFinishedListener.class);
    line.addExecutionFinishedListener(mockListener);

    line.fireExecutionFinishedListeners();

    verify(mockListener).finished(line);
  }

  @Test
  void testFireExecutionFinishedListenersExceptionOnPipelineFinished() throws Exception {
    Pipeline line = new LocalPipelineEngine();
    IExecutionFinishedListener mockListener = mock(IExecutionFinishedListener.class);
    doThrow(HopException.class).when(mockListener).finished(line);
    line.addExecutionFinishedListener(mockListener);

    assertThrows(HopException.class, line::fireExecutionFinishedListeners);
  }

  @Test
  void testFinishStatus() throws Exception {
    while (pipeline.isRunning()) {
      Thread.sleep(1);
    }
    assertEquals(Pipeline.STRING_FINISHED, pipeline.getStatusDescription());
  }

  private void startThreads(Runnable one, Runnable two, CountDownLatch start)
      throws InterruptedException {
    Thread th = new Thread(one);
    Thread tt = new Thread(two);
    th.start();
    tt.start();
    start.countDown();
    th.join();
    tt.join();
  }

  private abstract class PipelineKicker implements Runnable {
    protected IPipelineEngine<PipelineMeta> pipeline;
    protected int c = 0;
    protected CountDownLatch start;
    protected int max = count;

    PipelineKicker(IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start) {
      this.pipeline = pipeline;
      this.start = start;
    }

    protected boolean isStopped() {
      c++;
      return c >= max;
    }
  }

  private class PipelineStoppedCaller extends PipelineKicker {
    PipelineStoppedCaller(IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start) {
      super(pipeline, start);
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch (InterruptedException e) {
        throw new RuntimeException();
      }
      while (!isStopped()) {
        pipeline.stopAll();
      }
    }
  }

  private class PipelineStopListenerAdder extends PipelineKicker {
    PipelineStopListenerAdder(IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start) {
      super(pipeline, start);
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch (InterruptedException e) {
        throw new RuntimeException();
      }
      while (!isStopped()) {
        pipeline.addExecutionStoppedListener(pipelineStoppedListener);
      }
    }
  }

  private class PipelineFinishListenerAdder extends PipelineKicker {
    PipelineFinishListenerAdder(IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start) {
      super(pipeline, start);
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch (InterruptedException e) {
        throw new RuntimeException();
      }
      // run
      while (!isStopped()) {
        pipeline.addExecutionFinishedListener(listener);
      }
    }
  }

  private class PipelineFinishListenerFirer extends PipelineKicker {
    PipelineFinishListenerFirer(IPipelineEngine<PipelineMeta> tr, CountDownLatch start) {
      super(tr, start);
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch (InterruptedException e) {
        throw new RuntimeException();
      }
      // run
      while (!isStopped()) {
        try {
          pipeline.fireExecutionFinishedListeners();
          // clean array blocking queue
          pipeline.waitUntilFinished();
        } catch (HopException e) {
          throw new RuntimeException();
        }
      }
    }
  }

  private class PipelineStartListenerFirer extends PipelineKicker {
    PipelineStartListenerFirer(IPipelineEngine<PipelineMeta> pipeline, CountDownLatch start) {
      super(pipeline, start);
    }

    @Override
    public void run() {
      try {
        start.await();
      } catch (InterruptedException e) {
        throw new RuntimeException();
      }
      // run
      while (!isStopped()) {
        try {
          pipeline.fireExecutionStartedListeners();
        } catch (HopException e) {
          throw new RuntimeException();
        }
      }
    }
  }

  private final IExecutionFinishedListener<IPipelineEngine<PipelineMeta>> listener = p -> {};

  private final IExecutionStoppedListener<IPipelineEngine<PipelineMeta>> pipelineStoppedListener =
      p -> {};

  /**
   * When a workflow is scheduled twice, it gets the same log channel Id and both logs get merged
   */
  @Test
  void testTwoPipelineGetSameLogChannelId() throws Exception {
    PipelineMeta pMeta = mock(PipelineMeta.class);
    doReturn(new String[] {"A", "B", "C"}).when(pMeta).listParameters();
    doReturn("").when(pMeta).getParameterDescription(anyString());
    doReturn("").when(pMeta).getParameterDefault(anyString());

    IPipelineEngine<PipelineMeta> pipeline1 = new LocalPipelineEngine(pMeta);
    IPipelineEngine<PipelineMeta> pipeline2 = new LocalPipelineEngine(pMeta);

    assertEquals(pipeline1.getLogChannelId(), pipeline2.getLogChannelId());
  }

  @Test
  void testSetInternalEntryCurrentDirectoryWithFilename() {
    Pipeline pipelineTest = new LocalPipelineEngine();
    boolean hasFilename = true;
    pipelineTest.copyFrom(null);
    pipelineTest.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution");
    pipelineTest.setVariable(
        Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory");
    pipelineTest.setInternalEntryCurrentDirectory(hasFilename);

    assertEquals(
        "file:///C:/SomeFilenameDirectory",
        pipelineTest.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  @Test
  void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    Pipeline pipelineTest = new LocalPipelineEngine();
    pipelineTest.copyFrom(null);
    boolean hasFilename = false;
    pipelineTest.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution");
    pipelineTest.setVariable(
        Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory");
    pipelineTest.setInternalEntryCurrentDirectory(hasFilename);

    assertEquals(
        "Original value defined at run execution",
        pipelineTest.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }
}

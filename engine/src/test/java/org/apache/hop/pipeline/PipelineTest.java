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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.extension.ExtensionPointMap;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.IClassLoadingPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
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

  /**
   * A caller blocked in {@link Pipeline#waitUntilFinished()} must not be released before the
   * PipelineCompleted extension point has run. Extension points there still have work to flush -
   * lineage for one - and a released caller (hop-run, for example) can leave the JVM immediately.
   */
  @Test
  void pipelineCompletedExtensionPointRunsBeforeWaitersAreReleased() throws Exception {
    List<String> order = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch waiterReleased = new CountDownLatch(1);

    TestExtensionPointPlugin plugin =
        registerExtensionPoint(
            HopExtensionPoint.PipelineCompleted.id,
            () -> {
              order.add("extension-point");
              // If the waiter was released first (the bug) it records itself while this runs.
              // Waiting for that here keeps the assertion deterministic instead of a race.
              try {
                waiterReleased.await(400, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    try {
      Pipeline line = new LocalPipelineEngine(new PipelineMeta());
      line.setLogChannel(mock(ILogChannel.class));
      line.prepareExecution();

      Thread waiter =
          new Thread(
              () -> {
                line.waitUntilFinished();
                order.add("waiter");
                waiterReleased.countDown();
              });
      waiter.start();

      // A pipeline without transforms completes in startThreads(), on this thread.
      line.startThreads();

      waiter.join(10_000);
      assertEquals(List.of("extension-point", "waiter"), order);
    } finally {
      ExtensionPointMap.getInstance().removeExtensionPoint(plugin);
    }
  }

  /**
   * A transform has to finish announcing itself before the pipeline concludes that the last
   * transform is done. A plugin emitting its final events on TransformFinished - lineage does -
   * otherwise queues them after the pipeline has already fired its completion and flushed.
   */
  @Test
  void transformFinishedExtensionPointRunsBeforePipelineCompleted() throws Exception {
    List<String> order = Collections.synchronizedList(new ArrayList<>());

    TestExtensionPointPlugin transformFinished =
        registerExtensionPoint(
            HopExtensionPoint.TransformFinished.id, () -> order.add("transform-finished"));
    TestExtensionPointPlugin pipelineCompleted =
        registerExtensionPoint(
            HopExtensionPoint.PipelineCompleted.id, () -> order.add("pipeline-completed"));

    try {
      PipelineMeta meta = new PipelineMeta();
      meta.setName("ordering test");
      meta.addTransform(new TransformMeta("A", new DummyMeta()));

      LocalPipelineEngine run =
          new LocalPipelineEngine(meta, new Variables(), new LoggingObject("test"));
      run.prepareExecution();
      run.startThreads();
      run.waitUntilFinished();

      assertEquals(List.of("transform-finished", "pipeline-completed"), order);
    } finally {
      ExtensionPointMap.getInstance().removeExtensionPoint(transformFinished);
      ExtensionPointMap.getInstance().removeExtensionPoint(pipelineCompleted);
    }
  }

  /**
   * The TransformFinished extension point runs ahead of the pipeline's completion detection, so a
   * plugin that fails there must not keep the pipeline from finishing.
   */
  @Test
  void failingTransformFinishedExtensionPointDoesNotHangThePipeline() throws Exception {
    TestExtensionPointPlugin transformFinished =
        registerExtensionPoint(
            HopExtensionPoint.TransformFinished.id,
            () -> {
              throw new IllegalStateException("broken plugin");
            });

    try {
      PipelineMeta meta = new PipelineMeta();
      meta.setName("failing extension point test");
      meta.addTransform(new TransformMeta("A", new DummyMeta()));

      LocalPipelineEngine run =
          new LocalPipelineEngine(meta, new Variables(), new LoggingObject("test"));
      run.prepareExecution();
      run.startThreads();

      Thread waiter = new Thread(run::waitUntilFinished);
      waiter.start();
      waiter.join(10_000);

      assertFalse(waiter.isAlive(), "waitUntilFinished() did not return");
      assertTrue(run.isFinished());
    } finally {
      ExtensionPointMap.getInstance().removeExtensionPoint(transformFinished);
    }
  }

  /**
   * A single threaded run has to announce its transforms like any other. Those transforms are run
   * by the executor instead of by threads of their own, and that pipeline type used to fire neither
   * TransformBeforeStart nor TransformFinished - so a single threaded run produced no transform
   * level lineage at all.
   */
  @Test
  void singleThreadedPipelineAnnouncesItsTransforms() throws Exception {
    List<String> order = Collections.synchronizedList(new ArrayList<>());

    TestExtensionPointPlugin transformStart =
        registerExtensionPoint(
            HopExtensionPoint.TransformBeforeStart.id, () -> order.add("transform-start"));
    TestExtensionPointPlugin transformFinished =
        registerExtensionPoint(
            HopExtensionPoint.TransformFinished.id, () -> order.add("transform-finished"));

    try {
      PipelineMeta meta = new PipelineMeta();
      meta.setName("single threaded test");
      meta.addTransform(new TransformMeta("A", new DummyMeta()));

      LocalPipelineEngine run =
          new LocalPipelineEngine(meta, new Variables(), new LoggingObject("test"));
      run.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
      run.prepareExecution();
      run.startThreads();

      // What SingleThreadedPipelineEngine does with the executor...
      SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor(run);
      assertTrue(executor.init());
      executor.oneIteration();
      executor.dispose();

      assertEquals(List.of("transform-start", "transform-finished"), order);
    } finally {
      ExtensionPointMap.getInstance().removeExtensionPoint(transformStart);
      ExtensionPointMap.getInstance().removeExtensionPoint(transformFinished);
    }
  }

  /**
   * Hand the extension point map a fake extension point, so a test can watch for it. The caller
   * removes the returned plugin again.
   */
  private TestExtensionPointPlugin registerExtensionPoint(
      String extensionPointId, Runnable action) {
    IExtensionPoint<Object> extensionPoint = (logChannel, variables, object) -> action.run();

    TestExtensionPointPlugin plugin = mock(TestExtensionPointPlugin.class);
    // For an extension point plugin the name is the extension point it listens to, and the ids are
    // the plugin's own ids: ExtensionPointPluginType extracts them that way.
    when(plugin.getName()).thenReturn(extensionPointId);
    when(plugin.getIds()).thenReturn(new String[] {"test-" + extensionPointId});
    when(plugin.getMainType()).thenReturn((Class) IExtensionPoint.class);
    when(plugin.loadClass(IExtensionPoint.class)).thenReturn(extensionPoint);
    ExtensionPointMap.getInstance().addExtensionPoint(plugin);
    return plugin;
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
        throw new HopRuntimeException();
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
        throw new HopRuntimeException();
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
        throw new HopRuntimeException();
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
        throw new HopRuntimeException();
      }
      // run
      while (!isStopped()) {
        try {
          pipeline.fireExecutionFinishedListeners();
          // clean array blocking queue
          pipeline.waitUntilFinished();
        } catch (HopException e) {
          throw new HopRuntimeException();
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
        throw new HopRuntimeException();
      }
      // run
      while (!isStopped()) {
        try {
          pipeline.fireExecutionStartedListeners();
        } catch (HopException e) {
          throw new HopRuntimeException();
        }
      }
    }
  }

  private final IExecutionFinishedListener<IPipelineEngine<PipelineMeta>> listener = p -> {};

  /**
   * A plugin that loads its own classes, so a test can hand the extension point map a fake
   * extension point without going through the plugin registry.
   */
  private interface TestExtensionPointPlugin extends IClassLoadingPlugin, IPlugin {}

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

  /**
   * Issue #3861: a pipeline that never got as far as allocating its transforms could not be stopped
   * at all, so it kept whatever non-terminal state it was in. Nothing ever cleaned it up.
   */
  @Test
  void stopAllWorksBeforeTheTransformsAreAllocated() {
    Pipeline pipeline = new LocalPipelineEngine(new PipelineMeta());
    pipeline.setLogChannel(mock(ILogChannel.class));
    pipeline.setPreparing(true);

    pipeline.stopAll();

    assertTrue(pipeline.isStopped());
    assertEquals(Pipeline.STRING_STOPPED, pipeline.getStatusDescription());
  }

  /**
   * A stopped pipeline is no longer preparing or initializing. The Hop GUI reads those flags to
   * decide whether a pipeline is still on the go, so they may not survive a stop.
   */
  @Test
  void stopAllClearsTheTransientExecutionFlags() {
    Pipeline pipeline = new LocalPipelineEngine(new PipelineMeta());
    pipeline.setLogChannel(mock(ILogChannel.class));
    pipeline.setPreparing(true);
    pipeline.setInitializing(true);

    pipeline.stopAll();

    assertFalse(pipeline.isPreparing());
    assertFalse(pipeline.isInitializing());
  }

  /** Stopping stays a one-shot operation. */
  @Test
  void stopAllOnlyFiresTheStoppedListenersOnce() {
    Pipeline pipeline = new LocalPipelineEngine(new PipelineMeta());
    pipeline.setLogChannel(mock(ILogChannel.class));
    int[] stopped = new int[1];
    pipeline.addExecutionStoppedListener(p -> stopped[0]++);

    pipeline.stopAll();
    pipeline.stopAll();

    assertEquals(1, stopped[0]);
  }
}

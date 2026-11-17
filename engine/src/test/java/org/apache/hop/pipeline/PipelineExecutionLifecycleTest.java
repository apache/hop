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
package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointMap;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.plugins.IClassLoadingPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.execution.lifecycle.ExecutionHandle;
import org.apache.hop.execution.lifecycle.ExecutionKind;
import org.apache.hop.execution.lifecycle.ExecutionLifecycle;
import org.apache.hop.execution.lifecycle.ExecutionObserver;
import org.apache.hop.execution.lifecycle.ExecutionScope;
import org.apache.hop.execution.lifecycle.ExecutionState;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PipelineExecutionLifecycleTest {
  enum Mode {
    NORMAL,
    INIT_FAILURE,
    DISPOSE_FAILURE,
    RUN_AND_DISPOSE_FAILURE
  }

  @Transform(id = "LifecycleTestTransform", name = "Lifecycle test transform")
  public static class ProbeMeta extends BaseTransformMeta<Probe, ProbeData> {
    Mode mode = Mode.NORMAL;
    CountDownLatch rendezvous;
    CountDownLatch entered;
    CountDownLatch release;
    CountDownLatch disposed;
    final List<Visit> visits = new CopyOnWriteArrayList<>();
  }

  public static class ProbeData extends BaseTransformData {}

  record Visit(String phase, Object subject, ExecutionHandle handle, Thread thread) {}

  public static class Probe extends BaseTransform<ProbeMeta, ProbeData> {
    private int rows;

    public Probe(
        TransformMeta transformMeta,
        ProbeMeta meta,
        ProbeData data,
        int copy,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, copy, pipelineMeta, pipeline);
    }

    private void visit(String phase) {
      meta.visits.add(new Visit(phase, this, ExecutionLifecycle.current(), Thread.currentThread()));
    }

    @Override
    public boolean init() {
      visit("init");
      if (meta.mode == Mode.INIT_FAILURE) {
        throw new IllegalStateException("init failure");
      }
      return super.init();
    }

    @Override
    public boolean processRow() throws HopException {
      visit("run");
      if (meta.entered != null) {
        meta.entered.countDown();
        try {
          if (!meta.release.await(5, TimeUnit.SECONDS)) {
            throw new HopException("Release barrier timed out");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new HopException(e);
        }
        return false;
      }
      if (meta.mode == Mode.RUN_AND_DISPOSE_FAILURE) {
        throw new HopException("run failure");
      }
      if (meta.rendezvous != null && rows == 0) {
        meta.rendezvous.countDown();
        try {
          if (!meta.rendezvous.await(10, TimeUnit.SECONDS)) {
            throw new HopException("Copies did not run concurrently");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new HopException(e);
        }
      }
      if (++rows < 3) {
        return true;
      }
      setOutputDone();
      return false;
    }

    @Override
    public void dispose() {
      visit("dispose");
      super.dispose();
      if (meta.disposed != null) {
        meta.disposed.countDown();
      }
      if (meta.mode == Mode.DISPOSE_FAILURE || meta.mode == Mode.RUN_AND_DISPOSE_FAILURE) {
        throw new IllegalStateException("dispose failure");
      }
    }
  }

  private static class Recorder implements ExecutionObserver {
    final List<ExecutionHandle> starts = new CopyOnWriteArrayList<>();
    final Map<ExecutionHandle, ExecutionState> ends = new ConcurrentHashMap<>();
    final Map<ExecutionHandle, Throwable> failures = new ConcurrentHashMap<>();
    final List<ExecutionHandle> duplicateEnds = new CopyOnWriteArrayList<>();
    final List<Thread> leakedThreads = new CopyOnWriteArrayList<>();

    @Override
    public void started(ExecutionHandle handle) {
      starts.add(handle);
    }

    @Override
    public ExecutionScope activate(ExecutionHandle handle) {
      final var thread = Thread.currentThread();
      return () -> {
        if (thread != Thread.currentThread()) {
          leakedThreads.add(thread);
        }
      };
    }

    @Override
    public void finished(ExecutionHandle handle, ExecutionState state, Throwable failure) {
      if (failure != null) {
        failures.put(handle, failure);
      }
      if (ends.put(handle, state) != null) {
        duplicateEnds.add(handle);
      }
    }
  }

  @BeforeAll
  static void initialize() throws Exception {
    HopEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(ProbeMeta.class.getName(), TransformPluginType.class, Transform.class);
  }

  private LocalPipelineEngine pipeline(ProbeMeta probe, int copies) {
    final var meta = new PipelineMeta();
    meta.setName("same-name");
    final var transform = new TransformMeta("probe", probe);
    transform.setCopies(copies);
    meta.addTransform(transform);
    return new LocalPipelineEngine(meta);
  }

  private void run(Pipeline pipeline) throws HopException {
    pipeline.prepareExecution();
    pipeline.startThreads();
    pipeline.waitUntilFinished();
  }

  private void verifyVisits(ProbeMeta meta, Recorder recorder, Pipeline pipeline) {
    for (final var visit : meta.visits) {
      assertNotNull(visit.handle(), visit.phase());
      assertSame(visit.subject(), visit.handle().getSubject());
      assertEquals(ExecutionKind.TRANSFORM, visit.handle().getKind());
      assertSame(pipeline, visit.handle().getParent().getSubject());
      assertTrue(recorder.starts.contains(visit.handle()));
      for (final var other : meta.visits) {
        if (visit.subject() == other.subject()) {
          assertSame(visit.handle(), other.handle());
        }
      }
    }
    assertEquals(recorder.starts.size(), recorder.ends.size());
    assertTrue(recorder.duplicateEnds.isEmpty());
    assertTrue(recorder.leakedThreads.isEmpty());
    assertNull(ExecutionLifecycle.current());
    assertNull(ExecutionLifecycle.get(pipeline));
  }

  @Test
  void copiesAndConcurrentRootsHaveIndependentIdentities() throws Exception {
    final var recorder = new Recorder();
    final var first = new ProbeMeta();
    final var second = new ProbeMeta();
    final var rendezvous = new CountDownLatch(4);
    first.rendezvous = rendezvous;
    second.rendezvous = rendezvous;
    final var a = pipeline(first, 2);
    final var b = pipeline(second, 2);
    try (final var registration = ExecutionLifecycle.addObserver(recorder);
        final var workers = Executors.newFixedThreadPool(2)) {
      final var one =
          workers.submit(
              () -> {
                run(a);
                assertNull(ExecutionLifecycle.current());
                return null;
              });
      final var two =
          workers.submit(
              () -> {
                run(b);
                assertNull(ExecutionLifecycle.current());
                return null;
              });
      one.get(20, TimeUnit.SECONDS);
      two.get(20, TimeUnit.SECONDS);
      verifyVisits(first, recorder, a);
      verifyVisits(second, recorder, b);
      final var roots =
          recorder.starts.stream().filter(h -> h.getKind() == ExecutionKind.PIPELINE).toList();
      assertEquals(2, roots.size());
      assertNull(roots.get(0).getParent());
      assertNull(roots.get(1).getParent());
      assertNotSame(roots.get(0), roots.get(1));
      assertEquals(6, recorder.starts.size());
      assertEquals(
          2,
          recorder.starts.stream()
              .filter(h -> h.getKind() == ExecutionKind.TRANSFORM && h.getCopy() == 0)
              .count());
      assertEquals(
          2,
          recorder.starts.stream()
              .filter(h -> h.getKind() == ExecutionKind.TRANSFORM && h.getCopy() == 1)
              .count());
      assertTrue(recorder.ends.values().stream().allMatch(s -> s == ExecutionState.SUCCEEDED));
    }
  }

  @Test
  void initFailureStillDisposesTheSameCopyAndEndsPipeline() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    probe.mode = Mode.INIT_FAILURE;
    final var pipeline = pipeline(probe, 1);
    try (final var registration = ExecutionLifecycle.addObserver(recorder)) {
      assertThrows(HopException.class, pipeline::prepareExecution);
      verifyVisits(probe, recorder, pipeline);
      assertEquals(List.of("init", "dispose"), probe.visits.stream().map(Visit::phase).toList());
      assertTrue(recorder.ends.values().stream().allMatch(s -> s == ExecutionState.FAILED));
    }
  }

  @Test
  void disposeFailureDoesNotSkipCompletionOrContextRestoration() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    probe.mode = Mode.DISPOSE_FAILURE;
    final var pipeline = pipeline(probe, 2);
    try (final var registration = ExecutionLifecycle.addObserver(recorder)) {
      run(pipeline);
      verifyVisits(probe, recorder, pipeline);
      assertTrue(pipeline.isFinished());
      assertTrue(recorder.ends.values().stream().allMatch(s -> s == ExecutionState.FAILED));
    }
  }

  @Test
  void runFailureRemainsPrimaryWhenDisposalAlsoFails() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    probe.mode = Mode.RUN_AND_DISPOSE_FAILURE;
    final var pipeline = pipeline(probe, 1);
    try (final var registration = ExecutionLifecycle.addObserver(recorder)) {
      run(pipeline);
      verifyVisits(probe, recorder, pipeline);
      final var copy =
          recorder.starts.stream()
              .filter(handle -> handle.getKind() == ExecutionKind.TRANSFORM)
              .findFirst()
              .orElseThrow();
      final var failure = recorder.failures.get(copy);
      assertNotNull(failure);
      assertEquals("run failure", failure.getMessage().strip());
      assertEquals(1, failure.getSuppressed().length);
      assertEquals("dispose failure", failure.getSuppressed()[0].getMessage());
      assertEquals(ExecutionState.FAILED, recorder.ends.get(copy));
    }
  }

  @Test
  void earlyLocalSetupFailureIsObserved() {
    final var recorder = new Recorder();
    final var pipeline = pipeline(new ProbeMeta(), 1);
    pipeline.setPipelineRunConfiguration(null);
    try (final var registration = ExecutionLifecycle.addObserver(recorder)) {
      assertThrows(NullPointerException.class, pipeline::prepareExecution);
      assertEquals(1, recorder.starts.size());
      assertEquals(ExecutionState.FAILED, recorder.ends.get(recorder.starts.getFirst()));
      assertNull(ExecutionLifecycle.get(pipeline));
      assertNull(ExecutionLifecycle.current());
    }
  }

  @Test
  void finishedListenerFailureStillCompletesAndRestoresContext() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    final var pipeline = pipeline(probe, 1);
    final var listenerContexts = new CopyOnWriteArrayList<ExecutionHandle>();
    pipeline.addExecutionFinishedListener(
        engine -> {
          listenerContexts.add(ExecutionLifecycle.current());
          throw new HopException("listener failure");
        });
    try (final var registration = ExecutionLifecycle.addObserver(recorder)) {
      run(pipeline);
      verifyVisits(probe, recorder, pipeline);
      assertEquals(1, listenerContexts.size());
      assertSame(pipeline, listenerContexts.getFirst().getSubject());
      assertEquals(ExecutionState.FAILED, recorder.ends.get(listenerContexts.getFirst()));
    }
  }

  @Test
  void fastWorkersCannotPublishSuccessBeforeStartCallbacksReturn() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    final var run = pipeline(probe, 1);
    final var stopped = new CountDownLatch(1);
    final var plugin =
        extension(
            HopExtensionPoint.PipelineStart,
            (log, variables, subject) -> {
              await(stopped);
              assertNotNull(ExecutionLifecycle.get(run), "Startup is still executing");
            });
    try (final var registration = ExecutionLifecycle.addObserver(recorder);
        final var signal =
            ExecutionLifecycle.addObserver(
                new ExecutionObserver() {
                  @Override
                  public void started(ExecutionHandle handle) {}

                  @Override
                  public void finished(
                      ExecutionHandle handle, ExecutionState state, Throwable failure) {
                    if (handle.getKind() == ExecutionKind.TRANSFORM) {
                      stopped.countDown();
                    }
                  }
                })) {
      run(run);
      verifyVisits(probe, recorder, run);
    } finally {
      ExtensionPointMap.getInstance().removeExtensionPoint(plugin);
    }
  }

  @Test
  void startCallbackFailureWaitsForRunningCopiesButDoesNotBlockCaller() throws Exception {
    startFailure(false);
  }

  @Test
  void partialLaunchDisposesUnstartedCopyWithoutEndingRunningCopy() throws Exception {
    startFailure(true);
  }

  private void startFailure(boolean partial) throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    probe.entered = new CountDownLatch(1);
    probe.release = new CountDownLatch(1);
    final var run = pipeline(probe, partial ? 2 : 1);
    final var calls = new AtomicInteger();
    final var plugin =
        extension(
            partial ? HopExtensionPoint.TransformBeforeStart : HopExtensionPoint.PipelineStart,
            (log, variables, subject) -> {
              if (!partial || calls.incrementAndGet() == 2) {
                await(probe.entered);
                throw new HopException("startup failure");
              }
            });
    try (final var registration = ExecutionLifecycle.addObserver(recorder)) {
      run.prepareExecution();
      assertThrows(HopException.class, run::startThreads);
      final var root = recorder.starts.getFirst();
      assertFalse(recorder.ends.containsKey(root), "A running copy still owns the lifetime");
      assertSame(root, ExecutionLifecycle.get(run));
      probe.release.countDown();
      run.waitUntilFinished();
      verifyVisits(probe, recorder, run);
      assertEquals(ExecutionState.FAILED, recorder.ends.get(root));
      assertEquals(
          partial ? 2 : 1, probe.visits.stream().filter(v -> v.phase().equals("dispose")).count());
    } finally {
      probe.release.countDown();
      ExtensionPointMap.getInstance().removeExtensionPoint(plugin);
    }
  }

  private interface TestPlugin extends IPlugin, IClassLoadingPlugin {}

  @Test
  void startupFailureRemainsPrimaryWhenCompletionListenerAlsoFails() throws Exception {
    final var pipeline = new LocalPipelineEngine(new PipelineMeta());
    final var primary = new HopException("startup");
    final var secondary = new HopException("completion");
    pipeline.addExecutionStartedListener(
        engine -> {
          throw primary;
        });
    pipeline.addExecutionFinishedListener(
        engine -> {
          throw secondary;
        });
    pipeline.prepareExecution();
    assertSame(primary, assertThrows(HopException.class, pipeline::startThreads));
    assertEquals(1, primary.getSuppressed().length);
    assertSame(secondary, primary.getSuppressed()[0]);
    assertNull(ExecutionLifecycle.current());
  }

  @Test
  void stopBeforeWorkerLaunchAndDuringWorkerRunCloseExactlyOnce() throws Exception {
    for (final boolean during : new boolean[] {false, true}) {
      final var recorder = new Recorder();
      final var probe = new ProbeMeta();
      probe.entered = new CountDownLatch(1);
      probe.release = new CountDownLatch(1);
      final var pipeline = pipeline(probe, 1);
      final var plugin =
          extension(
              HopExtensionPoint.TransformBeforeStart,
              (log, variables, subject) -> {
                if (!during) {
                  pipeline.stopAll();
                }
              });
      try (final var registration = ExecutionLifecycle.addObserver(recorder)) {
        pipeline.prepareExecution();
        pipeline.startThreads();
        if (during) {
          await(probe.entered);
          pipeline.stopAll();
          assertFalse(recorder.ends.containsKey(recorder.starts.getFirst()));
        }
        probe.release.countDown();
        pipeline.waitUntilFinished();
        assertEquals(recorder.starts.size(), recorder.ends.size());
        assertEquals(ExecutionState.CANCELLED, recorder.ends.get(recorder.starts.getFirst()));
        assertNull(ExecutionLifecycle.get(pipeline));
        assertNull(ExecutionLifecycle.current());
      } finally {
        probe.release.countDown();
        ExtensionPointMap.getInstance().removeExtensionPoint(plugin);
      }
    }
  }

  private void await(CountDownLatch latch) throws HopException {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException(e);
    }
  }

  private TestPlugin extension(HopExtensionPoint point, IExtensionPoint<Object> callback)
      throws Exception {
    final var plugin = mock(TestPlugin.class);
    when(plugin.getName()).thenReturn(point.id);
    when(plugin.getIds()).thenReturn(new String[] {"lifecycle-" + point.id});
    when(plugin.getMainType()).thenReturn((Class) IExtensionPoint.class);
    when(plugin.loadClass(IExtensionPoint.class)).thenReturn(callback);
    ExtensionPointMap.getInstance().addExtensionPoint(plugin);
    return plugin;
  }

  @Test
  void singleThreadedVisitsReuseTheCopyIdentity() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    final var pipeline = pipeline(probe, 2);
    pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    try (final var registration = ExecutionLifecycle.addObserver(recorder)) {
      pipeline.prepareExecution();
      pipeline.startThreads();
      final var executor = new SingleThreadedPipelineExecutor(pipeline);
      try {
        assertFalse(executor.oneIteration());
      } finally {
        executor.dispose();
      }
      pipeline.waitUntilFinished();
      verifyVisits(probe, recorder, pipeline);
      assertEquals(3, recorder.starts.size());
      assertEquals(6, probe.visits.stream().filter(v -> v.phase().equals("run")).count());
      assertTrue(
          probe.visits.stream()
              .filter(v -> !v.phase().equals("init"))
              .allMatch(v -> v.thread() == Thread.currentThread()));
    }
  }
}

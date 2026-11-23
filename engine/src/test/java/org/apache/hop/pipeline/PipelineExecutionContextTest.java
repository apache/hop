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
import org.apache.hop.execution.context.ExecutionContextListener;
import org.apache.hop.execution.context.ExecutionContexts;
import org.apache.hop.execution.context.ExecutionScope;
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
class PipelineExecutionContextTest {
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
    final HopException runFailure = new HopException("run failure");
    final List<Visit> visits = new CopyOnWriteArrayList<>();
  }

  public static class ProbeData extends BaseTransformData {}

  private static final ThreadLocal<String> CONTEXT = new ThreadLocal<>();

  record Visit(String phase, Probe subject, String logChannelId, Thread thread) {}

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
      meta.visits.add(new Visit(phase, this, CONTEXT.get(), Thread.currentThread()));
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
        throw meta.runFailure;
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

  private static class Recorder implements ExecutionContextListener {
    private record Activation(String id, Thread thread) {}

    private final Map<Activation, AtomicInteger> opened = new ConcurrentHashMap<>();
    private final Map<Activation, AtomicInteger> closed = new ConcurrentHashMap<>();
    private final Map<Activation, AtomicInteger> active = new ConcurrentHashMap<>();
    private final List<String> violations = new CopyOnWriteArrayList<>();

    @Override
    public ExecutionScope activate(final String logChannelId) {
      final var previous = CONTEXT.get();
      final var activation = new Activation(logChannelId, Thread.currentThread());
      opened.computeIfAbsent(activation, ignored -> new AtomicInteger()).incrementAndGet();
      active.computeIfAbsent(activation, ignored -> new AtomicInteger()).incrementAndGet();
      CONTEXT.set(logChannelId);
      return () -> {
        if (activation.thread() != Thread.currentThread()) {
          violations.add("Scope closed on another thread: " + logChannelId);
        }
        if (!logChannelId.equals(CONTEXT.get())) {
          violations.add("Scope did not restore in nesting order: " + logChannelId);
        }
        closed.computeIfAbsent(activation, ignored -> new AtomicInteger()).incrementAndGet();
        if (active.get(activation).decrementAndGet() < 0) {
          violations.add("Scope closed more than once: " + logChannelId);
        }
        if (previous == null) {
          CONTEXT.remove();
        } else {
          CONTEXT.set(previous);
        }
      };
    }

    private void verifyClosed() {
      assertFalse(opened.isEmpty());
      assertEquals(opened.keySet(), closed.keySet());
      opened.forEach(
          (key, count) -> assertEquals(count.get(), closed.get(key).get(), key.toString()));
      active.forEach((key, count) -> assertEquals(0, count.get(), key.toString()));
      assertTrue(violations.isEmpty(), violations.toString());
      assertNull(CONTEXT.get());
    }

    private boolean observed(final String id, final Thread thread) {
      return opened.containsKey(new Activation(id, thread));
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

  private void verifyVisits(ProbeMeta meta, Recorder recorder, Pipeline pipeline)
      throws InterruptedException {
    joinWorkers(meta);
    assertFalse(meta.visits.isEmpty());
    for (final var visit : meta.visits) {
      assertNotNull(visit.logChannelId(), visit.phase());
      assertEquals(visit.subject().getLogChannelId(), visit.logChannelId(), visit.phase());
      assertEquals(pipeline.getLogChannelId(), visit.subject().getParent().getLogChannelId());
      assertTrue(recorder.observed(visit.logChannelId(), visit.thread()), visit.phase());
    }
    recorder.verifyClosed();
  }

  private void joinWorkers(final ProbeMeta meta) throws InterruptedException {
    // waitUntilFinished publishes completion before the worker's outer activation scope closes.
    for (final var thread :
        meta.visits.stream()
            .filter(v -> v.phase().equals("run"))
            .map(Visit::thread)
            .distinct()
            .toList()) {
      if (thread != Thread.currentThread()) {
        thread.join(5000);
        assertFalse(thread.isAlive(), "Transform worker must release its activation scope");
      }
    }
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
    try (final var registration = ExecutionContexts.addListener(recorder);
        final var workers = Executors.newFixedThreadPool(2)) {
      final var one =
          workers.submit(
              () -> {
                run(a);
                assertNull(CONTEXT.get());
                return null;
              });
      final var two =
          workers.submit(
              () -> {
                run(b);
                assertNull(CONTEXT.get());
                return null;
              });
      one.get(20, TimeUnit.SECONDS);
      two.get(20, TimeUnit.SECONDS);
      joinWorkers(first);
      joinWorkers(second);
      verifyVisits(first, recorder, a);
      verifyVisits(second, recorder, b);
      assertFalse(a.getLogChannelId().equals(b.getLogChannelId()));
      final var visits = new java.util.ArrayList<>(first.visits);
      visits.addAll(second.visits);
      assertEquals(4, visits.stream().map(Visit::logChannelId).distinct().count());
      assertEquals(
          4,
          visits.stream()
              .filter(v -> v.phase().equals("run"))
              .map(Visit::thread)
              .distinct()
              .count());
      for (final var probe : List.of(first, second)) {
        for (int copy = 0; copy < 2; copy++) {
          final int copyNumber = copy;
          assertEquals(
              1,
              probe.visits.stream()
                  .filter(v -> v.subject().getCopy() == copyNumber)
                  .map(Visit::logChannelId)
                  .distinct()
                  .count());
        }
      }
    }
  }

  @Test
  void rerunningPipelineCreatesFreshRunAndCopyChannels() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    final var pipeline = pipeline(probe, 2);
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      run(pipeline);
      verifyVisits(probe, recorder, pipeline);
      final var firstRunId = pipeline.getLogChannelId();
      final var firstCopyIds = probe.visits.stream().map(Visit::logChannelId).distinct().toList();
      probe.visits.clear();
      run(pipeline);
      verifyVisits(probe, recorder, pipeline);
      assertFalse(firstRunId.equals(pipeline.getLogChannelId()));
      assertEquals(2, probe.visits.stream().map(Visit::logChannelId).distinct().count());
      assertTrue(
          probe.visits.stream().noneMatch(visit -> firstCopyIds.contains(visit.logChannelId())));
    }
  }

  @Test
  void initFailureStillDisposesTheSameCopyAndEndsPipeline() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    probe.mode = Mode.INIT_FAILURE;
    final var pipeline = pipeline(probe, 1);
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      assertThrows(HopException.class, pipeline::prepareExecution);
      verifyVisits(probe, recorder, pipeline);
      assertEquals(List.of("init", "dispose"), probe.visits.stream().map(Visit::phase).toList());
      assertFalse(pipeline.getFailures().isEmpty());
      assertNotNull(pipeline.getRunFailure());
    }
  }

  @Test
  void disposeFailureDoesNotSkipCompletionOrContextRestoration() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    probe.mode = Mode.DISPOSE_FAILURE;
    final var pipeline = pipeline(probe, 2);
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      run(pipeline);
      verifyVisits(probe, recorder, pipeline);
      assertTrue(pipeline.isFinished());
      assertFalse(pipeline.getFailures().isEmpty());
      assertNotNull(pipeline.getRunFailure());
    }
  }

  @Test
  void runFailureRemainsPrimaryWhenDisposalAlsoFails() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    probe.mode = Mode.RUN_AND_DISPOSE_FAILURE;
    final var pipeline = pipeline(probe, 1);
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      run(pipeline);
      verifyVisits(probe, recorder, pipeline);
      assertEquals(1, pipeline.getFailures().size());
      assertEquals(
          "run failure", pipeline.getFailures().getFirst().getCause().getMessage().strip());
      assertEquals(1, probe.runFailure.getSuppressed().length);
      assertEquals("dispose failure", probe.runFailure.getSuppressed()[0].getMessage());
      assertNotNull(pipeline.getRunFailure());
    }
  }

  @Test
  void earlyLocalSetupFailureIsObserved() {
    final var recorder = new Recorder();
    final var pipeline = pipeline(new ProbeMeta(), 1);
    pipeline.setPipelineRunConfiguration(null);
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      assertThrows(NullPointerException.class, pipeline::prepareExecution);
      assertNull(CONTEXT.get());
      assertTrue(recorder.active.values().stream().allMatch(count -> count.get() == 0));
      assertTrue(recorder.violations.isEmpty());
    }
  }

  @Test
  void finishedListenerFailureStillCompletesAndRestoresContext() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    final var pipeline = pipeline(probe, 1);
    final var listenerContexts = new CopyOnWriteArrayList<String>();
    pipeline.addExecutionFinishedListener(
        engine -> {
          listenerContexts.add(CONTEXT.get());
          throw new HopException("listener failure");
        });
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      run(pipeline);
      verifyVisits(probe, recorder, pipeline);
      assertEquals(1, listenerContexts.size());
      assertEquals(pipeline.getLogChannelId(), listenerContexts.getFirst());
      assertTrue(pipeline.isFinished());
    }
  }

  @Test
  void startupCallbackKeepsPipelineContextWhileFastWorkerDisposes() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    probe.disposed = new CountDownLatch(1);
    final var run = pipeline(probe, 1);
    final var plugin =
        extension(
            HopExtensionPoint.PipelineStart,
            (log, variables, subject) -> {
              assertEquals(run.getLogChannelId(), CONTEXT.get());
              await(probe.disposed);
              assertEquals(run.getLogChannelId(), CONTEXT.get());
            });
    try (final var registration = ExecutionContexts.addListener(recorder)) {
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
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      run.prepareExecution();
      assertThrows(HopException.class, run::startThreads);
      assertEquals(1, probe.visits.stream().filter(v -> v.phase().equals("run")).count());
      assertEquals(
          partial ? 1 : 0, probe.visits.stream().filter(v -> v.phase().equals("dispose")).count());
      assertNull(
          CONTEXT.get(), "The startup caller must be restored while the worker remains active");
      assertTrue(
          recorder.active.entrySet().stream()
              .anyMatch(
                  entry ->
                      entry.getKey().thread() != Thread.currentThread()
                          && entry.getValue().get() > 0));
      probe.release.countDown();
      run.waitUntilFinished();
      verifyVisits(probe, recorder, run);
      assertNotNull(run.getRunFailure());
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
    assertNull(CONTEXT.get());
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
      try (final var registration = ExecutionContexts.addListener(recorder)) {
        pipeline.prepareExecution();
        pipeline.startThreads();
        if (during) {
          await(probe.entered);
          pipeline.stopAll();
          assertTrue(
              recorder.active.entrySet().stream()
                  .anyMatch(
                      entry ->
                          entry.getKey().thread() != Thread.currentThread()
                              && entry.getValue().get() > 0));
        }
        probe.release.countDown();
        pipeline.waitUntilFinished();
        verifyVisits(probe, recorder, pipeline);
        assertTrue(pipeline.isStopped());
        assertTrue(pipeline.isFinished());
        assertEquals(1, probe.visits.stream().filter(v -> v.phase().equals("dispose")).count());
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
  void singleThreadedDisposalFailureStillDisposesEveryCopyAndRestoresContext() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    probe.mode = Mode.DISPOSE_FAILURE;
    final var pipeline = pipeline(probe, 2);
    pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      pipeline.prepareExecution();
      pipeline.startThreads();
      final var executor = new SingleThreadedPipelineExecutor(pipeline);
      assertFalse(executor.oneIteration());
      final var failure = assertThrows(IllegalStateException.class, executor::dispose);
      assertEquals("dispose failure", failure.getMessage());
      assertEquals(1, failure.getSuppressed().length);
      pipeline.waitUntilFinished();
      verifyVisits(probe, recorder, pipeline);
      assertEquals(
          2, probe.visits.stream().filter(visit -> "dispose".equals(visit.phase())).count());
      assertEquals(2, pipeline.getFailures().size());
      assertTrue(pipeline.isFinished());
    }
  }

  @Test
  void singleThreadedVisitsReuseTheCopyIdentity() throws Exception {
    final var recorder = new Recorder();
    final var probe = new ProbeMeta();
    final var pipeline = pipeline(probe, 2);
    pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    try (final var registration = ExecutionContexts.addListener(recorder)) {
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
      assertEquals(2, probe.visits.stream().map(Visit::logChannelId).distinct().count());
      assertEquals(6, probe.visits.stream().filter(v -> v.phase().equals("run")).count());
      assertTrue(
          probe.visits.stream()
              .filter(v -> !v.phase().equals("init"))
              .allMatch(v -> v.thread() == Thread.currentThread()));
    }
  }
}

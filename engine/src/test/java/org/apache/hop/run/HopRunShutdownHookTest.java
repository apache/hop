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
 *
 */

package org.apache.hop.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;

class HopRunShutdownHookTest {

  private final ILogChannel log = mock(ILogChannel.class);
  private final AtomicInteger afterStopCalls = new AtomicInteger();
  private final Runnable afterStop = afterStopCalls::incrementAndGet;

  @Test
  void nothingIsStoppedWhenTheRunIsOver() {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);
    hook.track(pipeline);
    hook.runFinished();

    hook.run();

    verify(pipeline, never()).stopAll();
    verify(log, never()).logMinimal(anyString());
    assertEquals(0, afterStopCalls.get());
  }

  @Test
  void hookReturnsAtOnceWhenNothingWasStarted() throws Exception {
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);
    Thread hookThread = start(hook);

    // hop-run is still loading the pipeline: the hook waits for the run to end.
    Thread.sleep(300);
    assertTrue(hookThread.isAlive());

    hook.runFinished();
    hookThread.join(5000);
    assertFalse(hookThread.isAlive());
  }

  @Test
  void pipelineIsStoppedAndTheHookWaitsUntilTheRunEnds() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);
    hook.track(pipeline);

    Thread hookThread = start(hook);
    verify(pipeline, timeout(5000)).stopAll();

    // The pipeline is still winding down, so the JVM must not exit yet, nor may lineage be
    // delivered: the end of the run is not in it yet.
    Thread.sleep(300);
    assertTrue(hookThread.isAlive());
    assertEquals(0, afterStopCalls.get());

    hook.runFinished();
    hookThread.join(5000);
    assertFalse(hookThread.isAlive());
    assertEquals(1, afterStopCalls.get());
    verify(pipeline, times(1)).stopAll();
    verify(log).logMinimal("The run has stopped, hop-run exits.");
  }

  @Test
  void workflowIsStopped() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.getWorkflowName()).thenReturn("wf");
    when(workflow.isStopped()).thenReturn(true);
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);
    hook.track(workflow);

    Thread hookThread = start(hook);
    verify(workflow, timeout(5000)).stopExecution();
    hook.runFinished();
    hookThread.join(5000);

    assertFalse(hookThread.isAlive());
    verify(workflow, times(1)).stopExecution();
    verify(log).logMinimal("Stopping workflow 'wf'");
  }

  /** A workflow clears its stopped flag when it starts. A stop sent just before that is lost. */
  @Test
  void stopIsRepeatedWhenTheWorkflowClearsItsStoppedFlag() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.getWorkflowName()).thenReturn("wf");
    AtomicBoolean stopped = new AtomicBoolean();
    when(workflow.isStopped()).thenAnswer(invocation -> stopped.get());
    doAnswer(
            invocation -> {
              stopped.set(true);
              return null;
            })
        .when(workflow)
        .stopExecution();
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);
    hook.track(workflow);

    Thread hookThread = start(hook);
    verify(workflow, timeout(5000)).stopExecution();

    // The workflow starts and clears the flag.
    stopped.set(false);
    verify(workflow, timeout(5000).times(2)).stopExecution();

    // Once the stop holds it is not repeated.
    Thread.sleep(3 * HopRunShutdownHook.CHECK_INTERVAL_MS);
    verify(workflow, times(2)).stopExecution();

    hook.runFinished();
    hookThread.join(5000);
    assertFalse(hookThread.isAlive());
  }

  /**
   * A remote engine only reports the run as stopped once the server says so. Until then the hook
   * may not send a stop request on every check.
   */
  @Test
  void stopIsSentOnceWhileTheStoppedFlagIsNotSetYet() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    when(pipeline.isStopped()).thenReturn(false);
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);
    hook.track(pipeline);

    Thread hookThread = start(hook);
    verify(pipeline, timeout(5000)).stopAll();
    Thread.sleep(4 * HopRunShutdownHook.CHECK_INTERVAL_MS);
    verify(pipeline, times(1)).stopAll();

    hook.runFinished();
    hookThread.join(5000);
    assertFalse(hookThread.isAlive());
  }

  /** A remote engine throws when the server can't be reached or the run isn't submitted yet. */
  @Test
  void failedStopIsTriedAgainAndTheHookKeepsWaiting() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    doThrow(new HopRuntimeException("server down"))
        .doThrow(new HopRuntimeException("server down"))
        .doNothing()
        .when(pipeline)
        .stopAll();
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);
    hook.track(pipeline);

    Thread hookThread = start(hook);
    verify(pipeline, timeout(5000).times(3)).stopAll();
    assertTrue(hookThread.isAlive(), "The hook may not give up when a stop fails");
    assertEquals(0, afterStopCalls.get());

    String message = "Stopping pipeline 'pl' failed, trying again in a second.";
    verify(log).logError(eq(message), any(Throwable.class));
    verify(log).logError(message);
    verify(log, times(1)).logMinimal("Stopping pipeline 'pl'");

    // The retries are spaced out.
    Thread.sleep(3 * HopRunShutdownHook.CHECK_INTERVAL_MS);
    verify(pipeline, times(3)).stopAll();

    hook.runFinished();
    hookThread.join(5000);
    assertFalse(hookThread.isAlive());
    assertEquals(1, afterStopCalls.get());
  }

  /** A timeout meant as "wait forever" may not overflow the deadline. */
  @Test
  void hugeTimeoutStillStops() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    HopRunShutdownHook hook =
        new HopRunShutdownHook(log, Duration.ofSeconds(Long.MAX_VALUE), afterStop);
    hook.track(pipeline);

    Thread hookThread = start(hook);
    verify(pipeline, timeout(5000)).stopAll();
    Thread.sleep(300);
    assertTrue(hookThread.isAlive());

    hook.runFinished();
    hookThread.join(5000);
    assertFalse(hookThread.isAlive());
    assertEquals(1, afterStopCalls.get());
  }

  @Test
  void executionStartedAfterTheSignalIsStopped() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);

    Thread hookThread = start(hook);
    Thread.sleep(300);
    hook.track(pipeline);

    verify(pipeline, timeout(5000)).stopAll();
    hook.runFinished();
    hookThread.join(5000);
    assertFalse(hookThread.isAlive());
  }

  @Test
  void finishedExecutionIsNotStoppedAgain() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    when(pipeline.isStopped()).thenReturn(false);
    when(pipeline.isFinished()).thenReturn(true);
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);
    hook.track(pipeline);

    Thread hookThread = start(hook);
    verify(pipeline, timeout(5000)).stopAll();
    Thread.sleep(3 * HopRunShutdownHook.CHECK_INTERVAL_MS);
    hook.runFinished();
    hookThread.join(5000);

    verify(pipeline, times(1)).stopAll();
  }

  @Test
  void hookGivesUpAfterTheTimeout() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(1), afterStop);
    hook.track(pipeline);

    long start = System.nanoTime();
    hook.run();
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertTrue(elapsedMs >= 1000, "The hook returned after " + elapsedMs + "ms");
    assertTrue(elapsedMs < 5000, "The hook returned after " + elapsedMs + "ms");
    assertEquals(1, afterStopCalls.get(), "Lineage is delivered after a timeout too");
    verify(log)
        .logError(
            "The run did not stop within 1 seconds, hop-run exits anyway. Use --shutdown-timeout or HOP_RUN_SHUTDOWN_TIMEOUT to wait longer.");
  }

  @Test
  void zeroTimeoutStillStops() {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ZERO, afterStop);
    hook.track(pipeline);

    hook.run();

    verify(pipeline).stopAll();
  }

  @Test
  void secondCallDoesNothing() throws Exception {
    IPipelineEngine<PipelineMeta> pipeline = pipeline();
    HopRunShutdownHook hook = new HopRunShutdownHook(log, Duration.ofSeconds(10), afterStop);
    hook.track(pipeline);

    Thread first = start(hook);
    verify(pipeline, timeout(5000)).stopAll();

    Thread second = start(hook);
    second.join(5000);
    assertFalse(second.isAlive(), "A second call may not wait as well");

    hook.runFinished();
    first.join(5000);
    verify(pipeline, times(1)).stopAll();
    assertEquals(1, afterStopCalls.get());
    verify(log, atLeast(1)).logMinimal(anyString());
  }

  private IPipelineEngine<PipelineMeta> pipeline() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("pl");
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(pipelineMeta);
    when(pipeline.isStopped()).thenReturn(true);
    return pipeline;
  }

  private static Thread start(HopRunShutdownHook hook) {
    Thread thread = new Thread(hook, "test-shutdown-hook");
    thread.setDaemon(true);
    thread.start();
    return thread;
  }
}

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

package org.apache.hop.beam.engines;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineRunConfiguration;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Issue #9713, on the Beam engine: an asynchronous run saves its final execution information from
 * {@code evaluatePipelineStatus()}, which holds the engine lock. A location resolves its paths with
 * the variables it was given while it holds its own lock. When those variables were the engine
 * itself, a timer tick in flight needed the engine lock to resolve, and deadlocked against the
 * final save.
 */
class BeamPipelineEngineExecutionInfoDeadlockTest {

  private static final long WAIT_SECONDS = 10L;

  @BeforeAll
  static void beforeAll() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  @Timeout(60)
  void finalSaveUnderTheEngineLockDoesNotDeadlockWithARunningTick() throws Exception {
    Object locationLock = new Object();
    AtomicReference<IVariables> locationVariables = new AtomicReference<>();
    AtomicReference<Thread> tickThread = new AtomicReference<>();
    CountDownLatch tickInside = new CountDownLatch(1);
    CountDownLatch finisherWaiting = new CountDownLatch(1);
    AtomicBoolean firstUpdate = new AtomicBoolean(true);

    // A location that locks itself and resolves a path with its variables under that lock, like
    // FileExecutionInfoLocation.updateExecutionState() -> getSubFolder() does.
    IExecutionInfoLocation location = mock(IExecutionInfoLocation.class);
    doAnswer(
            invocation -> {
              locationVariables.set(invocation.getArgument(0));
              return null;
            })
        .when(location)
        .initialize(any(), any());
    doAnswer(
            invocation -> {
              synchronized (locationLock) {
                if (firstUpdate.compareAndSet(true, false)) {
                  // The first update is the timer tick: hold the location while the run finishes.
                  tickThread.set(Thread.currentThread());
                  tickInside.countDown();
                  finisherWaiting.await(WAIT_SECONDS, TimeUnit.SECONDS);
                }
                locationVariables.get().resolve("${java.io.tmpdir}/executions");
              }
              return null;
            })
        .when(location)
        .updateExecutionState(any());

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(ExecutionInfoLocation.class)
        .save(new ExecutionInfoLocation("location", "", "0", "600000", null, null, location));

    BeamDirectPipelineEngine engine = new BeamDirectPipelineEngine();
    engine.initializeFrom(null);
    engine.setPipelineMeta(new PipelineMeta());
    engine.setMetadataProvider(metadataProvider);
    engine.setPipelineRunConfiguration(
        new PipelineRunConfiguration(
            "beam",
            "",
            "location",
            new ArrayList<>(),
            new BeamDirectPipelineRunConfiguration(),
            null,
            false));
    engine.lookupExecutionInformationLocation();
    engine.startExecutionInfoTimer();
    assertTrue(
        tickInside.await(WAIT_SECONDS, TimeUnit.SECONDS),
        "the execution information timer never reached the location");

    // The run finishes while the tick holds the location: evaluatePipelineStatus() holds the
    // engine lock and saves the final state from there.
    AtomicReference<Throwable> finishError = new AtomicReference<>();
    Thread finisher =
        new Thread(
            () -> {
              try {
                synchronized (engine) {
                  engine.stopExecutionInfoTimer();
                }
              } catch (Throwable e) {
                finishError.set(e);
              }
            },
            "beam run finishing");
    finisher.setDaemon(true);
    finisher.start();

    // Let the tick go on only once the finishing thread waits for the location.
    waitUntilWaiting(finisher);
    finisherWaiting.countDown();

    finisher.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
    assertFalse(
        finisher.isAlive(),
        "the final save of the execution information deadlocked with the timer tick");
    assertNull(finishError.get());
    tickThread.get().join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
    assertNotSame(
        engine,
        locationVariables.get(),
        "the location must not resolve through the engine, whose resolve() locks it");
  }

  private static void waitUntilWaiting(Thread thread) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(WAIT_SECONDS);
    while (System.currentTimeMillis() < deadline) {
      Thread.State state = thread.getState();
      if (state == Thread.State.BLOCKED
          || state == Thread.State.WAITING
          || state == Thread.State.TIMED_WAITING
          || state == Thread.State.TERMINATED) {
        return;
      }
      Thread.sleep(10L);
    }
  }
}

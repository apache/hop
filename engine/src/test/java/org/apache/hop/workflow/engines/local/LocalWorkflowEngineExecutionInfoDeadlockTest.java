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

package org.apache.hop.workflow.engines.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Regression for issue #9713: a workflow hung at its end when the execution information timer
 * ticked while the workflow finished.
 *
 * <p>The file location resolves its folders with the workflow's variables while it holds its own
 * lock, and {@code Variables.resolve()} locks the workflow engine. The final save took the engine
 * lock first and then the location's: a tick in flight at that moment deadlocked against it.
 */
class LocalWorkflowEngineExecutionInfoDeadlockTest {

  private static final long WAIT_SECONDS = 10L;

  @BeforeAll
  static void beforeAll() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  @Timeout(60)
  void finalSaveDoesNotDeadlockWithARunningTick() throws Exception {
    Object locationLock = new Object();
    AtomicReference<IVariables> locationVariables = new AtomicReference<>();
    AtomicBoolean firstTick = new AtomicBoolean(true);
    CountDownLatch tickInside = new CountDownLatch(1);
    CountDownLatch stopperWaiting = new CountDownLatch(1);
    AtomicInteger stateUpdates = new AtomicInteger();
    AtomicBoolean closed = new AtomicBoolean(false);

    // A location that locks itself and resolves a path with the workflow's variables under that
    // lock, like FileExecutionInfoLocation.updateExecutionState() -> getSubFolder() does.
    IExecutionInfoLocation location = mock(IExecutionInfoLocation.class);
    when(location.clone()).thenReturn(location);
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
                if (isTimerThread() && firstTick.compareAndSet(true, false)) {
                  // Hold the location while the workflow finishes.
                  tickInside.countDown();
                  stopperWaiting.await(WAIT_SECONDS, TimeUnit.SECONDS);
                }
                locationVariables.get().resolve("${java.io.tmpdir}/executions");
                stateUpdates.incrementAndGet();
              }
              return null;
            })
        .when(location)
        .updateExecutionState(any());
    doAnswer(
            invocation -> {
              synchronized (locationLock) {
                closed.set(true);
              }
              return null;
            })
        .when(location)
        .close();

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(ExecutionInfoLocation.class)
        .save(new ExecutionInfoLocation("location", "", "0", "600000", null, null, location));

    LocalWorkflowEngine engine = new LocalWorkflowEngine(new WorkflowMeta());
    engine.initializeFrom(null);
    engine.setMetadataProvider(metadataProvider);
    engine.setWorkflowRunConfiguration(
        new WorkflowRunConfiguration(
            "local", "", "location", new LocalWorkflowRunConfiguration(), false));
    engine.lookupExecutionInformationLocation();
    engine.startExecutionInfoTimer();

    assertTrue(
        tickInside.await(WAIT_SECONDS, TimeUnit.SECONDS),
        "the execution information timer never reached the location");

    // The workflow finishes while the tick holds the location.
    AtomicReference<Throwable> stopError = new AtomicReference<>();
    Thread stopper =
        new Thread(
            () -> {
              try {
                engine.stopExecutionInfoTimer();
              } catch (Throwable e) {
                stopError.set(e);
              }
            },
            "workflow finishing");
    stopper.setDaemon(true);
    stopper.start();

    // Let the tick go on only once the finishing thread waits for a lock.
    waitUntilWaiting(stopper);
    stopperWaiting.countDown();

    stopper.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
    assertFalse(
        stopper.isAlive(),
        "the final save of the execution information deadlocked with the timer tick");
    assertNull(stopError.get());
    assertEquals(2, stateUpdates.get(), "the tick and the final save both update the state");
    assertTrue(closed.get(), "the location is closed after the final save");
  }

  private static boolean isTimerThread() {
    return Thread.currentThread().getName().startsWith("LocalWorkflowEngine execution timer");
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

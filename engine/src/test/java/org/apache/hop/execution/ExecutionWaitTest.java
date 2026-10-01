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

package org.apache.hop.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.Test;

class ExecutionWaitTest {

  @Test
  void parseTimeoutMsTreatsEmptyAndInvalidAsUnlimited() {
    Variables vars = new Variables();
    assertEquals(0L, ExecutionWait.parseTimeoutMs(vars, null));
    assertEquals(0L, ExecutionWait.parseTimeoutMs(vars, ""));
    assertEquals(0L, ExecutionWait.parseTimeoutMs(vars, "  "));
    assertEquals(0L, ExecutionWait.parseTimeoutMs(vars, "0"));
    assertEquals(0L, ExecutionWait.parseTimeoutMs(vars, "soon"));
    assertEquals(0L, ExecutionWait.parseTimeoutMs(vars, "-5"));
    assertEquals(1500L, ExecutionWait.parseTimeoutMs(vars, "1500"));
    vars.setVariable("T", "250");
    assertEquals(250L, ExecutionWait.parseTimeoutMs(vars, "${T}"));
  }

  @Test
  void waitForReturnsImmediatelyWhenAlreadyDone() {
    assertTrue(ExecutionWait.waitFor(() -> true, () -> false, 5_000L));
  }

  @Test
  void waitForTimesOutWhenNeverDone() {
    long start = System.currentTimeMillis();
    assertFalse(ExecutionWait.waitFor(() -> false, () -> false, 80L));
    assertTrue(System.currentTimeMillis() - start < 2_000L);
  }

  @Test
  void waitForReturnsTrueWhenAborted() {
    AtomicBoolean abort = new AtomicBoolean(true);
    assertTrue(ExecutionWait.waitFor(() -> false, abort::get, 5_000L));
  }

  @Test
  void waitForPipelineWithoutTimeoutDelegatesToEngine() {
    @SuppressWarnings("unchecked")
    IPipelineEngine<?> engine = mock(IPipelineEngine.class);
    assertTrue(ExecutionWait.waitForPipeline(engine, 0L));
    verify(engine).waitUntilFinished();
    verify(engine, never()).stopAll();
  }

  @Test
  void waitForPipelineStopsEngineWhenTimeoutElapses() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch released = new CountDownLatch(1);

    @SuppressWarnings("unchecked")
    IPipelineEngine<?> engine = mock(IPipelineEngine.class);
    when(engine.getLogChannelId()).thenReturn("test-channel");
    doAnswer(
            invocation -> {
              started.countDown();
              released.await();
              return null;
            })
        .when(engine)
        .waitUntilFinished();
    doAnswer(
            invocation -> {
              released.countDown();
              return null;
            })
        .when(engine)
        .stopAll();

    long start = System.currentTimeMillis();
    boolean finishedInTime = ExecutionWait.waitForPipeline(engine, 80L);
    assertFalse(finishedInTime);
    assertTrue(System.currentTimeMillis() - start < 2_000L);
    verify(engine).stopAll();
    started.await();
  }

  @Test
  void waitForPipelineReturnsTrueWhenEngineFinishesBeforeTimeout() {
    @SuppressWarnings("unchecked")
    IPipelineEngine<?> engine = mock(IPipelineEngine.class);
    when(engine.getLogChannelId()).thenReturn("fast");
    assertTrue(ExecutionWait.waitForPipeline(engine, 5_000L));
    verify(engine).waitUntilFinished();
    verify(engine, never()).stopAll();
  }
}

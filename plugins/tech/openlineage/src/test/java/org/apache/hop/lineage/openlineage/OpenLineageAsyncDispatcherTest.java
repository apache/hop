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

package org.apache.hop.lineage.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.lineage.openlineage.OpenLineageAsyncDispatcher.OverflowPolicy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class OpenLineageAsyncDispatcherTest {

  private static final OpenLineage OL = new OpenLineage(URI.create("https://example.com/producer"));

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  private static RunEvent event() {
    return OL.newRunEventBuilder()
        .eventType(RunEvent.EventType.OTHER)
        .eventTime(java.time.Instant.ofEpochMilli(1L).atZone(java.time.ZoneOffset.UTC))
        .run(OL.newRun(java.util.UUID.randomUUID(), null))
        .job(OL.newJob("ns", "job", null))
        .build();
  }

  /** Emitter that blocks in {@link #emit} until released, recording everything it receives. */
  private static final class BlockingEmitter implements OpenLineageEmitter {
    final List<RunEvent> emitted = new CopyOnWriteArrayList<>();
    final CountDownLatch firstEmitStarted = new CountDownLatch(1);
    final CountDownLatch release;

    BlockingEmitter(boolean block) {
      this.release = new CountDownLatch(block ? 1 : 0);
    }

    @Override
    public boolean emit(RunEvent event) {
      firstEmitStarted.countDown();
      try {
        release.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      emitted.add(event);
      return true;
    }

    @Override
    public void close() {}
  }

  @Test
  void dropPolicyCountsOverflowOnceQueueIsFull() throws Exception {
    BlockingEmitter emitter = new BlockingEmitter(true);
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(3, OverflowPolicy.DROP, emitter, new LogChannel("drop"));

    // Worker pulls one event and blocks inside emit, leaving the bounded queue (capacity 3) free.
    dispatcher.enqueueAll(List.of(event()));
    emitter.firstEmitStarted.await(5, TimeUnit.SECONDS);

    // 3 fit in the queue, 2 are dropped.
    List<RunEvent> five = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      five.add(event());
    }
    dispatcher.enqueueAll(five);
    assertEquals(2, dispatcher.getDroppedCount());

    emitter.release.countDown();
    dispatcher.close();
  }

  @Test
  void blockPolicyIsLosslessWhileTheCollectorKeepsUp() throws Exception {
    BlockingEmitter emitter = new BlockingEmitter(false);
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(2, OverflowPolicy.BLOCK, emitter, new LogChannel("block"));

    List<RunEvent> ten = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ten.add(event());
    }
    dispatcher.enqueueAll(ten); // waits for capacity when full, never drops
    dispatcher.close(); // drains the remainder

    assertEquals(0, dispatcher.getDroppedCount());
    assertEquals(10, emitter.emitted.size());
  }

  /**
   * The caller here is the lineage hub's dispatcher thread, and a pipeline finishing behind it is
   * waiting on a flush marker that thread has to process. So BLOCK must give up rather than wait
   * out an unreachable collector: past the enqueue timeout it drops and returns.
   */
  @Test
  void blockPolicyGivesUpInsteadOfWaitingForeverOnAStalledCollector() throws Exception {
    BlockingEmitter emitter = new BlockingEmitter(true);
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            2, OverflowPolicy.BLOCK, emitter, new LogChannel("bounded"), 0, 50);

    // Worker takes one event and stalls inside emit; the bounded queue then fills and stays full.
    dispatcher.enqueueAll(List.of(event()));
    emitter.firstEmitStarted.await(5, TimeUnit.SECONDS);

    List<RunEvent> six = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      six.add(event());
    }
    long start = System.nanoTime();
    dispatcher.enqueueAll(six);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    // 2 fit the queue, the other 4 each wait out the 50 ms timeout and are dropped.
    assertEquals(4, dispatcher.getDroppedCount());
    assertTrue(elapsedMs < 5_000, "enqueue should be bounded, took " + elapsedMs + "ms");

    emitter.release.countDown();
    dispatcher.close();
  }

  // Ensures the worker keeps draining and exits cleanly even with rapid enqueue.
  @Test
  void closeDrainsRemainingEvents() throws Exception {
    AtomicInteger count = new AtomicInteger();
    OpenLineageEmitter emitter =
        new OpenLineageEmitter() {
          @Override
          public boolean emit(RunEvent event) {
            count.incrementAndGet();
            return true;
          }

          @Override
          public void close() {}
        };
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            1000, OverflowPolicy.BLOCK, emitter, new LogChannel("drain"));
    List<RunEvent> events = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      events.add(event());
    }
    dispatcher.enqueueAll(events);
    dispatcher.close();
    assertEquals(50, count.get());
  }

  // With a metrics interval set, the periodic logger thread runs and is stopped cleanly on close().
  @Test
  void periodicMetricsThreadRunsAndStopsOnClose() throws Exception {
    OpenLineageEmitter emitter =
        new OpenLineageEmitter() {
          @Override
          public boolean emit(RunEvent event) {
            return true;
          }

          @Override
          public void close() {}
        };
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            100, OverflowPolicy.BLOCK, emitter, new LogChannel("metrics"), 40);
    dispatcher.enqueueAll(List.of(event(), event()));
    Thread.sleep(120); // let the scheduler fire a couple of times
    assertTrue(metricsThreadPresent(), "metrics thread should be running");

    dispatcher.close();
    boolean gone = false;
    for (int i = 0; i < 50 && !(gone = !metricsThreadPresent()); i++) {
      Thread.sleep(20);
    }
    assertFalse(metricsThreadPresent(), "metrics thread should be gone after close");
  }

  private static boolean metricsThreadPresent() {
    return Thread.getAllStackTraces().keySet().stream()
        .anyMatch(t -> "hop-openlineage-sink-metrics".equals(t.getName()) && t.isAlive());
  }
}

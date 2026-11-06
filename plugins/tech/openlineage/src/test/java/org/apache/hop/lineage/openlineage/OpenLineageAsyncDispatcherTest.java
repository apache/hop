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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.lineage.openlineage.OpenLineageAsyncDispatcher.OverflowPolicy;
import org.apache.hop.lineage.openlineage.OpenLineageEmitter.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OpenLineageAsyncDispatcherTest {

  private static final OpenLineage OL = new OpenLineage(URI.create("https://example.com/producer"));

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  private static RunEvent event() {
    return event("job");
  }

  private static RunEvent event(String jobName) {
    return OL.newRunEventBuilder()
        .eventType(RunEvent.EventType.OTHER)
        .eventTime(java.time.Instant.ofEpochMilli(1L).atZone(java.time.ZoneOffset.UTC))
        .run(OL.newRun(java.util.UUID.randomUUID(), null))
        .job(OL.newJob("ns", jobName, null))
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
    public Result emit(RunEvent event) {
      firstEmitStarted.countDown();
      try {
        release.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      emitted.add(event);
      return Result.SENT;
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
          public Result emit(RunEvent event) {
            count.incrementAndGet();
            return Result.SENT;
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
          public Result emit(RunEvent event) {
            return Result.SENT;
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

  // ---------------------------------------------------------------------------------------------
  // Spool: durability when the queue overflows, and replay of what an earlier process left behind
  // ---------------------------------------------------------------------------------------------

  @TempDir Path spoolFolder;

  private OpenLineageSpool spool() {
    OpenLineageSpool spool =
        OpenLineageSpool.open(spoolFolder.toString(), 8L * 1024 * 1024, new LogChannel("spool"));
    assertNotNull(spool);
    return spool;
  }

  private static void awaitTrue(String what, java.util.function.BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10_000;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(condition.getAsBoolean(), what);
  }

  @Test
  void queueOverflowGoesToTheSpoolInsteadOfBeingDropped() throws Exception {
    OpenLineageSpool spool = spool();
    BlockingEmitter emitter = new BlockingEmitter(true);
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            1, OverflowPolicy.DROP, emitter, new LogChannel("spool-drop"), 0, 50, spool);

    // The worker takes the first event and stalls; the 1-slot queue takes one, the rest would be
    // dropped without a spool.
    dispatcher.enqueueAll(List.of(event()));
    emitter.firstEmitStarted.await(5, TimeUnit.SECONDS);
    List<RunEvent> four = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      four.add(event());
    }
    dispatcher.enqueueAll(four);

    assertEquals(0, dispatcher.getDroppedCount(), "spooled events are not dropped, only deferred");
    assertEquals(3, dispatcher.getBacklogSize());
    assertEquals(5, spool.getStoredCount());

    emitter.release.countDown();
    awaitTrue("all five events should be delivered", () -> emitter.emitted.size() == 5);
    awaitTrue("delivered events should leave the spool", () -> spool.getStoredCount() == 0);
    dispatcher.close();
  }

  @Test
  void replaysWhatAnEarlierProcessLeftOnDisk() throws Exception {
    OpenLineageSpool firstProcess = spool();
    firstProcess.store(event());
    firstProcess.store(event());
    assertEquals(2, firstProcess.getStoredCount());

    CountingEmitter emitter = new CountingEmitter(true);
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            10, OverflowPolicy.DROP, emitter, new LogChannel("spool-replay"), 0, 50, spool());

    awaitTrue("both spooled events should be delivered", () -> emitter.count.get() == 2);
    awaitTrue("delivered events should leave the spool", () -> dispatcher.getBacklogSize() == 0);
    dispatcher.close();
    assertEquals(0, spool().getStoredCount());
  }

  @Test
  void failedBacklogDeliveryKeepsTheEventForTheNextAttempt() throws Exception {
    OpenLineageSpool spool = spool();
    String id = spool.store(event());
    assertNotNull(id);

    CountingEmitter emitter = new CountingEmitter(false);
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            10, OverflowPolicy.DROP, emitter, new LogChannel("spool-fail"), 0, 50, spool);

    // The only event is the spooled one: it fails, stays on disk and goes back to the front.
    awaitTrue("the spooled event should be attempted", () -> emitter.attempts.get() >= 1);
    assertEquals(1, dispatcher.getBacklogSize(), "a failed event is retried, not forgotten");
    dispatcher.close();
    assertEquals(1, spool().getStoredCount(), "a failed delivery must not delete the spool file");
  }

  // A replay used to wait out the queue's poll timeout before every backlog event, which capped it
  // at about four events a second.
  @Test
  void replaysABacklogWithoutWaitingOnTheQueue() throws Exception {
    OpenLineageSpool firstProcess = spool();
    for (int i = 0; i < 200; i++) {
      firstProcess.store(event());
    }
    CountingEmitter emitter = new CountingEmitter(true);
    long start = System.currentTimeMillis();
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            10, OverflowPolicy.DROP, emitter, new LogChannel("spool-fast"), 0, 50, spool());

    awaitTrue("all 200 spooled events should be delivered", () -> emitter.count.get() == 200);
    assertTrue(
        System.currentTimeMillis() - start < 5_000,
        "a replay of 200 events must not take a poll timeout per event");
    dispatcher.close();
  }

  // Events a run emits while older ones still wait on disk are sent after them, in order.
  @Test
  void newEventsQueueBehindTheBacklog() throws Exception {
    OpenLineageSpool firstProcess = spool();
    firstProcess.store(event("old-1"));
    firstProcess.store(event("old-2"));
    BlockingEmitter emitter = new BlockingEmitter(true);
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            10, OverflowPolicy.DROP, emitter, new LogChannel("spool-order"), 0, 50, spool());

    emitter.firstEmitStarted.await(5, TimeUnit.SECONDS);
    dispatcher.enqueueAll(List.of(event("new")));
    emitter.release.countDown();

    awaitTrue("all three events should be delivered", () -> emitter.emitted.size() == 3);
    assertEquals(
        List.of("old-1", "old-2", "new"),
        emitter.emitted.stream().map(e -> e.getJob().getName()).toList());
    dispatcher.close();
  }

  // A long-running server must retry a failed live event itself, not leave it for a restart.
  @Test
  void failedLiveEventIsRetriedByTheSameProcess() throws Exception {
    OpenLineageSpool spool = spool();
    AtomicInteger attempts = new AtomicInteger();
    OpenLineageEmitter failsOnce =
        new OpenLineageEmitter() {
          @Override
          public Result emit(RunEvent event) {
            return attempts.incrementAndGet() > 1 ? Result.SENT : Result.RETRY;
          }

          @Override
          public void close() {}
        };
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            10, OverflowPolicy.DROP, failsOnce, new LogChannel("spool-live"), 0, 50, spool, 100);

    dispatcher.enqueueAll(List.of(event()));

    awaitTrue("the failed event should be sent again", () -> attempts.get() >= 2);
    awaitTrue("the retried event should leave the spool", () -> spool.getStoredCount() == 0);
    dispatcher.close();
  }

  // During an outage the backlog backs off for a minute; a shutdown should not spin its whole drain
  // window waiting for an attempt that will not happen in time.
  @Test
  void drainDoesNotWaitForABacklogThatIsBackingOff() throws Exception {
    OpenLineageSpool spool = spool();
    spool.store(event());
    CountingEmitter emitter = new CountingEmitter(false);
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            10, OverflowPolicy.DROP, emitter, new LogChannel("spool-drain"), 0, 50, spool, 60_000);
    awaitTrue("the spooled event should be attempted", () -> emitter.attempts.get() >= 1);

    long start = System.currentTimeMillis();
    dispatcher.awaitDrain(10_000);

    assertTrue(System.currentTimeMillis() - start < 2_000, "drain should give up at once");
    dispatcher.close();
  }

  // A refused event (a 400) cannot succeed: it leaves the spool instead of heading every replay.
  @Test
  void aRejectedEventLeavesTheSpoolAndDoesNotBlockTheOthers() throws Exception {
    OpenLineageSpool firstProcess = spool();
    firstProcess.store(event("poison"));
    firstProcess.store(event("good"));
    List<String> sent = new CopyOnWriteArrayList<>();
    OpenLineageEmitter refusesPoison =
        new OpenLineageEmitter() {
          @Override
          public Result emit(RunEvent event) {
            if ("poison".equals(event.getJob().getName())) {
              return Result.REJECTED;
            }
            sent.add(event.getJob().getName());
            return Result.SENT;
          }

          @Override
          public void close() {}
        };
    OpenLineageSpool spool = spool();
    OpenLineageAsyncDispatcher dispatcher =
        new OpenLineageAsyncDispatcher(
            10, OverflowPolicy.DROP, refusesPoison, new LogChannel("spool-poison"), 0, 50, spool);

    awaitTrue("the good event should be delivered", () -> sent.contains("good"));
    awaitTrue("both files should be gone", () -> spool.getStoredCount() == 0);
    assertEquals(1, dispatcher.getRejectedCount());
    dispatcher.close();
  }

  /** Emits successfully (or not) and counts. */
  private static final class CountingEmitter implements OpenLineageEmitter {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger count = new AtomicInteger();
    final boolean succeed;

    CountingEmitter(boolean succeed) {
      this.succeed = succeed;
    }

    @Override
    public Result emit(RunEvent event) {
      attempts.incrementAndGet();
      if (succeed) {
        count.incrementAndGet();
      }
      return succeed ? Result.SENT : Result.RETRY;
    }

    @Override
    public void close() {}
  }
}

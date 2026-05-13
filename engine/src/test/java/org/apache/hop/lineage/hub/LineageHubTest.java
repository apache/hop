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

package org.apache.hop.lineage.hub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.spi.ILineageSink;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LineageHubTest {

  @BeforeAll
  static void initLogStore() {
    // The hub's error-log paths (queue overflow, sink failures) go through LogChannel,
    // which requires the central log store to be initialized.
    HopLogStore.init();
  }

  @Test
  void emitWhenDisabledDoesNotDeliver() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(false, 100, 10, 1000L, Set.of());
    List<Integer> sizes = new CopyOnWriteArrayList<>();
    ILineageSink sink = batch -> sizes.add(batch.size());
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    hub.emit(sampleEvent());
    Thread.sleep(200);
    hub.shutdown();
    assertTrue(sizes.isEmpty());
  }

  @Test
  void flushDeliversBatchedEvents() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 2, 5000L, Set.of());
    List<Integer> sizes = new ArrayList<>();
    ILineageSink sink = batch -> sizes.add(batch.size());
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    hub.emit(sampleEvent());
    hub.emit(sampleEvent());
    hub.emit(sampleEvent());
    hub.flush();
    hub.shutdown();
    assertEquals(List.of(2, 1), sizes);
  }

  @Test
  void failingSinkDoesNotPreventOtherSink() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 10, 5000L, Set.of());
    List<List<LineageEvent>> good = new ArrayList<>();
    ILineageSink bad =
        batch -> {
          throw new RuntimeException("sink failure");
        };
    ILineageSink ok = batch -> good.add(new ArrayList<>(batch));
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(bad, ok));
    hub.emit(sampleEvent());
    hub.flush();
    hub.shutdown();
    assertEquals(1, good.size());
    assertEquals(1, good.get(0).size());
  }

  @Test
  void dropWhenQueueFull() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 1, 10, 60_000L, Set.of());
    Semaphore holdDispatch = new Semaphore(0);
    ILineageSink blocking =
        batch -> {
          try {
            holdDispatch.acquire();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(blocking));
    hub.emit(sampleEvent());
    Thread.sleep(150);
    hub.emit(sampleEvent());
    hub.emit(sampleEvent());
    Thread.sleep(50);
    assertTrue(hub.getDroppedEventCount() >= 1);
    holdDispatch.release(20);
    hub.flush();
    hub.shutdown();
    assertTrue(hub.getDroppedEventCount() >= 1);
  }

  @Test
  void initInvokedOnceBeforeFirstBatch() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of());
    AtomicInteger initCount = new AtomicInteger();
    CountDownLatch initialized = new CountDownLatch(1);
    CountDownLatch firstBatch = new CountDownLatch(1);
    ILineageSink sink =
        new ILineageSink() {
          @Override
          public void init(IVariables variables, ILogChannel log) {
            initCount.incrementAndGet();
            initialized.countDown();
          }

          @Override
          public void accept(List<LineageEvent> events) {
            firstBatch.countDown();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    // Lazy: no init before the first emit.
    Thread.sleep(50);
    assertEquals(0, initCount.get());
    hub.emit(sampleEvent());
    assertTrue(initialized.await(2, TimeUnit.SECONDS));
    assertTrue(firstBatch.await(2, TimeUnit.SECONDS));
    hub.emit(sampleEvent());
    hub.emit(sampleEvent());
    hub.flush();
    hub.shutdown();
    assertEquals(1, initCount.get());
  }

  @Test
  void shutdownInvokesSinkShutdownAndIsIdempotent() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of());
    AtomicInteger shutdownCount = new AtomicInteger();
    ILineageSink sink =
        new ILineageSink() {
          @Override
          public void accept(List<LineageEvent> events) {
            // no-op
          }

          @Override
          public void shutdown() {
            shutdownCount.incrementAndGet();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    hub.emit(sampleEvent());
    hub.flush();
    hub.shutdown();
    assertEquals(1, shutdownCount.get());
    // Second call must be a safe no-op; sinks list is cleared after the first shutdown.
    hub.shutdown();
    assertEquals(1, shutdownCount.get());
  }

  @Test
  void flushIsSafeBeforeAnyEmit() {
    // Disabled hub: flush returns without starting a worker or blocking.
    LineageConfiguration disabled = LineageConfiguration.forTesting(false, 100, 10, 50L, Set.of());
    LineageHub.newIsolatedForTesting(disabled, List.of()).flush();
    // Enabled but idle: no events emitted, no worker running — flush still a no-op.
    LineageConfiguration enabledIdle =
        LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of());
    LineageHub.newIsolatedForTesting(enabledIdle, List.of()).flush();
  }

  @Test
  void multipleSinksEachReceiveAllEvents() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of());
    AtomicInteger a = new AtomicInteger();
    AtomicInteger b = new AtomicInteger();
    ILineageSink sa = batch -> a.addAndGet(batch.size());
    ILineageSink sb = batch -> b.addAndGet(batch.size());
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sa, sb));
    for (int i = 0; i < 7; i++) {
      hub.emit(sampleEvent());
    }
    hub.flush();
    hub.shutdown();
    assertEquals(7, a.get());
    assertEquals(7, b.get());
  }

  @Test
  void batchSizeNeverExceedsBatchMax() throws Exception {
    int max = 3;
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, max, 30_000L, Set.of());
    Semaphore hold = new Semaphore(0);
    List<Integer> sizes = new CopyOnWriteArrayList<>();
    ILineageSink sink =
        batch -> {
          sizes.add(batch.size());
          try {
            hold.acquire();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    // First emit triggers the worker; it pops event 1 and blocks in the sink.
    hub.emit(sampleEvent());
    Thread.sleep(150);
    // Queue up the rest while the dispatcher is held; nothing dispatches until release.
    for (int i = 0; i < 10; i++) {
      hub.emit(sampleEvent());
    }
    hold.release(20);
    hub.flush();
    hub.shutdown();
    for (int s : sizes) {
      assertTrue(s <= max, "batch size " + s + " exceeds batchMax " + max);
    }
    int total = sizes.stream().mapToInt(Integer::intValue).sum();
    assertEquals(11, total);
  }

  @Test
  void eventsPreserveFifoOrderAcrossBatches() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 4, 30_000L, Set.of());
    Semaphore hold = new Semaphore(0);
    List<String> delivered = new CopyOnWriteArrayList<>();
    ILineageSink sink =
        batch -> {
          for (LineageEvent e : batch) {
            delivered.add(e.getEventId());
          }
          try {
            hold.acquire();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    List<String> emitted = new ArrayList<>();
    LineageEvent first = sampleEvent();
    emitted.add(first.getEventId());
    hub.emit(first);
    Thread.sleep(150);
    for (int i = 0; i < 9; i++) {
      LineageEvent e = sampleEvent();
      emitted.add(e.getEventId());
      hub.emit(e);
    }
    hold.release(20);
    hub.flush();
    hub.shutdown();
    assertEquals(emitted, delivered);
  }

  @Test
  void concurrentEmitsAllEventsDelivered() throws Exception {
    int threads = 4;
    int perThread = 250;
    LineageConfiguration cfg =
        LineageConfiguration.forTesting(true, threads * perThread + 100, 50, 50L, Set.of());
    AtomicInteger delivered = new AtomicInteger();
    ILineageSink sink = batch -> delivered.addAndGet(batch.size());
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    CountDownLatch start = new CountDownLatch(1);
    Thread[] producers = new Thread[threads];
    for (int i = 0; i < threads; i++) {
      producers[i] =
          new Thread(
              () -> {
                try {
                  start.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
                for (int j = 0; j < perThread; j++) {
                  hub.emit(sampleEvent());
                }
              });
      producers[i].start();
    }
    start.countDown();
    for (Thread t : producers) {
      t.join(5_000);
    }
    hub.flush();
    hub.shutdown();
    assertEquals(threads * perThread, delivered.get());
    assertEquals(0L, hub.getDroppedEventCount());
  }

  @Test
  void sinkInitFailureDoesNotCrashHub() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of());
    CountDownLatch initCalled = new CountDownLatch(1);
    AtomicInteger acceptCount = new AtomicInteger();
    ILineageSink failing =
        new ILineageSink() {
          @Override
          public void init(IVariables variables, ILogChannel log) throws HopException {
            initCalled.countDown();
            throw new HopException("simulated init failure");
          }

          @Override
          public void accept(List<LineageEvent> events) {
            acceptCount.incrementAndGet();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(failing));
    hub.emit(sampleEvent());
    assertTrue(initCalled.await(2, TimeUnit.SECONDS));
    // Brief window for the worker to log the error and exit cleanly.
    Thread.sleep(100);
    hub.shutdown();
    assertEquals(0, acceptCount.get());
  }

  @Test
  void shutdownDiscardsPendingEventsAndUnblocksSink() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 1, 30_000L, Set.of());
    Semaphore hold = new Semaphore(0);
    AtomicInteger acceptCount = new AtomicInteger();
    ILineageSink sink =
        batch -> {
          acceptCount.addAndGet(batch.size());
          try {
            hold.acquire();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    for (int i = 0; i < 4; i++) {
      hub.emit(sampleEvent());
    }
    Thread.sleep(150);
    long start = System.currentTimeMillis();
    hub.shutdown();
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed < 5_000, "shutdown took " + elapsed + "ms");
    assertEquals(1, acceptCount.get());
  }

  @Test
  void failingSinkShutdownDoesNotAffectOtherSinks() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of());
    AtomicInteger okShutdownCount = new AtomicInteger();
    ILineageSink bad =
        new ILineageSink() {
          @Override
          public void accept(List<LineageEvent> events) {
            // no-op
          }

          @Override
          public void shutdown() throws HopException {
            throw new HopException("bad sink shutdown");
          }
        };
    ILineageSink ok =
        new ILineageSink() {
          @Override
          public void accept(List<LineageEvent> events) {
            // no-op
          }

          @Override
          public void shutdown() {
            okShutdownCount.incrementAndGet();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(bad, ok));
    hub.emit(sampleEvent());
    hub.flush();
    hub.shutdown();
    assertEquals(1, okShutdownCount.get());
  }

  @Test
  void processFlushSplitsLargeDrainAcrossBatchMaxSizedBatches() throws Exception {
    int max = 3;
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, max, 30_000L, Set.of());
    Semaphore hold = new Semaphore(0);
    List<Integer> sizes = new CopyOnWriteArrayList<>();
    ILineageSink sink =
        batch -> {
          sizes.add(batch.size());
          try {
            hold.acquire();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    hub.emit(sampleEvent());
    Thread.sleep(150);
    // While the worker is blocked, enqueue a FlushRequest at the head, then events behind it.
    Thread flusher = new Thread(hub::flush);
    flusher.start();
    Thread.sleep(150);
    for (int i = 0; i < 9; i++) {
      hub.emit(sampleEvent());
    }
    hold.release(20);
    flusher.join(5_000);
    hub.shutdown();
    for (int s : sizes) {
      assertTrue(s <= max, "processFlush produced batch of size " + s + " > " + max);
    }
    int total = sizes.stream().mapToInt(Integer::intValue).sum();
    assertEquals(10, total);
  }

  @Test
  void multipleConcurrentFlushesAreAllReleased() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 10, 30_000L, Set.of());
    Semaphore hold = new Semaphore(0);
    ILineageSink sink =
        batch -> {
          try {
            hold.acquire();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    hub.emit(sampleEvent());
    Thread.sleep(150);
    int flushCount = 5;
    CountDownLatch done = new CountDownLatch(flushCount);
    for (int i = 0; i < flushCount; i++) {
      new Thread(
              () -> {
                hub.flush();
                done.countDown();
              })
          .start();
    }
    Thread.sleep(150);
    hold.release(20);
    assertTrue(done.await(5, TimeUnit.SECONDS), "not all flushes returned");
    hub.shutdown();
  }

  @Test
  void hubResumesAfterShutdown() throws Exception {
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of());
    AtomicInteger initCount = new AtomicInteger();
    AtomicInteger delivered = new AtomicInteger();
    ILineageSink sink =
        new ILineageSink() {
          @Override
          public void init(IVariables variables, ILogChannel log) {
            initCount.incrementAndGet();
          }

          @Override
          public void accept(List<LineageEvent> events) {
            delivered.addAndGet(events.size());
          }
        };
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(sink));
    hub.emit(sampleEvent());
    hub.flush();
    hub.shutdown();
    // Reuse the same instance: a fresh worker should spin up and re-initialize the sink.
    hub.emit(sampleEvent());
    hub.flush();
    hub.shutdown();
    assertEquals(2, initCount.get());
    assertEquals(2, delivered.get());
  }

  private static LineageEvent sampleEvent() {
    return LineageEvent.of(LineageEventKind.RUN_LIFECYCLE, LineageContext.empty(), null);
  }
}

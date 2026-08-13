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

import io.openlineage.client.OpenLineage.RunEvent;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hop.core.logging.ILogChannel;

/**
 * Background worker that drains OpenLineage {@link RunEvent}s and posts them via {@link
 * OpenLineageHttpClient}, so the lineage hub dispatch thread is not blocked on HTTP.
 */
final class OpenLineageAsyncDispatcher implements AutoCloseable {

  /** Default ceiling on how long {@link OverflowPolicy#BLOCK} waits for buffer capacity. */
  static final long DEFAULT_ENQUEUE_TIMEOUT_MS = 5_000;

  /** Behaviour when the outbound queue is full. */
  enum OverflowPolicy {
    /**
     * Wait for space, up to {@link #enqueueTimeoutMs}, then drop and count. Lossless while the
     * collector merely lags; bounded so an unreachable collector cannot stall the caller forever.
     */
    BLOCK,
    /** Drop the event and count it immediately (bounded memory, lossy). This is the default. */
    DROP
  }

  private final BlockingQueue<RunEvent> queue;
  private final OpenLineageEmitter emitter;
  private final OverflowPolicy overflowPolicy;
  private final long enqueueTimeoutMs;
  private final ILogChannel log;
  private final AtomicLong droppedCount = new AtomicLong();
  private final AtomicLong sentCount = new AtomicLong();
  private final AtomicLong failedCount = new AtomicLong();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Thread worker;
  private final ScheduledExecutorService metricsScheduler;

  OpenLineageAsyncDispatcher(
      int bufferSize, OverflowPolicy overflowPolicy, OpenLineageEmitter emitter, ILogChannel log) {
    this(bufferSize, overflowPolicy, emitter, log, 0, DEFAULT_ENQUEUE_TIMEOUT_MS);
  }

  OpenLineageAsyncDispatcher(
      int bufferSize,
      OverflowPolicy overflowPolicy,
      OpenLineageEmitter emitter,
      ILogChannel log,
      long metricsIntervalMs) {
    this(bufferSize, overflowPolicy, emitter, log, metricsIntervalMs, DEFAULT_ENQUEUE_TIMEOUT_MS);
  }

  OpenLineageAsyncDispatcher(
      int bufferSize,
      OverflowPolicy overflowPolicy,
      OpenLineageEmitter emitter,
      ILogChannel log,
      long metricsIntervalMs,
      long enqueueTimeoutMs) {
    this.queue = new LinkedBlockingQueue<>(bufferSize);
    this.overflowPolicy = overflowPolicy;
    this.enqueueTimeoutMs = enqueueTimeoutMs;
    this.emitter = emitter;
    this.log = log;
    this.worker = new Thread(this::runWorker, "hop-openlineage-sink");
    this.worker.setDaemon(true);
    this.worker.start();
    this.metricsScheduler = startMetricsScheduler(metricsIntervalMs);
  }

  private ScheduledExecutorService startMetricsScheduler(long intervalMs) {
    if (intervalMs <= 0) {
      return null;
    }
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "hop-openlineage-sink-metrics");
              t.setDaemon(true);
              return t;
            });
    scheduler.scheduleAtFixedRate(this::logMetrics, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    return scheduler;
  }

  private void logMetrics() {
    log.logBasic(
        "OpenLineage sink metrics: sent="
            + sentCount.get()
            + ", failed="
            + failedCount.get()
            + ", dropped="
            + droppedCount.get()
            + ", queued="
            + queue.size());
  }

  /**
   * Enqueues events.
   *
   * <p>The caller is the lineage hub's single dispatcher thread, and the hub only regains that
   * thread — and with it the ability to process the flush marker a finishing pipeline is waiting on
   * — once this returns. An unbounded wait here therefore does not merely delay lineage: it stalls
   * every pipeline and workflow completion for the hub's full flush timeout. So {@link
   * OverflowPolicy#BLOCK} waits at most {@link #enqueueTimeoutMs} for capacity and then drops
   * (counted), and {@link OverflowPolicy#DROP} drops at once.
   */
  void enqueueAll(List<RunEvent> events) {
    for (RunEvent event : events) {
      try {
        boolean queued =
            overflowPolicy == OverflowPolicy.BLOCK
                ? queue.offer(event, enqueueTimeoutMs, TimeUnit.MILLISECONDS)
                : queue.offer(event);
        if (!queued) {
          countDrop();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        droppedCount.incrementAndGet();
        return;
      }
    }
  }

  /**
   * Counts a dropped event and reports the first drop, so an operator sees that lineage is being
   * shed rather than discovering it only in the shutdown summary.
   */
  private void countDrop() {
    if (droppedCount.getAndIncrement() == 0) {
      log.logError(
          "OpenLineage collector is not keeping up; dropping events (buffer of "
              + queue.size()
              + " full). Further drops are counted and reported on shutdown.");
    }
  }

  long getDroppedCount() {
    return droppedCount.get();
  }

  /** Waits for the queue to drain (best effort). */
  void awaitDrain(long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!queue.isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
  }

  private void runWorker() {
    while (running.get() || !queue.isEmpty()) {
      try {
        RunEvent event = queue.poll(250, TimeUnit.MILLISECONDS);
        if (event != null) {
          if (emitter.emit(event)) {
            sentCount.incrementAndGet();
          } else {
            failedCount.incrementAndGet();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  @Override
  public void close() {
    running.set(false);
    if (metricsScheduler != null) {
      metricsScheduler.shutdownNow();
    }
    try {
      worker.join(30_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long dropped = droppedCount.get();
    if (dropped > 0) {
      log.logError("OpenLineage sink dropped " + dropped + " events due to buffer overflow");
    }
    emitter.close();
  }

  int queueSizeForTesting() {
    return queue.size();
  }
}

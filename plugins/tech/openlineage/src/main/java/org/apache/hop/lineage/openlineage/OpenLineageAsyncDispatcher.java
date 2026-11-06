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
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
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
 *
 * <p>With a {@link OpenLineageSpool} the order is "write, then send": every event is persisted
 * before it is queued, so an overflow or an outage costs delivery time rather than lineage. Events
 * the bounded in-memory queue cannot take are moved to a disk-backed backlog that the worker drains
 * when the queue is empty, and anything still on disk at the end of the process is replayed by the
 * next one.
 */
final class OpenLineageAsyncDispatcher implements AutoCloseable {

  /** Default ceiling on how long {@link OverflowPolicy#BLOCK} waits for buffer capacity. */
  static final long DEFAULT_ENQUEUE_TIMEOUT_MS = 5_000;

  /**
   * How long a failed backlog delivery (a spooled event that could not be sent) waits before it is
   * tried again. Backlog events are the ones already on disk, so they are exactly what a dead
   * collector should not be hammered with: one attempt per minute is enough to notice a recovery,
   * and a new process retries them immediately in any case.
   */
  static final long DEFAULT_BACKLOG_RETRY_PAUSE_MS = 60_000;

  /**
   * How long {@link #close()} lets the worker go on delivering what is still queued. The sink has
   * already given it its drain window ({@link #awaitDrain}); this only finishes what is in flight,
   * so a process does not wait a second full window on its way out.
   */
  static final long CLOSE_GRACE_MS = 5_000;

  /**
   * How many waiting events the emitter is handed at once. An emitter that can write several in one
   * request (DataHub) saves a round trip per event, which is what a Pipeline Executor running a
   * pipeline per row needs to keep up.
   */
  static final int MAX_EMIT_BATCH = 50;

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

  /** One outbound event, with the spool file that has to be removed once it is delivered. */
  private record QueuedEvent(RunEvent event, String spoolId) {}

  private final BlockingQueue<QueuedEvent> queue;
  private final OpenLineageEmitter emitter;
  private final OverflowPolicy overflowPolicy;
  private final long enqueueTimeoutMs;
  private final OpenLineageSpool spool;
  private final long backlogRetryPauseMs;
  private final ILogChannel log;
  private final Deque<QueuedEvent> backlog = new ConcurrentLinkedDeque<>();
  private final AtomicLong droppedCount = new AtomicLong();
  private final AtomicLong sentCount = new AtomicLong();
  private final AtomicLong failedCount = new AtomicLong();
  private final AtomicLong rejectedCount = new AtomicLong();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final Thread worker;
  private final ScheduledExecutorService metricsScheduler;
  private volatile long backlogRetryAtMs;

  OpenLineageAsyncDispatcher(
      int bufferSize, OverflowPolicy overflowPolicy, OpenLineageEmitter emitter, ILogChannel log) {
    this(bufferSize, overflowPolicy, emitter, log, 0, DEFAULT_ENQUEUE_TIMEOUT_MS, null);
  }

  OpenLineageAsyncDispatcher(
      int bufferSize,
      OverflowPolicy overflowPolicy,
      OpenLineageEmitter emitter,
      ILogChannel log,
      long metricsIntervalMs) {
    this(
        bufferSize,
        overflowPolicy,
        emitter,
        log,
        metricsIntervalMs,
        DEFAULT_ENQUEUE_TIMEOUT_MS,
        null);
  }

  OpenLineageAsyncDispatcher(
      int bufferSize,
      OverflowPolicy overflowPolicy,
      OpenLineageEmitter emitter,
      ILogChannel log,
      long metricsIntervalMs,
      long enqueueTimeoutMs) {
    this(bufferSize, overflowPolicy, emitter, log, metricsIntervalMs, enqueueTimeoutMs, null);
  }

  OpenLineageAsyncDispatcher(
      int bufferSize,
      OverflowPolicy overflowPolicy,
      OpenLineageEmitter emitter,
      ILogChannel log,
      long metricsIntervalMs,
      long enqueueTimeoutMs,
      OpenLineageSpool spool) {
    this(
        bufferSize,
        overflowPolicy,
        emitter,
        log,
        metricsIntervalMs,
        enqueueTimeoutMs,
        spool,
        DEFAULT_BACKLOG_RETRY_PAUSE_MS);
  }

  OpenLineageAsyncDispatcher(
      int bufferSize,
      OverflowPolicy overflowPolicy,
      OpenLineageEmitter emitter,
      ILogChannel log,
      long metricsIntervalMs,
      long enqueueTimeoutMs,
      OpenLineageSpool spool,
      long backlogRetryPauseMs) {
    this.queue = new LinkedBlockingQueue<>(bufferSize);
    this.overflowPolicy = overflowPolicy;
    this.enqueueTimeoutMs = enqueueTimeoutMs;
    this.emitter = emitter;
    this.spool = spool;
    this.backlogRetryPauseMs = backlogRetryPauseMs;
    this.log = log;
    this.worker = new Thread(this::runWorker, "hop-openlineage-sink");
    this.worker.setDaemon(true);
    this.worker.start();
    this.metricsScheduler = startMetricsScheduler(metricsIntervalMs);
    replaySpoolFromAnEarlierRun();
  }

  /**
   * Hands the events an earlier process left on disk to the backlog. They are already durable, so
   * this only schedules their delivery: the worker keeps up with the in-memory queue first and
   * picks the backlog up when that is empty.
   */
  private void replaySpoolFromAnEarlierRun() {
    if (spool == null) {
      return;
    }
    List<OpenLineageSpool.SpooledEvent> stored = spool.loadAll();
    if (stored.isEmpty()) {
      return;
    }
    for (OpenLineageSpool.SpooledEvent event : stored) {
      backlog.addLast(new QueuedEvent(event.event(), event.id()));
    }
    log.logBasic(
        "Lineage spool: "
            + stored.size()
            + " event(s) left by an earlier run in "
            + spool.getLocation()
            + " will be sent again");
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
    StringBuilder message =
        new StringBuilder("Lineage delivery metrics: sent=")
            .append(sentCount.get())
            .append(", failed=")
            .append(failedCount.get())
            .append(", rejected=")
            .append(rejectedCount.get())
            .append(", dropped=")
            .append(droppedCount.get())
            .append(", queued=")
            .append(queue.size())
            .append(", spooled=")
            .append(spool == null ? "off" : String.valueOf(spool.getStoredCount()));
    if (spool != null) {
      message.append(", backlog=").append(backlog.size());
    }
    log.logBasic(message.toString());
  }

  /**
   * Enqueues events, persisting them first when a spool is configured.
   *
   * <p>The caller is the lineage hub's single dispatcher thread, and the hub only regains that
   * thread — and with it the ability to process the flush marker a finishing pipeline is waiting on
   * — once this returns. An unbounded wait here therefore does not merely delay lineage: it stalls
   * every pipeline and workflow completion for the hub's full flush timeout. So {@link
   * OverflowPolicy#BLOCK} waits at most {@link #enqueueTimeoutMs} for capacity and then drops
   * (counted), and {@link OverflowPolicy#DROP} drops at once. With a spool, "drops" are only the
   * events that could not be written to disk either: an event that is on disk but did not fit the
   * queue waits in the backlog instead.
   */
  void enqueueAll(List<RunEvent> events) {
    for (RunEvent event : events) {
      QueuedEvent queued = new QueuedEvent(event, spool == null ? null : spool.store(event));
      if (queued.spoolId() != null && !backlog.isEmpty()) {
        // Older events are waiting on disk (a replay, an overflow, a failed delivery): queue behind
        // them, so a run's events keep their order.
        backlog.addLast(queued);
        continue;
      }
      if (!offer(queued)) {
        if (queued.spoolId() == null) {
          countDrop();
        } else {
          backlog.addLast(queued);
        }
      }
    }
  }

  private boolean offer(QueuedEvent queued) {
    try {
      return overflowPolicy == OverflowPolicy.BLOCK
          ? queue.offer(queued, enqueueTimeoutMs, TimeUnit.MILLISECONDS)
          : queue.offer(queued);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Counts a dropped event and reports the first drop, so an operator sees that lineage is being
   * shed rather than discovering it only in the shutdown summary.
   */
  private void countDrop() {
    if (droppedCount.getAndIncrement() == 0) {
      log.logError(
          "The lineage collector is not keeping up; dropping events (buffer of "
              + queue.size()
              + " full). Further drops are counted and reported on shutdown.");
    }
  }

  long getDroppedCount() {
    return droppedCount.get();
  }

  long getSentCount() {
    return sentCount.get();
  }

  /** Delivery attempts that failed and will be (or were) tried again. */
  long getFailedCount() {
    return failedCount.get();
  }

  /** Events the collector refused; they are not resent. */
  long getRejectedCount() {
    return rejectedCount.get();
  }

  /** Events waiting in the disk-backed backlog (queue overflow plus earlier runs' spool files). */
  int getBacklogSize() {
    return backlog.size();
  }

  /**
   * Waits for the queue and the disk-backed backlog to drain (best effort). The backlog is included
   * so a short-lived process — a {@code hop-run} that replays what the previous one left — finishes
   * its replay instead of handing every file on to the run after it.
   */
  void awaitDrain(long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline
        && (!queue.isEmpty() || (!backlog.isEmpty() && backlogRetryAtMs < deadline))) {
      // A backlog that is backing off past the deadline will not be sent in time; it stays on
      // disk for the next start, so there is no point in waiting for it.
      Thread.sleep(20);
    }
  }

  private void runWorker() {
    while ((running.get() || !queue.isEmpty()) && !stopped.get()) {
      try {
        // With backlog work waiting, check the queue without blocking: otherwise every backlog
        // event would wait out the poll timeout and a replay would crawl at a few events a second.
        QueuedEvent first = backlogReady() ? queue.poll() : queue.poll(250, TimeUnit.MILLISECONDS);
        List<QueuedEvent> batch = new ArrayList<>();
        boolean fromBacklog = first == null;
        if (first != null) {
          batch.add(first);
          queue.drainTo(batch, MAX_EMIT_BATCH - 1);
        } else {
          QueuedEvent next;
          while (batch.size() < MAX_EMIT_BATCH && (next = pollBacklog()) != null) {
            batch.add(next);
          }
        }
        if (!batch.isEmpty()) {
          deliver(batch, fromBacklog);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  /**
   * The next spooled event to retry, or {@code null} while the queue is the thing to serve, the
   * backlog is empty, or a failed backlog delivery is still backing off.
   */
  private QueuedEvent pollBacklog() {
    return backlogReady() ? backlog.pollFirst() : null;
  }

  private boolean backlogReady() {
    return running.get() && !backlog.isEmpty() && System.currentTimeMillis() >= backlogRetryAtMs;
  }

  private void deliver(List<QueuedEvent> batch, boolean fromBacklog) {
    List<OpenLineageEmitter.Result> results;
    try {
      results = emitter.emitAll(batch.stream().map(QueuedEvent::event).toList());
    } catch (RuntimeException e) {
      log.logError("Lineage event delivery failed unexpectedly: " + e.getMessage(), e);
      results = null;
    }
    for (int i = 0; i < batch.size(); i++) {
      OpenLineageEmitter.Result result =
          results != null && results.size() == batch.size()
              ? results.get(i)
              : OpenLineageEmitter.Result.RETRY;
      settle(batch.get(i), result, fromBacklog);
    }
  }

  private void settle(QueuedEvent queued, OpenLineageEmitter.Result result, boolean fromBacklog) {
    if (result == OpenLineageEmitter.Result.SENT) {
      sentCount.incrementAndGet();
      if (queued.spoolId() != null) {
        spool.delete(queued.spoolId());
      }
      backlogRetryAtMs = 0;
    } else if (result == OpenLineageEmitter.Result.REJECTED) {
      // Refused for what it is: resending can only fail the same way, and keeping it would make
      // every replay start with it.
      rejectedCount.incrementAndGet();
      if (queued.spoolId() != null) {
        spool.delete(queued.spoolId());
      }
    } else {
      failedCount.incrementAndGet();
      if (fromBacklog) {
        // Still on disk, so nothing is lost. It goes to the back of the backlog: an event that
        // keeps failing must not stop the ones behind it, and the pause keeps a dead collector
        // from being hammered with every file in turn.
        backlog.addLast(queued);
        backlogRetryAtMs = System.currentTimeMillis() + backlogRetryPauseMs;
      } else if (queued.spoolId() != null) {
        // A live event that is on disk is retried by this process too, not only by the next one:
        // a long-running server would otherwise keep it until a restart, and a full spool would
        // quietly stop protecting new events.
        backlog.addLast(queued);
        backlogRetryAtMs = System.currentTimeMillis() + backlogRetryPauseMs;
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
      worker.join(CLOSE_GRACE_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    stopped.set(true);
    long notSpooled = queue.stream().filter(queued -> queued.spoolId() == null).count();
    if (notSpooled > 0) {
      log.logError(
          notSpooled
              + " lineage event(s) were still queued when the sink shut down and are lost; set a"
              + " spool folder to keep them for the next start");
    }
    long dropped = droppedCount.get();
    if (dropped > 0) {
      log.logError("Lineage sink dropped " + dropped + " events due to buffer overflow");
    }
    if (spool != null) {
      long remaining = spool.getStoredCount();
      if (remaining > 0) {
        log.logError(
            "Lineage sink shut down with "
                + remaining
                + " event(s) still spooled in "
                + spool.getLocation()
                + "; they will be sent on the next start");
      }
      if (spool.getFullRefusals() > 0 || spool.getWriteFailures() > 0) {
        log.logError(
            "Lineage spool metrics: stored="
                + remaining
                + " event(s)/"
                + spool.getStoredBytes()
                + " bytes, refused (spool full)="
                + spool.getFullRefusals()
                + ", unwritable="
                + spool.getWriteFailures()
                + ", unreadable on replay="
                + spool.getUnreadableFiles());
      }
    }
    emitter.close();
  }

  int queueSizeForTesting() {
    return queue.size();
  }
}

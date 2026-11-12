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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hop.core.logging.ILogChannel;

/**
 * Background worker that delivers OpenLineage {@link RunEvent}s through an {@link
 * OpenLineageEmitter}, so the lineage hub's dispatcher thread never waits on a collector or a disk.
 *
 * <p>The hub thread only puts events in a bounded queue. From there they move, in order, to a list
 * of pending events, and the worker sends from the head of that list. With an {@link
 * OpenLineageSpool}, a thread of its own writes each event to disk on the way, so the spool keeps
 * filling while the worker waits on a collector that does not answer. An event leaves the list only
 * once it is settled (sent or refused), so a run's events reach the collector in the order they
 * were produced, and nothing in flight is invisible to {@link #awaitDrain}.
 *
 * <p>When a delivery fails, the batch stops at that event and the worker pauses before it tries
 * again, doubling the pause up to {@link #DEFAULT_RETRY_PAUSE_MS}: a dead collector costs one
 * attempt per pause, not one per event. Meanwhile new events keep moving off the queue, onto disk
 * with a spool and into memory, up to the buffer size, without one.
 */
final class OpenLineageAsyncDispatcher implements AutoCloseable {

  /** Default ceiling on how long {@link OverflowPolicy#BLOCK} waits for buffer capacity. */
  static final long DEFAULT_ENQUEUE_TIMEOUT_MS = 5_000;

  /** The longest pause between two attempts while the collector cannot be reached. */
  static final long DEFAULT_RETRY_PAUSE_MS = 60_000;

  /** The first pause after a failed delivery; it doubles with every failure in a row. */
  static final long FIRST_RETRY_PAUSE_MS = 1_000;

  /**
   * How long {@link #close()} lets the worker go on delivering. The sink has already given it its
   * drain window ({@link #awaitDrain}); this only finishes what is in flight, so a process does not
   * wait a second full window on its way out.
   */
  static final long CLOSE_GRACE_MS = 5_000;

  /** How long {@link #close()} waits for the worker once it has been interrupted. */
  private static final long INTERRUPT_GRACE_MS = 1_000;

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

  /**
   * An event waiting for delivery: on disk ({@code spoolId}), in memory ({@code event}), or both.
   * An event that is on disk is only kept in memory while few are waiting.
   */
  private record Pending(RunEvent event, String spoolId) {}

  private final BlockingQueue<RunEvent> queue;
  private final int bufferSize;
  private final OpenLineageEmitter emitter;
  private final OverflowPolicy overflowPolicy;
  private final long enqueueTimeoutMs;
  private final OpenLineageSpool spool;
  private final long maxRetryPauseMs;
  private final ILogChannel log;

  /**
   * Appended to by whichever thread takes events off the queue (the spooler, or the worker without
   * a spool); only the worker removes from it, from the head.
   */
  private final Deque<Pending> pending = new ConcurrentLinkedDeque<>();

  /** Entries of {@link #pending} that hold their event in memory. */
  private final AtomicInteger heldInMemory = new AtomicInteger();

  /** Wakes the worker when the spooler adds to an empty {@link #pending}. */
  private final Object newWork = new Object();

  /** Failed deliveries in a row, for the pause before the next one. Worker only. */
  private int failuresInARow;

  /** Events queued or pending and not settled yet. Zero means everything has been delivered. */
  private final AtomicInteger unsettled = new AtomicInteger();

  private final AtomicLong droppedCount = new AtomicLong();
  private final AtomicLong sentCount = new AtomicLong();
  private final AtomicLong failedCount = new AtomicLong();
  private final AtomicLong rejectedCount = new AtomicLong();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Thread worker;

  /** Writes queued events to the spool; {@code null} without a spool. */
  private final Thread spooler;

  private final ScheduledExecutorService scheduler;
  private volatile long retryAtMs;
  private volatile long closeDeadlineMs = Long.MAX_VALUE;
  private volatile boolean noWait;

  /** Whether the worker has put what earlier processes left in the spool in line. */
  private volatile boolean recovered;

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
        DEFAULT_RETRY_PAUSE_MS);
  }

  OpenLineageAsyncDispatcher(
      int bufferSize,
      OverflowPolicy overflowPolicy,
      OpenLineageEmitter emitter,
      ILogChannel log,
      long metricsIntervalMs,
      long enqueueTimeoutMs,
      OpenLineageSpool spool,
      long maxRetryPauseMs) {
    this.queue = new LinkedBlockingQueue<>(bufferSize);
    this.bufferSize = bufferSize;
    this.overflowPolicy = overflowPolicy;
    this.enqueueTimeoutMs = enqueueTimeoutMs;
    this.emitter = emitter;
    this.spool = spool;
    this.maxRetryPauseMs = maxRetryPauseMs;
    this.log = log;
    this.scheduler = startScheduler(metricsIntervalMs);
    this.worker = new Thread(this::runWorker, "hop-openlineage-sink");
    this.worker.setDaemon(true);
    if (spool == null) {
      this.spooler = null;
      this.recovered = true;
    } else {
      this.spooler = new Thread(this::runSpooler, "hop-openlineage-sink-spool");
      this.spooler.setDaemon(true);
      this.spooler.start();
    }
    this.worker.start();
  }

  /**
   * The metrics logger and the spool heartbeat. The heartbeat has a thread of its own: the worker
   * can spend minutes in a single delivery to a collector that does not answer, and a missed
   * heartbeat would let another process take over this one's events.
   */
  private ScheduledExecutorService startScheduler(long metricsIntervalMs) {
    if (metricsIntervalMs <= 0 && spool == null) {
      return null;
    }
    ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "hop-openlineage-sink-metrics");
              t.setDaemon(true);
              return t;
            });
    if (metricsIntervalMs > 0) {
      executor.scheduleAtFixedRate(
          this::logMetrics, metricsIntervalMs, metricsIntervalMs, TimeUnit.MILLISECONDS);
    }
    if (spool != null) {
      executor.scheduleAtFixedRate(
          spool::heartbeat,
          OpenLineageSpool.HEARTBEAT_INTERVAL_MS,
          OpenLineageSpool.HEARTBEAT_INTERVAL_MS,
          TimeUnit.MILLISECONDS);
    }
    return executor;
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
            .append(", pending=")
            .append(pending.size())
            .append(", spooled=")
            .append(spool == null ? "off" : String.valueOf(spool.getStoredCount()));
    log.logBasic(message.toString());
  }

  /**
   * Queues events for delivery.
   *
   * <p>The caller is the lineage hub's single dispatcher thread, and the hub only regains that
   * thread — and with it the ability to process the flush marker a finishing pipeline is waiting on
   * — once this returns. So this does no I/O (the spooler thread writes the spool), and {@link
   * OverflowPolicy#BLOCK} waits at most {@link #enqueueTimeoutMs} per event for capacity and then
   * drops (counted), while {@link OverflowPolicy#DROP} drops at once.
   */
  void enqueueAll(List<RunEvent> events) {
    for (RunEvent event : events) {
      unsettled.incrementAndGet();
      if (!offer(event)) {
        unsettled.decrementAndGet();
        countDrop();
      }
    }
  }

  private boolean offer(RunEvent event) {
    if (!running.get()) {
      return false;
    }
    if (overflowPolicy != OverflowPolicy.BLOCK || noWait) {
      return queue.offer(event);
    }
    try {
      return queue.offer(event, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // The hub interrupts its worker when it shuts down, which can be while this waits: still take
      // the event if there is room, instead of dropping the rest of the run's last batch.
      Thread.currentThread().interrupt();
      return queue.offer(event);
    }
  }

  /**
   * From now on, {@link OverflowPolicy#BLOCK} no longer waits for capacity. The sink calls this
   * when it shuts down, so a hub thread still handing over events cannot hold the shutdown up.
   */
  void stopWaiting() {
    noWait = true;
  }

  /**
   * Counts a dropped event and reports the first drop, so an operator sees that lineage is being
   * shed rather than discovering it only in the shutdown summary.
   */
  private void countDrop() {
    if (droppedCount.getAndIncrement() == 0) {
      log.logError(
          "The lineage collector is not keeping up; dropping events (buffer of "
              + bufferSize
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

  /** Events taken off the queue and not delivered yet, in memory or on disk. */
  int getPendingCount() {
    return pending.size();
  }

  /**
   * Waits until every event handed over so far — including those an earlier process left in the
   * spool — has been delivered, best effort. The spool's backlog is included so a short-lived
   * process (a {@code hop-run}) finishes its replay instead of handing every file on to the next
   * run. It stops waiting as soon as a delivery fails: the collector is down, and a process should
   * not sit out its whole drain window on its way out. What is on disk is sent by the next start.
   */
  void awaitDrain(long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline
        && (!recovered || unsettled.get() > 0)
        && retryAtMs <= System.currentTimeMillis()
        && worker.isAlive()) {
      Thread.sleep(20);
    }
  }

  private void runWorker() {
    try {
      while (hasWork()) {
        if (spooler == null) {
          takeFromQueue();
        }
        long wait = pending.isEmpty() ? 250 : retryAtMs - System.currentTimeMillis();
        if (wait <= 0) {
          deliverNextBatch();
        } else if (spooler == null && canHoldMore()) {
          // Wake up for a new event as well as for the next attempt.
          RunEvent next = queue.poll(Math.min(wait, 250), TimeUnit.MILLISECONDS);
          if (next != null) {
            hold(next);
          }
        } else {
          // Paused after a failure, or waiting for the spooler (or for room in memory).
          synchronized (newWork) {
            long left = pending.isEmpty() ? 250 : retryAtMs - System.currentTimeMillis();
            if (left > 0) {
              newWork.wait(Math.min(left, 250));
            }
          }
        }
      }
    } catch (InterruptedException e) {
      // close() gave up waiting; what is on disk stays there for the next start.
    } catch (RuntimeException e) {
      log.logError("The lineage delivery worker stopped unexpectedly: " + e.getMessage(), e);
    } finally {
      emitter.close();
    }
  }

  /**
   * With a spool: puts what earlier processes left on disk first in line, then writes every queued
   * event to disk and adds it to the pending list. On {@link #close()} it goes on until the queue
   * is empty (or the grace period is over), so a shutdown during an outage hands the queued events
   * on to the next process instead of losing them.
   */
  private void runSpooler() {
    try {
      recoverSpool();
      while (running.get() || (!queue.isEmpty() && System.currentTimeMillis() < closeDeadlineMs)) {
        RunEvent next = queue.poll(250, TimeUnit.MILLISECONDS);
        if (next != null) {
          hold(next);
        }
      }
    } catch (InterruptedException e) {
      // close() gave up waiting for the disk.
    } catch (RuntimeException e) {
      log.logError("The lineage spool writer stopped unexpectedly: " + e.getMessage(), e);
    }
  }

  /**
   * Whether the worker goes on: until {@link #close()}, and after it for as long as there is
   * something to deliver within the close grace period and the collector has not failed.
   */
  private boolean hasWork() {
    if (running.get()) {
      return true;
    }
    long now = System.currentTimeMillis();
    return now < closeDeadlineMs && (!queue.isEmpty() || !pending.isEmpty()) && retryAtMs <= now;
  }

  /** Puts what earlier processes left in the spool at the head of the line, before new events. */
  private void recoverSpool() {
    try {
      for (String id : spool.recover()) {
        unsettled.incrementAndGet();
        pending.addLast(new Pending(null, id));
      }
      signalNewWork();
    } finally {
      recovered = true;
    }
  }

  private void signalNewWork() {
    synchronized (newWork) {
      newWork.notifyAll();
    }
  }

  /** Without a spool: moves what waits in the queue to the pending list, while there is room. */
  private void takeFromQueue() {
    RunEvent event;
    while (canHoldMore() && (event = queue.poll()) != null) {
      hold(event);
    }
  }

  /**
   * Without a spool, the pending list takes events up to the buffer size; a full list leaves new
   * events in the queue, where the overflow policy applies to them.
   */
  private boolean canHoldMore() {
    return heldInMemory.get() < bufferSize;
  }

  /**
   * Adds an event to the pending list, on disk first when there is a spool. An event on disk is
   * kept in memory too while fewer than the buffer size are; an event the spool cannot take (full
   * or failing) is dropped once memory is full as well.
   */
  private void hold(RunEvent event) {
    String spoolId = spool == null ? null : spool.store(event);
    boolean keepInMemory = heldInMemory.get() < bufferSize;
    if (spoolId == null && !keepInMemory) {
      unsettled.decrementAndGet();
      countDrop();
      return;
    }
    if (keepInMemory) {
      heldInMemory.incrementAndGet();
    }
    pending.addLast(new Pending(keepInMemory ? event : null, spoolId));
    if (spooler != null) {
      signalNewWork();
    }
  }

  /**
   * Sends up to {@link #MAX_EMIT_BATCH} events from the head of the pending list and settles them
   * in order. The first one that fails, and every one after it, stays where it is for the next
   * attempt.
   */
  private void deliverNextBatch() {
    List<RunEvent> events = new ArrayList<>();
    Iterator<Pending> it = pending.iterator();
    while (it.hasNext() && events.size() < MAX_EMIT_BATCH) {
      Pending next = it.next();
      RunEvent event = next.event() != null ? next.event() : spool.load(next.spoolId());
      if (event == null) {
        // Unreadable on disk; the spool discarded and counted it.
        it.remove();
        unsettled.decrementAndGet();
        continue;
      }
      events.add(event);
    }
    if (events.isEmpty()) {
      return;
    }
    List<OpenLineageEmitter.Result> results;
    try {
      results = emitter.emitAll(events);
    } catch (RuntimeException e) {
      log.logError("Lineage event delivery failed unexpectedly: " + e.getMessage(), e);
      results = List.of();
    }
    for (int i = 0; i < events.size(); i++) {
      OpenLineageEmitter.Result result =
          i < results.size() ? results.get(i) : OpenLineageEmitter.Result.RETRY;
      if (result == OpenLineageEmitter.Result.RETRY) {
        pauseAfterFailure(events.size() - i);
        return;
      }
      settle(pending.pollFirst(), result);
    }
    failuresInARow = 0;
    retryAtMs = 0;
  }

  private void settle(Pending delivered, OpenLineageEmitter.Result result) {
    if (result == OpenLineageEmitter.Result.SENT) {
      sentCount.incrementAndGet();
    } else {
      // Refused for what it is: resending can only fail the same way, and keeping it would hold up
      // every event behind it.
      rejectedCount.incrementAndGet();
    }
    if (delivered.spoolId() != null) {
      spool.delete(delivered.spoolId());
    }
    if (delivered.event() != null) {
      heldInMemory.decrementAndGet();
    }
    unsettled.decrementAndGet();
  }

  private void pauseAfterFailure(int notDelivered) {
    failuresInARow++;
    long pause =
        Math.min(maxRetryPauseMs, FIRST_RETRY_PAUSE_MS << Math.min(failuresInARow - 1, 16));
    retryAtMs = System.currentTimeMillis() + pause;
    failedCount.incrementAndGet();
    if (failuresInARow == 1) {
      log.logError(
          "Lineage delivery failed; "
              + pending.size()
              + " event(s) wait and are sent again, in order, once the collector answers"
              + (spool == null ? "" : " (they are kept in " + spool.getLocation() + ")"));
    } else if (log.isDetailed()) {
      log.logDetailed(
          "Lineage delivery failed again ("
              + notDelivered
              + " event(s) of this batch not delivered); next attempt in "
              + pause
              + " ms");
    }
  }

  @Override
  public void close() {
    if (!running.getAndSet(false)) {
      return;
    }
    closeDeadlineMs = System.currentTimeMillis() + CLOSE_GRACE_MS;
    noWait = true;
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    stop(spooler);
    // Stuck in a delivery: an interrupt ends the client's retry backoff.
    stop(worker);
    if (worker.isAlive() || (spooler != null && spooler.isAlive())) {
      // The worker closes the emitter itself once the request it is in returns; closing it here
      // would pull the HTTP client out from under that request.
      log.logError(
          "The lineage collector did not answer in time; "
              + unsettled.get()
              + " event(s) were not delivered when the sink shut down"
              + (spool == null ? " and are lost" : "; those on disk are sent on the next start"));
    } else {
      reportWhatIsLeft();
    }
    long dropped = droppedCount.get();
    if (dropped > 0) {
      log.logError("Lineage sink dropped " + dropped + " events due to buffer overflow");
    }
    if (spool != null) {
      if (spool.getFullRefusals() > 0 || spool.getWriteFailures() > 0) {
        log.logError(
            "Lineage spool metrics: stored="
                + spool.getStoredCount()
                + " event(s)/"
                + spool.getStoredBytes()
                + " bytes, refused (spool full)="
                + spool.getFullRefusals()
                + ", unwritable="
                + spool.getWriteFailures()
                + ", unreadable on replay="
                + spool.getUnreadableFiles());
      }
      if (!worker.isAlive() && !spooler.isAlive()) {
        spool.release();
      }
    }
  }

  /** Waits for a thread until the close deadline, then interrupts it and waits a little more. */
  private void stop(Thread thread) {
    if (thread == null) {
      return;
    }
    try {
      thread.join(Math.max(1, closeDeadlineMs - System.currentTimeMillis()));
      if (thread.isAlive()) {
        thread.interrupt();
        thread.join(INTERRUPT_GRACE_MS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Reports, once the worker has stopped, what was not delivered and where it is. */
  private void reportWhatIsLeft() {
    long notSpooled =
        queue.size() + pending.stream().filter(entry -> entry.spoolId() == null).count();
    if (notSpooled > 0) {
      log.logError(
          notSpooled
              + " lineage event(s) were not delivered when the sink shut down and are lost"
              + (spool == null ? "; set a spool folder to keep them for the next start" : ""));
    }
    if (spool != null && spool.getStoredCount() > 0) {
      log.logError(
          "Lineage sink shut down with "
              + spool.getStoredCount()
              + " event(s) still spooled in "
              + spool.getLocation()
              + "; they will be sent on the next start");
    }
  }

  int queueSizeForTesting() {
    return queue.size();
  }
}

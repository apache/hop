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

package org.apache.hop.vfs.gs;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;

/**
 * Reports a {@code gs://} read or write that stopped making progress, and ends a read that stays
 * stuck.
 *
 * <p>The storage client can keep a single read call busy indefinitely: it re-opens a stalled
 * download over and over inside one call, ignoring the configured attempts and total timeout, so
 * the code calling it learns nothing. Retries are logged by {@link LoggingStorageRetryStrategy},
 * but a transfer that does not move is only visible from outside the call. Each read or write marks
 * itself busy while it is inside the client, and a single background thread looks at those every so
 * often: a call that has been busy for longer than the warning period is logged, again after every
 * further period, and once more if it gets going again.
 *
 * <p>A read that is still stuck after the time the retry settings allow is interrupted. The client
 * does not retry an interrupted read, so the call ends and the stream fails with a clear error.
 * Writes are never interrupted: the client uploads a whole 16 MB chunk inside one call, so a slow
 * upload cannot be told apart from a stuck one, and is only reported.
 */
final class GoogleStorageStallWatchdog {

  private static final Class<?> PKG = GoogleStorageStallWatchdog.class;

  private static final GoogleStorageStallWatchdog INSTANCE =
      new GoogleStorageStallWatchdog(
          message -> LogChannel.GENERAL.logBasic("Google Cloud Storage: " + message),
          System::nanoTime,
          Duration.ofSeconds(1),
          daemonInterrupter());

  /** Where a stall is reported. */
  @FunctionalInterface
  interface StallLog {
    void report(String message);
  }

  /** Whether a transfer reads or writes, which is all the messages need to know. */
  enum Direction {
    READING("Reading"),
    WRITING("Writing");

    private final String key;

    Direction(String key) {
      this.key = key;
    }
  }

  /**
   * How long a call may go without progress before it is reported or, for a read, ended.
   *
   * @param readWarning a read busy for this long is reported as stalled
   * @param readAbort a read busy for this long is ended; zero for never
   * @param writeWarning a write busy for this long is reported as slow or stalled
   */
  record Limits(Duration readWarning, Duration readAbort, Duration writeWarning) {

    /** Never report a stall sooner than this, whatever the read timeout. */
    private static final long MINIMUM_WARNING_SECONDS = 60;

    /** The client's upload chunk, sent whole inside one write call. Hop leaves it as it is. */
    private static final long UPLOAD_CHUNK_BYTES = 16L * 1024 * 1024;

    /** Below this an upload is too slow to tell from a stuck one. */
    private static final long SLOWEST_UPLOAD_BYTES_PER_SECOND = 100L * 1024;

    /**
     * Derive the limits from the Google Cloud configuration.
     *
     * <ul>
     *   <li>A read is reported after three read timeouts without progress, at least a minute: a
     *       single timed-out attempt is already reported as a retry.
     *   <li>A read is ended after the time the retry settings allow for one call - every attempt
     *       running into both the connection and the read timeout, plus the delays between them -
     *       and never before it was reported. Until then the client may still give up by itself,
     *       with its own error. A timeout of zero is no timeout at all, so an attempt is not
     *       bounded and neither is that time.
     *   <li>The total timeout caps both: a read is never ended later than that, and when that comes
     *       before the warning, it is reported and ended at the same moment.
     *   <li>A write is reported once one upload chunk has taken longer than it would at the slowest
     *       rate we still call working, and not before a read would be.
     * </ul>
     *
     * @param config the Google Cloud configuration
     * @return the limits
     */
    static Limits from(GoogleCloudConfig config) {
      long readTimeout = timeoutSeconds(config.getReadTimeout());
      long connectTimeout = timeoutSeconds(config.getConnectionTimeout());
      Duration readWarning = Duration.ofSeconds(Math.max(MINIMUM_WARNING_SECONDS, 3 * readTimeout));

      Duration readAbort =
          readTimeout == 0 || connectTimeout == 0
              ? Duration.ZERO
              : retryBudget(config, connectTimeout + readTimeout);
      if (!readAbort.isZero() && readAbort.compareTo(readWarning) < 0) {
        readAbort = readWarning;
      }
      long totalTimeoutMinutes = Const.toLong(config.getTotalTimeout(), 50);
      if (totalTimeoutMinutes > 0) {
        Duration totalTimeout = Duration.ofMinutes(totalTimeoutMinutes);
        if (readAbort.isZero() || readAbort.compareTo(totalTimeout) > 0) {
          readAbort = totalTimeout;
        }
        if (readWarning.compareTo(readAbort) > 0) {
          readWarning = readAbort;
        }
      }

      long chunkSeconds =
          (UPLOAD_CHUNK_BYTES + SLOWEST_UPLOAD_BYTES_PER_SECOND - 1)
              / SLOWEST_UPLOAD_BYTES_PER_SECOND;
      Duration writeWarning = Duration.ofSeconds(Math.max(readWarning.toSeconds(), chunkSeconds));

      return new Limits(readWarning, readAbort, writeWarning);
    }

    /**
     * A configured timeout in seconds, the way the client takes it: none at all, or a negative one,
     * is the 20 second default, and zero is no timeout.
     */
    private static long timeoutSeconds(String configured) {
      long seconds = Const.toInt(configured, 20);
      return seconds < 0 ? 20 : seconds;
    }

    /** Every attempt running into its timeouts, plus the delays between them. */
    private static Duration retryBudget(GoogleCloudConfig config, long attemptSeconds) {
      int attempts = Const.toInt(config.getMaxAttempts(), 6);
      if (attempts <= 0) {
        // Unlimited attempts: only the total timeout bounds them.
        return Duration.ZERO;
      }
      double delay = Const.toLong(config.getInitialRetryDelay(), 1);
      double multiplier = Const.toDouble(config.getRetryDelayMultiplier(), 2.0);
      long maxDelay = Const.toLong(config.getMaxRetryDelay(), 32);
      double delays = 0;
      for (int i = 1; i < attempts; i++) {
        delays += Math.min(delay, maxDelay);
        delay *= multiplier;
      }
      return Duration.ofSeconds(attempts * attemptSeconds + (long) Math.ceil(delays));
    }
  }

  private final StallLog log;
  private final LongSupplier clock;
  private final Duration checkInterval;
  private final Executor interrupter;
  private final Set<Transfer> busy = ConcurrentHashMap.newKeySet();
  private ScheduledExecutorService checker;

  /**
   * @param log where stalls are reported
   * @param clock the time in nanoseconds
   * @param checkInterval how often to look for stalls; null to only look when {@link #check()} is
   *     called
   * @param interrupter runs the interrupts that end stuck reads
   */
  GoogleStorageStallWatchdog(
      StallLog log, LongSupplier clock, Duration checkInterval, Executor interrupter) {
    this.log = log;
    this.clock = clock;
    this.checkInterval = checkInterval;
    this.interrupter = interrupter;
  }

  /**
   * Interrupting a stuck read makes the client close its HTTP stream, and that close can wait for
   * the blocked socket read to time out. That wait must not hold up the checks on every other
   * transfer, so interrupts get threads of their own.
   *
   * @return an executor running each interrupt on a daemon thread
   */
  static Executor daemonInterrupter() {
    return Executors.newCachedThreadPool(
        r -> {
          Thread thread = new Thread(r, "Google Cloud Storage stall watchdog interrupt");
          thread.setDaemon(true);
          return thread;
        });
  }

  static GoogleStorageStallWatchdog getInstance() {
    return INSTANCE;
  }

  /**
   * @param uri the object being read
   * @param limits when to report the read, and when to end it
   * @return a transfer to mark the reads with
   */
  Transfer reading(String uri, Limits limits) {
    return new Transfer(this, Direction.READING, uri, limits.readWarning(), limits.readAbort());
  }

  /**
   * @param uri the object being written
   * @param limits when to report the write
   * @return a transfer to mark the writes with
   */
  Transfer writing(String uri, Limits limits) {
    return new Transfer(this, Direction.WRITING, uri, limits.writeWarning(), Duration.ZERO);
  }

  /** Report every busy transfer that has gone without progress for too long, and end reads. */
  void check() {
    long now = clock.getAsLong();
    for (Transfer transfer : busy) {
      transfer.checkAt(now);
    }
  }

  /** Stop looking for stalls. Only the tests have a reason to. */
  synchronized void shutdown() {
    if (checker != null) {
      checker.shutdownNow();
      checker = null;
    }
  }

  private synchronized void startChecking() {
    if (checker != null || checkInterval == null) {
      return;
    }
    checker =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "Google Cloud Storage stall watchdog");
              thread.setDaemon(true);
              return thread;
            });
    long millis = checkInterval.toMillis();
    checker.scheduleWithFixedDelay(this::checkQuietly, millis, millis, TimeUnit.MILLISECONDS);
  }

  /** An exception escaping a scheduled task silently cancels it, so none may. */
  private void checkQuietly() {
    try {
      check();
    } catch (RuntimeException e) {
      // Keep watching; the next round may well succeed.
    }
  }

  private void report(String message) {
    try {
      log.report(message);
    } catch (RuntimeException e) {
      // Failing to log a stall must not fail the transfer or stop the watchdog.
    }
  }

  /**
   * One object being read or written. The stream marks each call into the client with {@link
   * #begin()}, and with {@link #end(long)} or {@link #failed()} from a {@code finally}. Calls may
   * overlap - a stream's {@code close()} sends the last upload chunk while a {@code write()} may
   * still be inside the client - and the transfer stays busy until the last of them is done.
   */
  static final class Transfer {
    private final GoogleStorageStallWatchdog watchdog;
    private final Direction direction;
    private final String uri;
    private final long warnAfterNanos;
    private final long abortAfterNanos;

    /**
     * Held while an interrupt is delivered and while {@link #end(long)} lets go of the thread, so
     * that an interrupt never reaches a thread that has moved on. Taken before the transfer's own
     * lock, and never by the checks: delivering an interrupt can take as long as a read timeout.
     */
    private final Object interruptLock = new Object();

    private long bytes;

    /** The threads inside the client right now, one entry per call. Busy while not empty. */
    private final List<Thread> threads = new ArrayList<>();

    private long busySince;
    private long nextReport;
    private boolean stalled;
    private String abortMessage;
    private boolean interrupting;

    private Transfer(
        GoogleStorageStallWatchdog watchdog,
        Direction direction,
        String uri,
        Duration warnAfter,
        Duration abortAfter) {
      this.watchdog = watchdog;
      this.direction = direction;
      this.uri = uri;
      this.warnAfterNanos = warnAfter == null ? 0 : warnAfter.toNanos();
      this.abortAfterNanos = abortAfter == null ? 0 : abortAfter.toNanos();
    }

    /**
     * A transfer that is never reported or ended, for a stream that does not know what it moves.
     *
     * @param uri the object, named in the retry log; may be null
     * @return a transfer that does nothing
     */
    static Transfer untracked(String uri) {
      return new Transfer(null, Direction.READING, uri, null, null);
    }

    /**
     * @return the object being transferred, or null when unknown
     */
    String uri() {
      return uri;
    }

    /**
     * A call into the client is about to start on the current thread. While another call is already
     * inside the client, the time without progress keeps counting from when that one began.
     */
    void begin() {
      if (watchdog == null) {
        return;
      }
      synchronized (this) {
        if (threads.isEmpty()) {
          busySince = watchdog.clock.getAsLong();
          nextReport = busySince + warnAfterNanos;
          stalled = false;
          abortMessage = null;
          interrupting = false;
          watchdog.busy.add(this);
        }
        threads.add(Thread.currentThread());
      }
      watchdog.startChecking();
    }

    /**
     * The call into the client on the current thread returned. When the watchdog ended it, the
     * interrupt it used is cleared here, so it does not leak into whatever the thread does next.
     *
     * @param transferred the number of bytes it moved
     * @return true when the watchdog ended the call; {@link #abortedError(Throwable)} says why
     */
    boolean end(long transferred) {
      return leave(transferred, true);
    }

    /**
     * The call into the client on the current thread threw. A transfer that was reported as stalled
     * is not reported as resumed: the error says what happened.
     *
     * @return true when the watchdog ended the call; {@link #abortedError(Throwable)} says why
     */
    boolean failed() {
      return leave(0, false);
    }

    private boolean leave(long transferred, boolean returned) {
      if (watchdog == null) {
        return false;
      }
      String resumed = null;
      boolean aborted;
      synchronized (interruptLock) {
        synchronized (this) {
          bytes += Math.max(0, transferred);
          threads.remove(Thread.currentThread());
          aborted = abortMessage != null;
          if (aborted) {
            Thread.interrupted();
          }
          if (threads.isEmpty()) {
            watchdog.busy.remove(this);
            if (!aborted && stalled && returned) {
              resumed =
                  BaseMessages.getString(
                      PKG,
                      "GoogleStorageStallWatchdog.Resumed." + direction.key,
                      uri,
                      seconds(watchdog.clock.getAsLong() - busySince));
            }
            stalled = false;
          }
        }
      }
      if (resumed != null) {
        watchdog.report(resumed);
      }
      return aborted;
    }

    /**
     * @param cause what the client threw when it was interrupted
     * @return the error to fail the stream with after the watchdog ended a call
     */
    synchronized IOException abortedError(Throwable cause) {
      return new IOException(abortMessage, cause);
    }

    private void checkAt(long now) {
      String message = null;
      boolean interrupt = false;
      synchronized (this) {
        // The watchdog may still be holding this transfer from before it was removed.
        if (threads.isEmpty()) {
          return;
        }
        if (abortAfterNanos > 0 && now - busySince >= abortAfterNanos) {
          if (abortMessage == null) {
            abortMessage =
                BaseMessages.getString(
                    PKG,
                    "GoogleStorageStallWatchdog.Aborted." + direction.key,
                    uri,
                    seconds(now - busySince));
            message = abortMessage;
          }
          // Again on every round until the call ends: the client may swallow one, for example
          // in the sleep between two attempts. One at a time, as an interrupt can take a while.
          if (!interrupting) {
            interrupting = true;
            interrupt = true;
          }
        } else if (now >= nextReport) {
          nextReport = now + warnAfterNanos;
          stalled = true;
          message =
              BaseMessages.getString(
                  PKG,
                  "GoogleStorageStallWatchdog.Stalled." + direction.key,
                  uri,
                  seconds(now - busySince),
                  bytes);
        }
      }
      if (message != null) {
        watchdog.report(message);
      }
      if (interrupt) {
        try {
          watchdog.interrupter.execute(this::interruptIfStillBusy);
        } catch (RejectedExecutionException e) {
          synchronized (this) {
            interrupting = false;
          }
        }
      }
    }

    /**
     * Interrupt the threads, but only while they are still inside the call the watchdog ended:
     * holding the {@link #interruptLock} that {@link #end(long)} needs makes sure an interrupt
     * never reaches a thread that has moved on to something else. The transfer's own lock is let go
     * before the interrupt, so the checks never wait for it.
     */
    private void interruptIfStillBusy() {
      synchronized (interruptLock) {
        List<Thread> targets;
        synchronized (this) {
          targets = abortMessage == null ? List.of() : new ArrayList<>(threads);
        }
        try {
          for (Thread target : targets) {
            target.interrupt();
          }
        } finally {
          synchronized (this) {
            interrupting = false;
          }
        }
      }
    }

    private static long seconds(long nanos) {
      return TimeUnit.NANOSECONDS.toSeconds(nanos);
    }
  }
}

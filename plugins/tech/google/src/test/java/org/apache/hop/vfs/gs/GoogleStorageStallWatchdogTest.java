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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Which transfers the watchdog reports or ends, and when - on a clock the test moves by hand. The
 * transfers run on the test thread, so an interrupt from the watchdog lands on the test thread too.
 */
class GoogleStorageStallWatchdogTest {

  private static final String URI = "gs://bucket/file.txt";

  /** Report after a minute, end a read after two and a half, report a write after a minute. */
  private static final GoogleStorageStallWatchdog.Limits LIMITS =
      new GoogleStorageStallWatchdog.Limits(
          Duration.ofSeconds(60), Duration.ofSeconds(150), Duration.ofSeconds(60));

  private long now = TimeUnit.HOURS.toNanos(1);
  private final List<String> logged = new ArrayList<>();
  private final GoogleStorageStallWatchdog watchdog =
      new GoogleStorageStallWatchdog(logged::add, () -> now, null, Runnable::run);

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  @Test
  void aTransferThatKeepsMovingIsNotReported() {
    GoogleStorageStallWatchdog.Transfer transfer = watchdog.reading(URI, LIMITS);

    for (int i = 0; i < 10; i++) {
      transfer.begin();
      advance(59);
      watchdog.check();
      transfer.end(100);
    }

    assertEquals(List.of(), logged);
  }

  @Test
  void aBlockedReadIsReportedOncePerWarningPeriod() {
    GoogleStorageStallWatchdog.Transfer transfer = watchdog.reading(URI, LIMITS);
    transfer.begin();
    transfer.end(512);

    transfer.begin();
    advance(59);
    watchdog.check();
    assertEquals(List.of(), logged, "not yet");

    advance(1);
    watchdog.check();
    advance(1);
    watchdog.check();
    assertEquals(
        List.of(
            "No progress reading "
                + URI
                + " for 60 seconds (512 bytes read so far), still waiting."),
        logged);

    advance(59);
    watchdog.check();
    assertEquals(2, logged.size());
    assertEquals(
        "No progress reading " + URI + " for 120 seconds (512 bytes read so far), still waiting.",
        logged.get(1));
    assertFalse(Thread.currentThread().isInterrupted(), "reported, not ended");
  }

  @Test
  void aReadStuckForTheAbortPeriodIsEnded() {
    GoogleStorageStallWatchdog.Transfer transfer = watchdog.reading(URI, LIMITS);
    transfer.begin();
    advance(149);
    watchdog.check();
    assertFalse(Thread.currentThread().isInterrupted(), "not yet");

    advance(1);
    watchdog.check();

    assertTrue(Thread.currentThread().isInterrupted(), "the reading thread is interrupted");
    String aborted =
        "No progress reading "
            + URI
            + " for 150 seconds, giving up: that is as long as the Google Cloud retry settings"
            + " allow for one read.";
    assertEquals(aborted, logged.get(logged.size() - 1));

    assertTrue(transfer.end(0), "the call reports that it was ended");
    assertFalse(
        Thread.currentThread().isInterrupted(), "the interrupt must not outlive the call it ended");
    IOException error = transfer.abortedError(new IOException("closed by interrupt"));
    assertEquals(aborted, error.getMessage());
  }

  /** The client can swallow an interrupt, in the sleep between two attempts for example. */
  @Test
  void anEndedReadIsInterruptedAgainUntilItStops() {
    GoogleStorageStallWatchdog.Transfer transfer = watchdog.reading(URI, LIMITS);
    transfer.begin();
    advance(150);
    watchdog.check();
    assertTrue(Thread.interrupted(), "interrupted, and the client swallows it");

    advance(1);
    watchdog.check();
    assertTrue(Thread.currentThread().isInterrupted(), "interrupted again");
    assertEquals(
        1, logged.stream().filter(line -> line.contains("giving up")).count(), "said only once");

    assertTrue(transfer.end(0));
  }

  @Test
  void aWriteIsReportedAsSlowOrStalledButNeverEnded() {
    GoogleStorageStallWatchdog.Transfer transfer = watchdog.writing(URI, LIMITS);
    transfer.begin();
    advance(3600);
    watchdog.check();

    assertFalse(Thread.currentThread().isInterrupted(), "a write is never interrupted");
    assertEquals(
        List.of(
            "Writing "
                + URI
                + " has not finished an upload chunk for 3600 seconds (0 bytes written so far):"
                + " the connection is slow or stalled."),
        logged);
    assertFalse(transfer.end(65536));
  }

  @Test
  void aSlowWriteThatFinishesSaysSo() {
    GoogleStorageStallWatchdog.Transfer transfer = watchdog.writing(URI, LIMITS);
    transfer.begin();
    advance(60);
    watchdog.check();
    advance(15);
    transfer.end(65536);

    assertEquals(
        List.of(
            "Writing "
                + URI
                + " has not finished an upload chunk for 60 seconds (0 bytes written so far):"
                + " the connection is slow or stalled.",
            "Writing " + URI + " finished the upload chunk after 75 seconds."),
        logged);

    // A later call that is quick again says nothing.
    transfer.begin();
    advance(1);
    watchdog.check();
    transfer.end(65536);
    assertEquals(2, logged.size());
  }

  @Test
  void aStalledReadThatGetsGoingAgainSaysSo() {
    GoogleStorageStallWatchdog.Transfer transfer = watchdog.reading(URI, LIMITS);
    transfer.begin();
    advance(60);
    watchdog.check();
    advance(15);

    assertFalse(transfer.end(65536));
    assertEquals("Reading " + URI + " resumed after 75 seconds without progress.", logged.get(1));
  }

  /**
   * Backpressure: a transform that cannot hand its rows on stops reading for as long as the next
   * transform needs. That time is spent outside the client, so it is not a stall.
   */
  @Test
  void timeBetweenReadsIsNotAStall() {
    GoogleStorageStallWatchdog.Transfer transfer = watchdog.reading(URI, LIMITS);
    transfer.begin();
    transfer.end(65536);

    advance(3600);
    watchdog.check();

    transfer.begin();
    advance(1);
    watchdog.check();
    transfer.end(65536);

    assertEquals(List.of(), logged);
    assertFalse(Thread.currentThread().isInterrupted());
  }

  /** The watchdog can hold a transfer that has just finished; it must not act on that one. */
  @Test
  void aTransferThatFinishedIsLeftAlone() {
    GoogleStorageStallWatchdog.Transfer transfer = watchdog.reading(URI, LIMITS);
    transfer.begin();
    advance(30);
    transfer.end(10);
    advance(3600);
    watchdog.check();

    assertEquals(List.of(), logged);
    assertFalse(Thread.currentThread().isInterrupted());
  }

  @Test
  void anUntrackedTransferIsNeverReported() {
    GoogleStorageStallWatchdog.Transfer transfer =
        GoogleStorageStallWatchdog.Transfer.untracked(URI);
    transfer.begin();
    advance(3600);
    watchdog.check();

    assertFalse(transfer.end(0));
    assertEquals(List.of(), logged);
    assertEquals(URI, transfer.uri());
  }

  @Test
  void aFailingLogDoesNotStopTheWatchdog() {
    GoogleStorageStallWatchdog failing =
        new GoogleStorageStallWatchdog(
            message -> {
              throw new IllegalStateException("the logging system is not initialized");
            },
            () -> now,
            null,
            Runnable::run);
    GoogleStorageStallWatchdog.Transfer transfer = failing.reading(URI, LIMITS);
    transfer.begin();
    advance(60);

    assertDoesNotThrow(failing::check);
    advance(90);
    assertDoesNotThrow(failing::check);
    assertTrue(Thread.currentThread().isInterrupted(), "a failing log does not stop the abort");
    assertTrue(transfer.end(0));
  }

  /**
   * Interrupting a read blocked in a channel closes that channel on the interrupting thread, and
   * closing a socket can take as long as a read timeout. Meanwhile the other transfers must still
   * be checked.
   */
  @Test
  void aSlowInterruptDoesNotHoldUpTheOtherTransfers() throws Exception {
    CountDownLatch reading = new CountDownLatch(1);
    CountDownLatch closing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    // Like a socket: neither the read nor the close gives way to an interrupt.
    InputStream socket =
        new InputStream() {
          @Override
          public int read() {
            throw new UnsupportedOperationException();
          }

          @Override
          public int read(byte[] b, int off, int len) {
            reading.countDown();
            awaitUninterruptibly(release);
            return -1;
          }

          @Override
          public void close() {
            closing.countDown();
            awaitUninterruptibly(release);
          }
        };
    ReadableByteChannel channel = Channels.newChannel(socket);
    List<String> reported = new CopyOnWriteArrayList<>();
    GoogleStorageStallWatchdog interrupting =
        new GoogleStorageStallWatchdog(
            reported::add, () -> now, null, GoogleStorageStallWatchdog.daemonInterrupter());

    GoogleStorageStallWatchdog.Transfer stuck = interrupting.reading(URI, LIMITS);
    AtomicBoolean ended = new AtomicBoolean();
    Thread reader =
        new Thread(
            () -> {
              stuck.begin();
              try {
                channel.read(ByteBuffer.allocate(16));
              } catch (IOException e) {
                // Closed by the interrupt.
              }
              ended.set(stuck.end(0));
            },
            "stuck reader");
    reader.setDaemon(true);
    reader.start();
    try {
      assertTrue(reading.await(5, TimeUnit.SECONDS), "the read is under way");
      advance(150);
      interrupting.check();
      assertTrue(closing.await(5, TimeUnit.SECONDS), "the interrupt is closing the channel");

      GoogleStorageStallWatchdog.Transfer other =
          interrupting.reading("gs://bucket/other.txt", LIMITS);
      other.begin();
      advance(60);
      assertTimeoutPreemptively(
          Duration.ofSeconds(5), interrupting::check, "the checks wait for the interrupt");
      assertTrue(
          reported.stream().anyMatch(line -> line.startsWith("No progress reading gs://bucket/o")),
          "the other transfer is reported: " + reported);
      assertFalse(other.end(0));
    } finally {
      release.countDown();
    }
    reader.join(5000);
    assertTrue(ended.get(), "the stuck read was ended by the watchdog");
  }

  @Test
  void theDefaultLimits() {
    GoogleStorageStallWatchdog.Limits limits =
        GoogleStorageStallWatchdog.Limits.from(new GoogleCloudConfig());

    assertEquals(Duration.ofSeconds(60), limits.readWarning(), "three read timeouts, at least 60");
    assertEquals(
        Duration.ofSeconds(6 * (20 + 20) + 1 + 2 + 4 + 8 + 16),
        limits.readAbort(),
        "six attempts running into both timeouts, plus the delays between them");
    assertEquals(
        Duration.ofSeconds(164), limits.writeWarning(), "a 16 MB chunk at 100 KB per second");
  }

  @Test
  void theLimitsFollowTheTimeouts() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setReadTimeout("45");

    GoogleStorageStallWatchdog.Limits limits = GoogleStorageStallWatchdog.Limits.from(config);

    assertEquals(Duration.ofSeconds(135), limits.readWarning());
    assertEquals(Duration.ofSeconds(6 * (20 + 45) + 31), limits.readAbort());
    assertEquals(Duration.ofSeconds(164), limits.writeWarning());
  }

  @Test
  void theRetryDelaysAreCappedAtTheMaximumDelay() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setMaxAttempts("4");
    config.setInitialRetryDelay("10");
    config.setMaxRetryDelay("15");

    assertEquals(
        Duration.ofSeconds(4 * 40 + 10 + 15 + 15),
        GoogleStorageStallWatchdog.Limits.from(config).readAbort());
  }

  @Test
  void aReadIsNeverEndedBeforeItWasReported() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setMaxAttempts("1");

    GoogleStorageStallWatchdog.Limits limits = GoogleStorageStallWatchdog.Limits.from(config);

    assertEquals(limits.readWarning(), limits.readAbort());
  }

  @Test
  void theTotalTimeoutCapsTheAbort() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setTotalTimeout("2");
    assertEquals(Duration.ofMinutes(2), GoogleStorageStallWatchdog.Limits.from(config).readAbort());

    config.setTotalTimeout("50");
    config.setMaxAttempts("0");
    assertEquals(
        Duration.ofMinutes(50),
        GoogleStorageStallWatchdog.Limits.from(config).readAbort(),
        "unlimited attempts are bounded by the total timeout alone");
  }

  @Test
  void noTimeoutCountsAsTheDefault() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setReadTimeout("0");
    config.setConnectionTimeout("0");

    assertEquals(
        GoogleStorageStallWatchdog.Limits.from(new GoogleCloudConfig()),
        GoogleStorageStallWatchdog.Limits.from(config));
  }

  private void advance(long seconds) {
    now += TimeUnit.SECONDS.toNanos(seconds);
  }

  /** Wait the way a socket read does: an interrupt does not end it. Gives up after 30 seconds. */
  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (latch.getCount() > 0 && System.nanoTime() < deadline) {
      try {
        latch.await(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}

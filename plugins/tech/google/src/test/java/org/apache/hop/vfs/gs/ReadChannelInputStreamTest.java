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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for the GCS VFS read-path hang.
 *
 * <p>A transform thread parked inside {@code channel.read(...)} (a stalled / trickling socket) must
 * not prevent {@link ReadChannelInputStream#close()} from running on another thread (Hop's
 * stop/teardown path). Previously {@code read()} and {@code close()} shared one monitor, so {@code
 * close()} deadlocked behind the stalled read and the whole pipeline froze uninterruptibly. The fix
 * keeps the channel reference volatile and closes it without contending on the read monitor, so
 * {@code close()} aborts the in-flight read.
 *
 * <p>If these assertions ever fail again the monitor-deadlock has been reintroduced.
 *
 * <p>This only covers Hop's own stream, against a channel that gives way on close. The real GCS
 * client does not: its {@code close()} waits for a read in progress (see {@link
 * GoogleStorageStalledDownloadTest}), so a stuck read is ended by {@link
 * GoogleStorageStallWatchdog} instead.
 */
class ReadChannelInputStreamTest {

  /** Report after a minute, end a read after two and a half. */
  private static final GoogleStorageStallWatchdog.Limits LIMITS =
      new GoogleStorageStallWatchdog.Limits(
          Duration.ofSeconds(60), Duration.ofSeconds(150), Duration.ofSeconds(60));

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  @Test
  void closeBreaksAStuckReadAndDoesNotDeadlock() throws Exception {
    CountDownLatch readEntered = new CountDownLatch(1);
    // Released by close() on the mock channel, modelling a real ReadChannel whose in-flight
    // read is aborted (throws) when the channel is closed from another thread.
    CountDownLatch channelClosed = new CountDownLatch(1);

    ReadChannel stuck = mock(ReadChannel.class);
    when(stuck.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              readEntered.countDown();
              channelClosed.await(); // unparked by close() below
              throw new ClosedChannelException(); // real channels fail the read on async close
            });
    // ReadChannel.close() is void; make it release the parked read like the real client does.
    org.mockito.Mockito.doAnswer(
            inv -> {
              channelClosed.countDown();
              return null;
            })
        .when(stuck)
        .close();

    ReadChannelInputStream in = new ReadChannelInputStream(stuck);

    AtomicBoolean readReturned = new AtomicBoolean(false);
    Thread reader =
        new Thread(
            () -> {
              try {
                in.read(new byte[16]);
              } catch (Exception ignored) {
                // ignore
              } finally {
                readReturned.set(true);
              }
            },
            "gcs-stuck-reader");
    reader.setDaemon(true);
    reader.start();

    assertTrue(
        readEntered.await(5, TimeUnit.SECONDS),
        "reader thread should have entered the blocking read()");

    Thread closer = new Thread(in::close, "gcs-closer");
    closer.setDaemon(true);
    closer.start();

    // The fix: close() must return promptly even though a read() is stalled.
    closer.join(3000);
    assertFalse(
        closer.isAlive(),
        "close() is blocked behind a stalled read() — the monitor-deadlock has been reintroduced");

    // ...and closing must break the stalled read so the transform thread is freed too.
    reader.join(3000);
    assertTrue(
        readReturned.get(), "close() did not unblock the stalled read() — pipeline would freeze");
  }

  @Test
  void aReadEndedByTheWatchdogFailsWithItsReason() throws Exception {
    assertEndedReadFails(new ClosedByInterruptException());
  }

  @Test
  void aReadEndedByTheWatchdogFailsWithItsReasonWhenTheClientThrowsARuntimeException()
      throws Exception {
    assertEndedReadFails(new StorageException(0, "interrupted"));
  }

  /**
   * The watchdog ends a stuck read by interrupting it. Whatever the client then throws, the stream
   * fails with the watchdog's reason, on that read and every later one, and the interrupt is gone.
   */
  private static void assertEndedReadFails(Exception clientError) throws Exception {
    long[] now = {0};
    GoogleStorageStallWatchdog watchdog =
        new GoogleStorageStallWatchdog(message -> {}, () -> now[0], null, Runnable::run);
    ReadChannel stuck = mock(ReadChannel.class);
    when(stuck.read(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              now[0] += TimeUnit.SECONDS.toNanos(150);
              watchdog.check(); // interrupts this thread, as the read is stuck for too long
              throw clientError;
            });
    ReadChannelInputStream in =
        new ReadChannelInputStream(stuck, watchdog.reading("gs://bucket/file.txt", LIMITS));

    IOException error = assertThrows(IOException.class, () -> in.read(new byte[16]));
    assertTrue(error.getMessage().contains("giving up"), error.getMessage());
    assertFalse(Thread.currentThread().isInterrupted(), "the interrupt must not outlive the read");

    IOException again = assertThrows(IOException.class, () -> in.read(new byte[16]));
    assertTrue(
        again.getMessage().contains("giving up"), "not end-of-stream: the file is cut short");
  }

  /** An Error from the client used to leave the transfer busy, and its thread interruptible. */
  @Test
  void anErrorFromTheClientStillEndsTheTransfer() throws Exception {
    List<String> logged = new ArrayList<>();
    long[] now = {TimeUnit.HOURS.toNanos(1)};
    GoogleStorageStallWatchdog watchdog =
        new GoogleStorageStallWatchdog(logged::add, () -> now[0], null, Runnable::run);

    ReadChannel broken = mock(ReadChannel.class);
    when(broken.read(any(ByteBuffer.class))).thenThrow(new NoClassDefFoundError("io/grpc/Context"));
    ReadChannelInputStream in =
        new ReadChannelInputStream(broken, watchdog.reading("gs://bucket/file.txt", LIMITS));

    assertThrows(NoClassDefFoundError.class, () -> in.read(new byte[16], 0, 16));

    now[0] += TimeUnit.HOURS.toNanos(1);
    watchdog.check();
    assertTrue(logged.isEmpty(), "no longer watched: " + logged);
    assertFalse(Thread.currentThread().isInterrupted());
  }
}

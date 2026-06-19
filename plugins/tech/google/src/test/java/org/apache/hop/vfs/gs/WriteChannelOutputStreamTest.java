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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.WriteChannel;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Regression guards for the GCS VFS write-path hangs ({@link WriteChannelOutputStream}):
 *
 * <ul>
 *   <li>a stuck {@code channel.write} must not deadlock {@code close()} (same monitor-decoupling
 *       fix as the read path);
 *   <li>the inner write loop must terminate (throw) instead of spinning forever when the channel
 *       makes no progress.
 * </ul>
 *
 * If either assertion fails again the deadlock / infinite spin has been reintroduced.
 */
class WriteChannelOutputStreamTest {

  @Test
  void closeBreaksAStuckWriteAndDoesNotDeadlock() throws Exception {
    CountDownLatch writeEntered = new CountDownLatch(1);
    CountDownLatch channelClosed = new CountDownLatch(1);

    WriteChannel stuck = mock(WriteChannel.class);
    when(stuck.write(any(ByteBuffer.class)))
        .thenAnswer(
            inv -> {
              writeEntered.countDown();
              channelClosed.await();
              throw new ClosedChannelException(); // real channel fails the write on async close
            });
    doAnswer(
            inv -> {
              channelClosed.countDown();
              return null;
            })
        .when(stuck)
        .close();

    WriteChannelOutputStream out = new WriteChannelOutputStream(stuck);

    AtomicBoolean writeReturned = new AtomicBoolean(false);
    Thread writer =
        new Thread(
            () -> {
              try {
                out.write(new byte[32], 0, 32);
              } catch (Exception ignored) {
                // ignore
              } finally {
                writeReturned.set(true);
              }
            },
            "gcs-stuck-writer");
    writer.setDaemon(true);
    writer.start();

    assertTrue(
        writeEntered.await(5, TimeUnit.SECONDS),
        "writer thread should have entered the blocking write()");

    Thread closer =
        new Thread(
            () -> {
              try {
                out.close();
              } catch (Exception ignored) {
                // ignore
              }
            },
            "gcs-closer");
    closer.setDaemon(true);
    closer.start();

    closer.join(3000);
    assertFalse(
        closer.isAlive(),
        "close() is blocked behind a stalled write() — the monitor-deadlock has been reintroduced");

    writer.join(3000);
    assertTrue(
        writeReturned.get(), "close() did not unblock the stalled write() — pipeline would freeze");
  }

  @Test
  void writeLoopTerminatesWhenChannelMakesNoProgress() throws Exception {
    WriteChannel noProgress = mock(WriteChannel.class);
    when(noProgress.write(any(ByteBuffer.class))).thenReturn(0); // never accepts any byte

    WriteChannelOutputStream out = new WriteChannelOutputStream(noProgress);
    AtomicBoolean returned = new AtomicBoolean(false);

    Thread writer =
        new Thread(
            () -> {
              try {
                out.write(new byte[8], 0, 8);
              } catch (Exception ignored) {
                // expected: the progress guard throws IOException instead of spinning forever
              } finally {
                returned.set(true);
              }
            },
            "gcs-no-progress-writer");
    writer.setDaemon(true);
    writer.start();

    writer.join(3000);
    assertTrue(
        returned.get(),
        "write() never returned despite zero channel progress — the infinite spin loop has been "
            + "reintroduced");
  }
}

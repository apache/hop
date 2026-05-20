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
 */
package org.apache.hop.vfs.s3.s3common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Characterises the hang behaviour of the S3 VFS read wrapper ({@link S3CommonFileInputStream},
 * which extends commons-vfs {@code MonitorInputStream}).
 *
 * <p>Two distinct properties matter for the "network hiccup hangs the pipeline" assumption:
 *
 * <ul>
 *   <li><b>Hang property (shared by every VFS read wrapper):</b> a stalled socket makes {@code
 *       read()} block indefinitely with no overall deadline and no response to thread interruption.
 *   <li><b>Deadlock property (GCS-specific):</b> commons-vfs {@code MonitorInputStream} declares
 *       {@code read()} {@code synchronized} but {@code close()} is <em>not</em> synchronised on the
 *       same monitor. So unlike the GCS wrapper, {@code close()} here is NOT blocked by a stuck
 *       {@code read()} — Hop's teardown can still run. This test pins that safer behaviour so a
 *       future refactor cannot silently regress it into the GCS-style deadlock.
 * </ul>
 */
class S3CommonFileInputStreamTest {

  /** An InputStream whose reads park until released — models a stalled S3 socket. */
  private static final class StalledInputStream extends InputStream {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    @Override
    public int read() throws IOException {
      entered.countDown();
      // Model a real blocking socket read: it does NOT respond to Thread.interrupt(); it only
      // returns when data/EOF arrives (here: when released).
      boolean interrupted = false;
      while (true) {
        try {
          if (release.await(1, TimeUnit.HOURS)) {
            break;
          }
        } catch (InterruptedException e) {
          interrupted = true; // swallow, keep blocking — like a JDK socket read
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return read();
    }

    @Override
    public void close() {
      // no-op: this stalled stream models a socket that never returns
    }
  }

  @Test
  void readHangsWithNoDeadlineAndIgnoresInterrupt() throws Exception {
    StalledInputStream stalled = new StalledInputStream();
    S3CommonFileInputStream in = new S3CommonFileInputStream(stalled, () -> {});
    AtomicBoolean returned = new AtomicBoolean(false);

    Thread reader =
        new Thread(
            () -> {
              try {
                in.read(new byte[16]);
              } catch (Exception ignored) {
                // ignore
              } finally {
                returned.set(true);
              }
            },
            "s3-stalled-reader");
    reader.setDaemon(true);
    reader.start();

    assertTrue(stalled.entered.await(5, TimeUnit.SECONDS));
    Thread.sleep(800);
    assertFalse(returned.get(), "read() should still be parked — no overall read deadline exists");

    reader.interrupt();
    Thread.sleep(300);
    assertFalse(returned.get(), "blocking read() does not respond to Thread.interrupt()");

    stalled.release.countDown();
    reader.join(5000);
  }

  /**
   * Safer-than-GCS guarantee: {@code close()} is not synchronised against {@code read()}, so a
   * stuck read does NOT deadlock teardown. If this assertion ever fails, the S3 read path has
   * acquired the GCS-style monitor-deadlock and must be fixed.
   */
  @Test
  void closeIsNotBlockedByAStuckRead() throws Exception {
    StalledInputStream stalled = new StalledInputStream();
    S3CommonFileInputStream in = new S3CommonFileInputStream(stalled, () -> {});

    Thread reader =
        new Thread(
            () -> {
              try {
                in.read(new byte[16]);
              } catch (Exception ignored) {
                // ignore
              }
            },
            "s3-stalled-reader");
    reader.setDaemon(true);
    reader.start();
    assertTrue(stalled.entered.await(5, TimeUnit.SECONDS));

    Thread closer =
        new Thread(
            () -> {
              try {
                in.close();
              } catch (IOException ignored) {
                // ignore
              }
            },
            "s3-closer");
    closer.setDaemon(true);
    closer.start();

    try {
      closer.join(3000);
      assertFalse(
          closer.isAlive(),
          "close() is blocked by a stuck read() — S3 read path regressed into the GCS-style "
              + "monitor-deadlock");
    } finally {
      stalled.release.countDown();
      reader.join(5000);
      closer.join(5000);
    }
  }
}

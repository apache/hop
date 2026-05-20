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

package org.apache.hop.vfs.minio.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hop.vfs.minio.MinioFileSystem;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for the MinIO VFS write-path forever-hang — same fix shape as {@code
 * S3CommonPipedOutputStream}: {@link MinioPipedOutputStream#close()} now joins the uploader thread
 * interruptibly and surfaces upload failures instead of polling forever and swallowing them.
 */
class MinioPipedOutputStreamTest {

  @Test
  void closeIsInterruptibleWhenPutObjectStalls() throws Exception {
    CountDownLatch putEntered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1); // models a stalled MinIO endpoint

    MinioClient client = mock(MinioClient.class);
    when(client.putObject(any(PutObjectArgs.class)))
        .thenAnswer(
            inv -> {
              putEntered.countDown();
              release.await();
              return null;
            });

    MinioFileSystem fileSystem = mock(MinioFileSystem.class);
    when(fileSystem.getClient()).thenReturn(client);
    when(fileSystem.getPartSize()).thenReturn(5L * 1024 * 1024);

    MinioPipedOutputStream out = new MinioPipedOutputStream(fileSystem, "test-bucket", "test-key");

    Thread closer =
        new Thread(
            () -> {
              try {
                out.close();
              } catch (Exception ignored) {
                // InterruptedIOException expected once we interrupt
              }
            },
            "minio-piped-closer");
    closer.setDaemon(true);
    closer.start();

    try {
      assertTrue(putEntered.await(5, TimeUnit.SECONDS), "background putObject should have started");

      closer.join(1500);
      assertTrue(closer.isAlive(), "close() should still be waiting for the in-progress upload");

      closer.interrupt();
      closer.join(3000);
      assertFalse(
          closer.isAlive(),
          "close() did not respond to interruption — the uninterruptible forever-hang has been "
              + "reintroduced");
    } finally {
      release.countDown();
    }
  }

  @Test
  void closeSurfacesAFailedUploadInsteadOfSwallowingIt() throws Exception {
    MinioClient client = mock(MinioClient.class);
    when(client.putObject(any(PutObjectArgs.class)))
        .thenThrow(new RuntimeException("simulated MinIO failure"));

    MinioFileSystem fileSystem = mock(MinioFileSystem.class);
    when(fileSystem.getClient()).thenReturn(client);
    when(fileSystem.getPartSize()).thenReturn(5L * 1024 * 1024);

    MinioPipedOutputStream out = new MinioPipedOutputStream(fileSystem, "test-bucket", "test-key");
    out.write(new byte[] {1, 2, 3});

    assertThrows(
        IOException.class,
        out::close,
        "a failed MinIO upload must be reported from close(), not silently swallowed");
  }
}

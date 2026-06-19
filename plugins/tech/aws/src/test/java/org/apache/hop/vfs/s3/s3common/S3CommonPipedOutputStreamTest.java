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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;

/**
 * Regression guard for the S3 VFS write-path forever-hang.
 *
 * <p>{@link S3CommonPipedOutputStream#close()} used to poll {@code while (!result.isDone())} and
 * swallow interruption, so a stalled multipart upload froze the transform thread forever and could
 * not be stopped. The fix waits on the upload {@code Future} interruptibly and surfaces failures.
 */
class S3CommonPipedOutputStreamTest {

  @Test
  void closeIsInterruptibleWhenUploadStalls() throws Exception {
    CountDownLatch uploadEntered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1); // models a stalled S3 endpoint

    S3Client s3 = mock(S3Client.class);
    when(s3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenAnswer(
            inv -> {
              uploadEntered.countDown();
              release.await();
              return null;
            });

    S3CommonFileSystem fileSystem = mock(S3CommonFileSystem.class);
    when(fileSystem.getS3Client()).thenReturn(s3);

    S3CommonPipedOutputStream out =
        new S3CommonPipedOutputStream(fileSystem, "test-bucket", "test-key");

    Thread closer =
        new Thread(
            () -> {
              try {
                out.close();
              } catch (Exception ignored) {
                // InterruptedIOException expected once we interrupt
              }
            },
            "s3-piped-closer");
    closer.setDaemon(true);
    closer.start();

    try {
      assertTrue(
          uploadEntered.await(5, TimeUnit.SECONDS),
          "background multipart upload should have started");

      // Correct synchronous semantics are preserved: close() keeps waiting while the upload runs.
      closer.join(1500);
      assertTrue(closer.isAlive(), "close() should still be waiting for the in-progress upload");

      // The fix: a pipeline stop interrupts the transform thread and close() must return.
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
    S3Client s3 = mock(S3Client.class);
    when(s3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenThrow(new RuntimeException("simulated S3 failure"));

    S3CommonFileSystem fileSystem = mock(S3CommonFileSystem.class);
    when(fileSystem.getS3Client()).thenReturn(s3);

    S3CommonPipedOutputStream out =
        new S3CommonPipedOutputStream(fileSystem, "test-bucket", "test-key");
    out.write(new byte[] {1, 2, 3});

    assertThrows(
        IOException.class,
        out::close,
        "a failed S3 upload must be reported from close(), not silently swallowed");
  }
}

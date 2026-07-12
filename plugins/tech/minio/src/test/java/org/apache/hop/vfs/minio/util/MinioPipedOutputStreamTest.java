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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hop.vfs.minio.MinioFileSystem;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Regression guard for MinIO VFS writes: content must be uploaded with a known length (no
 * pipe+unknown-size races) and upload failures must surface from {@code close()}.
 */
class MinioPipedOutputStreamTest {

  @Test
  void closeUploadsBufferedContentWithKnownLength() throws Exception {
    MinioClient client = mock(MinioClient.class);
    when(client.putObject(any(PutObjectArgs.class))).thenReturn(null);

    MinioFileSystem fileSystem = mock(MinioFileSystem.class);
    when(fileSystem.getClient()).thenReturn(client);

    MinioPipedOutputStream out =
        new MinioPipedOutputStream(fileSystem, "test-bucket", "path/file.txt");
    byte[] payload = "hello-minio".getBytes();
    out.write(payload);
    out.close();

    ArgumentCaptor<PutObjectArgs> argsCaptor = ArgumentCaptor.forClass(PutObjectArgs.class);
    verify(client).putObject(argsCaptor.capture());
    PutObjectArgs args = argsCaptor.getValue();
    assertEquals("test-bucket", args.bucket());
    assertEquals("path/file.txt", args.object());
    assertEquals(payload.length, args.objectSize());

    try (InputStream stream = args.stream()) {
      assertArrayEquals(payload, stream.readAllBytes());
    }

    verify(fileSystem).invalidateListCacheForParentOf(eq("test-bucket"), eq("path/file.txt"));
  }

  @Test
  void closeWithNoWritesUploadsEmptyObject() throws Exception {
    MinioClient client = mock(MinioClient.class);
    when(client.putObject(any(PutObjectArgs.class))).thenReturn(null);

    MinioFileSystem fileSystem = mock(MinioFileSystem.class);
    when(fileSystem.getClient()).thenReturn(client);

    MinioPipedOutputStream out = new MinioPipedOutputStream(fileSystem, "bucket", "empty.txt");
    out.close();

    ArgumentCaptor<PutObjectArgs> argsCaptor = ArgumentCaptor.forClass(PutObjectArgs.class);
    verify(client).putObject(argsCaptor.capture());
    assertEquals(0L, argsCaptor.getValue().objectSize());
  }

  @Test
  void closeSurfacesAFailedUploadInsteadOfSwallowingIt() throws Exception {
    MinioClient client = mock(MinioClient.class);
    when(client.putObject(any(PutObjectArgs.class)))
        .thenThrow(new RuntimeException("simulated MinIO failure"));

    MinioFileSystem fileSystem = mock(MinioFileSystem.class);
    when(fileSystem.getClient()).thenReturn(client);

    MinioPipedOutputStream out = new MinioPipedOutputStream(fileSystem, "test-bucket", "test-key");
    out.write(new byte[] {1, 2, 3});

    assertThrows(
        IOException.class,
        out::close,
        "a failed MinIO upload must be reported from close(), not silently swallowed");
  }

  @Test
  void writeAfterCloseIsAcceptedAndUploadedOnNextClose() throws Exception {
    // Nested VFS/Text File Output close chains may flush after the first close; late bytes must
    // still be uploaded on a subsequent close().
    MinioClient client = mock(MinioClient.class);
    when(client.putObject(any(PutObjectArgs.class))).thenReturn(null);

    MinioFileSystem fileSystem = mock(MinioFileSystem.class);
    when(fileSystem.getClient()).thenReturn(client);

    MinioPipedOutputStream out = new MinioPipedOutputStream(fileSystem, "test-bucket", "key");
    out.close(); // first close uploads empty

    out.write(new byte[] {1, 2, 3});
    out.close(); // second close uploads the late bytes

    // first empty + second with content
    verify(client, org.mockito.Mockito.times(2)).putObject(any(PutObjectArgs.class));
  }

  @Test
  void closeIsIdempotent() throws Exception {
    MinioClient client = mock(MinioClient.class);
    when(client.putObject(any(PutObjectArgs.class))).thenReturn(null);

    MinioFileSystem fileSystem = mock(MinioFileSystem.class);
    when(fileSystem.getClient()).thenReturn(client);

    MinioPipedOutputStream out = new MinioPipedOutputStream(fileSystem, "test-bucket", "key");
    out.write(new byte[] {9});
    out.close();
    out.close(); // second close must not re-upload or throw

    verify(client).putObject(any(PutObjectArgs.class));
  }

  @Test
  void largeWriteIsFullyUploaded() throws Exception {
    MinioClient client = mock(MinioClient.class);
    when(client.putObject(any(PutObjectArgs.class))).thenReturn(null);

    MinioFileSystem fileSystem = mock(MinioFileSystem.class);
    when(fileSystem.getClient()).thenReturn(client);

    byte[] payload = new byte[256 * 1024];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i % 251);
    }

    MinioPipedOutputStream out = new MinioPipedOutputStream(fileSystem, "test-bucket", "large.bin");
    out.write(payload);
    out.close();

    ArgumentCaptor<PutObjectArgs> argsCaptor = ArgumentCaptor.forClass(PutObjectArgs.class);
    verify(client).putObject(argsCaptor.capture());
    assertEquals(payload.length, argsCaptor.getValue().objectSize());

    try (InputStream stream = argsCaptor.getValue().stream();
        ByteArrayOutputStream readBack = new ByteArrayOutputStream()) {
      stream.transferTo(readBack);
      assertArrayEquals(payload, readBack.toByteArray());
    }
  }
}

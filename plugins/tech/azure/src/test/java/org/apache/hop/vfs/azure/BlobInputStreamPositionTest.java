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

package org.apache.hop.vfs.azure;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.PathProperties;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Position bookkeeping of {@link BlobInputStream}: skip, available and the clamp to the file size
 * must agree with the bytes actually consumed from the underlying stream.
 */
class BlobInputStreamPositionTest {

  private static final int SIZE = 100;

  private static byte[] content(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) i;
    }
    return bytes;
  }

  private static BlobInputStream wrap(InputStream source, long fileSize) {
    DataLakeFileOpenInputStreamResult result = mock(DataLakeFileOpenInputStreamResult.class);
    when(result.getInputStream()).thenReturn(source);
    return new BlobInputStream(result, fileSize);
  }

  private static BlobInputStream wrap(byte[] bytes) {
    return wrap(new ByteArrayInputStream(bytes), bytes.length);
  }

  @Test
  void bulkReadsReturnTheWholeFile() throws IOException {
    byte[] bytes = content(SIZE);
    try (BlobInputStream in = wrap(bytes)) {
      assertArrayEquals(bytes, in.readAllBytes());
    }
  }

  @Test
  void availableShrinksAfterBulkReads() throws IOException {
    try (BlobInputStream in = wrap(content(SIZE))) {
      assertEquals(30, in.read(new byte[30], 0, 30));
      assertEquals(SIZE - 30, in.available());
    }
  }

  @Test
  void skipReturnsTheNumberOfBytesSkipped() throws IOException {
    try (BlobInputStream in = wrap(content(SIZE))) {
      assertEquals(60, in.skip(60));
      assertEquals(SIZE - 60, in.available());
    }
  }

  @Test
  void bulkReadAfterSkipContinuesAtTheNewPosition() throws IOException {
    byte[] bytes = content(SIZE);
    try (BlobInputStream in = wrap(bytes)) {
      in.skip(60);
      assertArrayEquals(Arrays.copyOfRange(bytes, 60, SIZE), in.readAllBytes());
    }
  }

  @Test
  void singleByteReadAfterSkipContinuesAtTheNewPosition() throws IOException {
    try (BlobInputStream in = wrap(content(SIZE))) {
      in.skip(60);
      assertEquals(60, in.read());
    }
  }

  @Test
  void skipPastTheEndStopsAtTheEnd() throws IOException {
    try (BlobInputStream in = wrap(content(SIZE))) {
      assertEquals(40, in.read(new byte[40], 0, 40));
      assertEquals(SIZE - 40, in.skip(1000));
      assertEquals(0, in.available());
      assertEquals(-1, in.read());
    }
  }

  /** The way VFS consumes it: wrapped in a BufferedInputStream (FileContentInputStream). */
  @Test
  void bufferedSkipThenReadReturnsTheRemainingBytes() throws IOException {
    byte[] bytes = content(SIZE);
    try (InputStream in = new BufferedInputStream(wrap(bytes))) {
      in.skipNBytes(60);
      assertArrayEquals(Arrays.copyOfRange(bytes, 60, SIZE), in.readAllBytes());
    }
  }

  /** The clamp exists for blobs padded past their real size; padding must never leak out. */
  @Test
  void paddingBeyondTheFileSizeIsNotReturned() throws IOException {
    byte[] padded = Arrays.copyOf(content(SIZE), 128);
    try (BlobInputStream in = wrap(new ByteArrayInputStream(padded), SIZE)) {
      assertArrayEquals(content(SIZE), in.readAllBytes());
      assertEquals(-1, in.read());
    }
  }

  /** A cached size can be older than the file; the opened stream's own size wins. */
  @Test
  void sizeReportedByTheOpenedStreamWinsOverAStaleSize() throws IOException {
    byte[] bytes = content(SIZE);
    PathProperties properties = mock(PathProperties.class);
    when(properties.getFileSize()).thenReturn((long) SIZE);
    DataLakeFileOpenInputStreamResult result = mock(DataLakeFileOpenInputStreamResult.class);
    when(result.getInputStream()).thenReturn(new ByteArrayInputStream(bytes));
    when(result.getProperties()).thenReturn(properties);
    try (BlobInputStream in = new BlobInputStream(result, 50)) {
      assertArrayEquals(bytes, in.readAllBytes());
    }
  }

  @Test
  void paddingIsNotReturnedAcrossSeveralSmallReads() throws IOException {
    byte[] padded = Arrays.copyOf(content(SIZE), 128);
    try (BlobInputStream in = wrap(new ByteArrayInputStream(padded), SIZE)) {
      byte[] buffer = new byte[32];
      int total = 0;
      int n;
      while ((n = in.read(buffer, 0, buffer.length)) > 0) {
        total += n;
      }
      assertEquals(SIZE, total);
    }
  }
}

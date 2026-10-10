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

package org.apache.commons.vfs2;

import static org.apache.commons.vfs2.ProviderTestFixtures.LARGE_FILE;
import static org.apache.commons.vfs2.ProviderTestFixtures.LARGE_SIZE;
import static org.apache.commons.vfs2.ProviderTestFixtures.largeByte;
import static org.apache.commons.vfs2.ProviderTestFixtures.largeBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.util.Random;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.vfs.IRangeReadableFile;
import org.apache.hop.core.vfs.ReadAheadInputStream;
import org.junit.jupiter.api.Test;

/**
 * Random-access reads of a file larger than any read buffer: seeking forward and back, and opening
 * a stream at the seek position, which is how a reader starts at a byte offset. Skipped for
 * providers that don't declare {@link Capability#RANDOM_ACCESS_READ}.
 */
public class ProviderRangeReadTests extends AbstractProviderTestCase {

  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {Capability.GET_TYPE, Capability.RANDOM_ACCESS_READ};
  }

  private RandomAccessContent open(FileObject file) throws FileSystemException {
    return file.getContent().getRandomAccessContent(RandomAccessMode.READ);
  }

  @Test
  public void testLengthOfLargeFile() throws Exception {
    try (FileObject file = getBaseFolder().resolveFile(LARGE_FILE);
        RandomAccessContent content = open(file)) {
      assertEquals(LARGE_SIZE, content.length());
    }
  }

  @Test
  public void testReadAtFirstMiddleAndLastByte() throws Exception {
    try (FileObject file = getBaseFolder().resolveFile(LARGE_FILE);
        RandomAccessContent content = open(file)) {
      for (long position : new long[] {0, LARGE_SIZE / 2, LARGE_SIZE - 1}) {
        content.seek(position);
        assertEquals(largeByte(position), content.readByte(), "at " + position);
        assertEquals(position + 1, content.getFilePointer());
      }
    }
  }

  @Test
  public void testSeekBackwardsAfterReading() throws Exception {
    try (FileObject file = getBaseFolder().resolveFile(LARGE_FILE);
        RandomAccessContent content = open(file)) {
      byte[] buffer = new byte[64];
      content.seek(1_500_000);
      content.readFully(buffer);
      assertArrayEquals(largeBytes(1_500_000, 1_500_064), buffer);
      content.seek(1000);
      content.readFully(buffer);
      assertArrayEquals(largeBytes(1000, 1064), buffer);
    }
  }

  @Test
  public void testRandomSeeks() throws Exception {
    Random random = new Random(20261010L);
    try (FileObject file = getBaseFolder().resolveFile(LARGE_FILE);
        RandomAccessContent content = open(file)) {
      byte[] buffer = new byte[16];
      for (int i = 0; i < 40; i++) {
        long position = random.nextInt(LARGE_SIZE - buffer.length);
        content.seek(position);
        content.readFully(buffer);
        assertArrayEquals(largeBytes(position, position + buffer.length), buffer, "at " + position);
      }
    }
  }

  @Test
  public void testStreamFromAnOffsetToTheEnd() throws Exception {
    long offset = 1_234_567;
    try (FileObject file = getBaseFolder().resolveFile(LARGE_FILE);
        RandomAccessContent content = open(file)) {
      content.seek(offset);
      try (InputStream in = content.getInputStream()) {
        assertArrayEquals(largeBytes(offset, LARGE_SIZE), in.readAllBytes());
      }
    }
  }

  /**
   * One random-access handle per ranged stream, closed early: closing the stream may close the
   * handle (it does for local files), so a reader opens a new handle for its next range.
   */
  @Test
  public void testCloseRangedStreamsBeforeTheEnd() throws Exception {
    try (FileObject file = getBaseFolder().resolveFile(LARGE_FILE)) {
      for (long offset : new long[] {100, 2_000_000, 100}) {
        try (RandomAccessContent content = open(file)) {
          content.seek(offset);
          try (InputStream in = content.getInputStream()) {
            assertArrayEquals(largeBytes(offset, offset + 10), in.readNBytes(10), "at " + offset);
          }
        }
      }
    }
  }

  /**
   * A byte range read as one piece: exactly its bytes, also at and past the end of the file. Where
   * the provider reads exact ranges ({@link IRangeReadableFile}) the stream ends with the range;
   * otherwise the reader stops after it.
   */
  @Test
  public void testReadExactRanges() throws Exception {
    try (FileObject file = getBaseFolder().resolveFile(LARGE_FILE)) {
      boolean exact = isRangeReadable(file);
      long[][] ranges = {
        {0, 100}, {LARGE_SIZE / 2, 1_000_000}, {LARGE_SIZE - 10, 100}, {LARGE_SIZE, 10}
      };
      for (long[] range : ranges) {
        long offset = range[0];
        long length = range[1];
        byte[] expected = largeBytes(offset, Math.min(LARGE_SIZE, offset + length));
        try (InputStream in = HopVfs.getInputStream(file, offset, length)) {
          byte[] read = exact ? in.readAllBytes() : in.readNBytes((int) length);
          assertArrayEquals(expected, read, "range " + offset + "+" + length);
        }
      }
    }
  }

  /** The whole file in small blocks fetched ahead, several at a time, each by its own request. */
  @Test
  public void testReadAheadInSmallBlocks() throws Exception {
    try (FileObject file = getBaseFolder().resolveFile(LARGE_FILE);
        InputStream in = new ReadAheadInputStream(file, LARGE_SIZE, 1000, LARGE_SIZE, 300_000, 3)) {
      assertArrayEquals(largeBytes(1000, LARGE_SIZE), in.readAllBytes());
    }
  }

  private static boolean isRangeReadable(FileObject file) {
    FileObject undecorated = file;
    while (undecorated instanceof org.apache.commons.vfs2.impl.DecoratedFileObject decorated) {
      undecorated = decorated.getDecoratedFileObject();
    }
    return undecorated instanceof IRangeReadableFile;
  }
}

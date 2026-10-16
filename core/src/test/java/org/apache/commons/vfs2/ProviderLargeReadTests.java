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
import static org.apache.commons.vfs2.ProviderTestFixtures.largeBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import org.junit.jupiter.api.Test;

/**
 * Sequential reads of a file larger than any provider's read buffer: whole-file content, size, and
 * {@code skip()} followed by a read. Skipping is how readers without random access seek.
 */
public class ProviderLargeReadTests extends AbstractProviderTestCase {

  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {Capability.GET_TYPE, Capability.READ_CONTENT};
  }

  private FileObject largeFile() throws FileSystemException {
    return getBaseFolder().resolveFile(LARGE_FILE);
  }

  @Test
  public void testSizeOfLargeFile() throws Exception {
    try (FileObject file = largeFile()) {
      assertEquals(LARGE_SIZE, file.getContent().getSize());
    }
  }

  @Test
  public void testReadWholeLargeFile() throws Exception {
    try (FileObject file = largeFile();
        InputStream in = file.getContent().getInputStream()) {
      assertArrayEquals(largeBytes(0, LARGE_SIZE), in.readAllBytes());
    }
  }

  @Test
  public void testReadInSmallChunks() throws Exception {
    byte[] expected = largeBytes(0, LARGE_SIZE);
    byte[] actual = new byte[LARGE_SIZE];
    byte[] buffer = new byte[1000];
    int total = 0;
    try (FileObject file = largeFile();
        InputStream in = file.getContent().getInputStream()) {
      int n;
      while ((n = in.read(buffer, 0, buffer.length)) > 0) {
        System.arraycopy(buffer, 0, actual, total, n);
        total += n;
      }
    }
    assertEquals(LARGE_SIZE, total);
    assertArrayEquals(expected, actual);
  }

  @Test
  public void testSkipThenReadToTheEnd() throws Exception {
    long offset = LARGE_SIZE / 2 + 11;
    try (FileObject file = largeFile();
        InputStream in = file.getContent().getInputStream()) {
      in.skipNBytes(offset);
      assertArrayEquals(largeBytes(offset, LARGE_SIZE), in.readAllBytes());
    }
  }

  @Test
  public void testSkipSeveralTimesBetweenReads() throws Exception {
    try (FileObject file = largeFile();
        InputStream in = file.getContent().getInputStream()) {
      long position = 0;
      for (long target : new long[] {7, 70_000, 1_100_000, LARGE_SIZE - 100}) {
        in.skipNBytes(target - position);
        assertArrayEquals(largeBytes(target, target + 50), in.readNBytes(50), "at " + target);
        position = target + 50;
      }
    }
  }

  @Test
  public void testCloseBeforeTheEnd() throws Exception {
    try (FileObject file = largeFile()) {
      try (InputStream in = file.getContent().getInputStream()) {
        assertArrayEquals(largeBytes(0, 10), in.readNBytes(10));
      }
      try (InputStream again = file.getContent().getInputStream()) {
        assertArrayEquals(largeBytes(0, 10), again.readNBytes(10));
      }
    }
  }
}

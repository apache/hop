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

package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.vfs2.FileObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** {@link HopVfs#getInputStream(FileObject, long)} and {@link HopVfs#supportsRangeRead}. */
class HopVfsRangeReadTest {

  private static final byte[] DATA = new byte[300_000];

  static {
    for (int i = 0; i < DATA.length; i++) {
      DATA[i] = (byte) ((i * 31) ^ (i >>> 8));
    }
  }

  @TempDir static Path dir;
  private static FileObject local;
  private static FileObject gzip;

  @BeforeAll
  static void files() throws Exception {
    Path plain = Files.write(dir.resolve("data.bin"), DATA);
    Path compressed = dir.resolve("data.bin.gz");
    try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(compressed))) {
      out.write(DATA);
    }
    local = HopVfs.getFileObject(plain.toUri().toString());
    // A layered file system without random access: reads must fall back to skipping
    gzip = HopVfs.getFileObject("gz:" + compressed.toUri() + "!/data.bin");
  }

  private static byte[] readFrom(FileObject file, long offset) throws IOException {
    try (InputStream in = HopVfs.getInputStream(file, offset)) {
      return in.readAllBytes();
    }
  }

  @Test
  void localFilesSupportRangeReads() {
    assertTrue(HopVfs.supportsRangeRead(local));
  }

  @Test
  void compressedFilesDoNotSupportRangeReads() {
    assertFalse(HopVfs.supportsRangeRead(gzip));
  }

  @Test
  void readsALocalFileFromAnOffset() throws IOException {
    assertArrayEquals(Arrays.copyOfRange(DATA, 123_457, DATA.length), readFrom(local, 123_457));
  }

  @Test
  void offsetZeroReadsTheWholeFile() throws IOException {
    assertArrayEquals(DATA, readFrom(local, 0));
  }

  @Test
  void offsetAtOrPastTheEndGivesNothing() throws IOException {
    assertEquals(0, readFrom(local, DATA.length).length);
    assertEquals(0, readFrom(local, DATA.length + 10L).length);
  }

  @Test
  void readsWithoutRandomAccessBySkipping() throws IOException {
    assertArrayEquals(Arrays.copyOfRange(DATA, 200_001, DATA.length), readFrom(gzip, 200_001));
    assertEquals(0, readFrom(gzip, DATA.length + 10L).length);
  }

  @Test
  void closingTheStreamReleasesTheFile() throws IOException {
    for (int i = 0; i < 50; i++) {
      readFrom(local, i * 1000L);
    }
    // Every stream and random-access content was closed, so the file system holds nothing open
    assertFalse(
        ((org.apache.commons.vfs2.provider.AbstractFileSystem) local.getFileSystem()).isOpen());
  }
}

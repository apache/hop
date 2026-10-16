/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** {@link ReadAheadInputStream} and {@link HopVfs#getReadAheadInputStream}. */
class ReadAheadInputStreamTest {

  private static final byte[] DATA = new byte[300_000];

  static {
    for (int i = 0; i < DATA.length; i++) {
      DATA[i] = (byte) ((i * 31) ^ (i >>> 8));
    }
  }

  @TempDir static Path dir;
  private static FileObject local;
  private static FileObject ram;

  @BeforeAll
  static void files() throws Exception {
    local = HopVfs.getFileObject(Files.write(dir.resolve("data.bin"), DATA).toUri().toString());
    ram = HopVfs.getFileObject("ram:///read-ahead/data.bin");
    ram.getParent().createFolder();
    try (OutputStream out = HopVfs.getOutputStream(ram, false)) {
      out.write(DATA);
    }
  }

  private static byte[] read(InputStream in, int readSize) throws IOException {
    try (in) {
      ByteArrayOutputStream all = new ByteArrayOutputStream();
      byte[] buffer = new byte[readSize];
      int n;
      while ((n = in.read(buffer, 0, readSize)) >= 0) {
        all.write(buffer, 0, n);
      }
      return all.toByteArray();
    }
  }

  private static byte[] tail(long offset) {
    return Arrays.copyOfRange(DATA, (int) Math.min(offset, DATA.length), DATA.length);
  }

  /**
   * Every byte from the offset in order, whatever the block size, the number of blocks in flight,
   * how far ahead blocks are fetched and how much each read asks for.
   */
  @ParameterizedTest
  @CsvSource({
    "0, 300000, 65536, 4, 8192",
    "0, 300000, 1000, 3, 1",
    "12345, 300000, 4096, 2, 100000",
    "12345, 50000, 4096, 4, 777",
    "299999, 300000, 4096, 4, 10",
    "0, 0, 4096, 4, 5000",
    "300000, 300000, 4096, 4, 10",
    "0, 300000, 1000000, 4, 4096"
  })
  void readsEveryByteFromTheOffset(
      long offset, long readAheadEnd, int blockSize, int blocks, int readSize) throws Exception {
    InputStream in =
        new ReadAheadInputStream(local, DATA.length, offset, readAheadEnd, blockSize, blocks);

    assertArrayEquals(tail(offset), read(in, readSize));
  }

  @Test
  void readsSingleBytes() throws Exception {
    try (InputStream in = new ReadAheadInputStream(local, DATA.length, 100, 5000, 1000, 2)) {
      for (int i = 100; i < 3000; i++) {
        assertEquals(DATA[i] & 0xFF, in.read());
      }
    }
  }

  /** A reader that stops early leaves nothing running and can't read on. */
  @Test
  void closesEarly() throws Exception {
    InputStream in = new ReadAheadInputStream(local, DATA.length, 0, DATA.length, 1000, 8);
    byte[] first = new byte[10];
    assertEquals(10, in.read(first));
    in.close();
    in.close();

    assertArrayEquals(Arrays.copyOf(DATA, 10), first);
    assertThrows(IOException.class, () -> in.read());
  }

  /** A file shorter than its size said, because it changed, is an error, not silently short. */
  @Test
  void reportsAFileThatShrank() {
    IOException e =
        assertThrows(
            IOException.class,
            () ->
                read(
                    new ReadAheadInputStream(local, DATA.length + 5000, 0, 400_000, 4096, 4),
                    8192));

    assertTrue(e.getMessage().contains("changed while it was read"), e.getMessage());
  }

  @Test
  void refusesNoBlocks() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ReadAheadInputStream(local, DATA.length, 0, DATA.length, 4096, 0));
  }

  /** Local files are read directly: the operating system reads ahead already. */
  @Test
  void readsLocalFilesDirectly() throws Exception {
    try (InputStream in = HopVfs.getReadAheadInputStream(local, 1000, DATA.length, 4)) {
      assertFalse(in instanceof ReadAheadInputStream);
      assertArrayEquals(tail(1000), in.readAllBytes());
    }
  }

  @Test
  void fetchesAheadOnOtherStorage() throws Exception {
    try (InputStream in = HopVfs.getReadAheadInputStream(ram, 1000, DATA.length, 4)) {
      assertTrue(in instanceof ReadAheadInputStream);
      assertArrayEquals(tail(1000), in.readAllBytes());
    }
  }

  @Test
  void readsWithoutFetchingAheadWithZeroBlocks() throws Exception {
    try (InputStream in = HopVfs.getReadAheadInputStream(ram, 1000, DATA.length, 0)) {
      assertFalse(in instanceof ReadAheadInputStream);
      assertArrayEquals(tail(1000), in.readAllBytes());
    }
  }

  /** A file whose storage reads exact ranges, recording each range it was asked for. */
  private static FileObject rangeReadable(int readBlockSize, List<long[]> ranges)
      throws IOException {
    FileObject file =
        mock(FileObject.class, withSettings().extraInterfaces(IRangeReadableFile.class));
    IRangeReadableFile storage = (IRangeReadableFile) file;
    when(storage.getReadBlockSize()).thenReturn(readBlockSize);
    when(storage.getInputStream(anyLong(), anyLong()))
        .thenAnswer(
            call -> {
              long offset = call.getArgument(0);
              long length = call.getArgument(1);
              ranges.add(new long[] {offset, length});
              int from = (int) Math.min(offset, DATA.length);
              int to = (int) Math.min(offset + length, DATA.length);
              return new ByteArrayInputStream(Arrays.copyOfRange(DATA, from, to));
            });
    return file;
  }

  /** Every block is one exact range of the block size, the last one up to the end of the file. */
  @Test
  void readsEveryBlockAsOneExactRange() throws Exception {
    List<long[]> ranges = Collections.synchronizedList(new ArrayList<>());
    FileObject file = rangeReadable(100_000, ranges);

    byte[] read =
        read(new ReadAheadInputStream(file, DATA.length, 50_000, DATA.length, 100_000, 2), 8192);

    assertArrayEquals(tail(50_000), read);
    List<String> asked = new ArrayList<>();
    ranges.forEach(range -> asked.add(range[0] + "+" + range[1]));
    asked.sort(null);
    assertEquals(List.of("150000+100000", "250000+50000", "50000+100000"), asked);
  }

  @Test
  void takesTheReadBlockSizeFromTheStorage() throws Exception {
    assertEquals(4096, HopVfs.getReadBlockSize(rangeReadable(4096, new ArrayList<>())));
    assertEquals(HopVfs.DEFAULT_READ_BLOCK_SIZE, HopVfs.getReadBlockSize(ram));
    assertEquals(HopVfs.DEFAULT_READ_BLOCK_SIZE, HopVfs.getReadBlockSize(local));
  }

  /** Without exact ranges, a range is the stream from its offset: the caller stops after it. */
  @Test
  void readsARangeFromTheOffsetOnOtherStorage() throws Exception {
    try (InputStream in = HopVfs.getInputStream(ram, 1000, 500)) {
      assertArrayEquals(Arrays.copyOfRange(DATA, 1000, 1500), in.readNBytes(500));
    }
  }

  /**
   * Storage that breaks off the download of the block at {@code failingOffset}, halfway, for the
   * first {@code failures} requests of that block. Every request is recorded as "offset+length".
   */
  private static FileObject breakingStorage(long failingOffset, int failures, List<String> requests)
      throws IOException {
    FileObject file =
        mock(FileObject.class, withSettings().extraInterfaces(IRangeReadableFile.class));
    FileName name = mock(FileName.class);
    when(name.getFriendlyURI()).thenReturn("test:///breaking.bin");
    when(file.getName()).thenReturn(name);
    IRangeReadableFile storage = (IRangeReadableFile) file;
    when(storage.getReadBlockSize()).thenReturn(10_000);
    int[] failed = {0};
    when(storage.getInputStream(anyLong(), anyLong()))
        .thenAnswer(
            call -> {
              long offset = call.getArgument(0);
              long length = call.getArgument(1);
              requests.add(offset + "+" + length);
              int from = (int) offset;
              int to = (int) Math.min(offset + length, DATA.length);
              byte[] bytes = Arrays.copyOfRange(DATA, from, to);
              boolean breaks;
              synchronized (failed) {
                breaks = offset == failingOffset && failed[0] < failures;
                if (breaks) {
                  failed[0]++;
                }
              }
              if (!breaks) {
                return new ByteArrayInputStream(bytes);
              }
              // Half the block arrives, then the connection is lost
              return new SequenceInputStream(
                  new ByteArrayInputStream(bytes, 0, bytes.length / 2),
                  new InputStream() {
                    @Override
                    public int read() throws IOException {
                      throw new IOException("Connection reset");
                    }
                  });
            });
    return file;
  }

  /** A block whose download breaks off is downloaded again, and the reader gets every byte. */
  @Test
  void downloadsABrokenBlockAgain() throws Exception {
    List<String> requests = Collections.synchronizedList(new ArrayList<>());
    FileObject file = breakingStorage(20_000, ReadAheadInputStream.MAX_ATTEMPTS - 1, requests);

    byte[] read =
        read(new ReadAheadInputStream(file, DATA.length, 0, DATA.length, 10_000, 3, 0), 4096);

    assertArrayEquals(DATA, read);
    assertEquals(
        ReadAheadInputStream.MAX_ATTEMPTS,
        requests.stream().filter("20000+10000"::equals).count(),
        requests.toString());
  }

  /**
   * A block that still fails after every attempt fails the stream, and keeps failing it: the reader
   * never gets the bytes after the failed block, so no row is made of a file with a hole in it.
   */
  @Test
  void failsForGoodWhenABlockKeepsFailing() throws Exception {
    List<String> requests = Collections.synchronizedList(new ArrayList<>());
    FileObject file = breakingStorage(20_000, Integer.MAX_VALUE, requests);
    InputStream in = new ReadAheadInputStream(file, DATA.length, 0, DATA.length, 10_000, 3, 0);

    byte[] before = in.readNBytes(20_000);
    IOException error = assertThrows(IOException.class, () -> in.read(new byte[100]));
    IOException again = assertThrows(IOException.class, () -> in.read(new byte[100]));
    assertThrows(IOException.class, in::read);
    in.close();

    assertArrayEquals(Arrays.copyOf(DATA, 20_000), before);
    assertTrue(
        error.getMessage().contains("after " + ReadAheadInputStream.MAX_ATTEMPTS),
        error.getMessage());
    assertTrue(error.getMessage().contains("Connection reset"), error.getMessage());
    assertEquals(error.getMessage(), again.getMessage());
    assertEquals(
        ReadAheadInputStream.MAX_ATTEMPTS,
        requests.stream().filter("20000+10000"::equals).count(),
        requests.toString());
  }
}

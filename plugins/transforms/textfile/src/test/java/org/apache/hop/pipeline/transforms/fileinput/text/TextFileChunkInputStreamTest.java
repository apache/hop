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

package org.apache.hop.pipeline.transforms.fileinput.text;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Splitting a file into chunks loses and duplicates nothing: the chunks, read in order and joined,
 * are the file, byte for byte, wherever the cuts fall.
 */
class TextFileChunkInputStreamTest {

  private static final byte[] LF = {0x0A};
  private static final byte[] LF_UTF16LE = {0x0A, 0x00};
  private static final byte[] LF_UTF16BE = {0x00, 0x0A};

  record Sample(String name, byte[] bytes, byte[] lineFeed) {
    @Override
    public String toString() {
      return name;
    }
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] all = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, all, a.length, b.length);
    return all;
  }

  static List<Sample> samples() {
    String text = "1,alpha\n22,beta\n\n333,gamma\n4,δέλτα 日本\n55,\n";
    return List.of(
        new Sample("lf", text.getBytes(StandardCharsets.UTF_8), LF),
        new Sample("crlf", text.replace("\n", "\r\n").getBytes(StandardCharsets.UTF_8), LF),
        new Sample("no final newline", "a\nbb\nccc".getBytes(StandardCharsets.UTF_8), LF),
        new Sample("only newlines", "\n\n\n\n".getBytes(StandardCharsets.UTF_8), LF),
        new Sample("one long line", "x".repeat(40).getBytes(StandardCharsets.UTF_8), LF),
        new Sample(
            "utf-8 bom",
            concat(
                new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF},
                text.getBytes(StandardCharsets.UTF_8)),
            LF),
        new Sample(
            "utf-16le bom",
            concat(new byte[] {(byte) 0xFF, (byte) 0xFE}, text.getBytes(StandardCharsets.UTF_16LE)),
            LF_UTF16LE),
        new Sample(
            "utf-16be bom crlf",
            concat(
                new byte[] {(byte) 0xFE, (byte) 0xFF},
                text.replace("\n", "\r\n").getBytes(StandardCharsets.UTF_16BE)),
            LF_UTF16BE));
  }

  private static byte[] readChunk(byte[] file, long start, long end, byte[] lineFeed, int readSize)
      throws IOException {
    long openedAt = start == 0 ? 0 : start - lineFeed.length;
    InputStream source =
        new ByteArrayInputStream(file, (int) openedAt, file.length - (int) openedAt);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (TextFileChunkInputStream in =
        new TextFileChunkInputStream(source, openedAt, start, end, lineFeed)) {
      byte[] buffer = new byte[readSize];
      int n;
      while ((n = in.read(buffer, 0, buffer.length)) >= 0) {
        out.write(buffer, 0, n);
      }
    }
    return out.toByteArray();
  }

  private static byte[] readAllChunks(byte[] file, List<Long> cuts, byte[] lineFeed, int readSize)
      throws IOException {
    ByteArrayOutputStream joined = new ByteArrayOutputStream();
    long start = 0;
    for (long cut : cuts) {
      joined.write(readChunk(file, start, cut, lineFeed, readSize));
      start = cut;
    }
    joined.write(readChunk(file, start, file.length, lineFeed, readSize));
    return joined.toByteArray();
  }

  @ParameterizedTest
  @MethodSource("samples")
  void twoChunksAtEveryCut(Sample sample) throws IOException {
    int unit = sample.lineFeed().length;
    for (int cut = unit; cut < sample.bytes().length; cut += unit) {
      for (int readSize : new int[] {1, 3, 8192}) {
        assertArrayEquals(
            sample.bytes(),
            readAllChunks(sample.bytes(), List.of((long) cut), sample.lineFeed(), readSize),
            "cut at " + cut + ", reads of " + readSize);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("samples")
  void manyChunksAtRandomCuts(Sample sample) throws IOException {
    int unit = sample.lineFeed().length;
    int units = sample.bytes().length / unit;
    Random random = new Random(sample.name().hashCode());
    for (int round = 0; round < 200; round++) {
      TreeSet<Long> cuts = new TreeSet<>();
      int count = 1 + random.nextInt(Math.max(1, units - 1));
      for (int i = 0; i < count; i++) {
        cuts.add((long) (1 + random.nextInt(Math.max(1, units - 1))) * unit);
      }
      cuts.removeIf(c -> c >= sample.bytes().length);
      assertArrayEquals(
          sample.bytes(),
          readAllChunks(sample.bytes(), new ArrayList<>(cuts), sample.lineFeed(), 8192),
          "cuts " + cuts);
    }
  }

  @ParameterizedTest
  @MethodSource("samples")
  void everyChunkHoldsWholeLines(Sample sample) throws IOException {
    int unit = sample.lineFeed().length;
    for (int cut = unit; cut < sample.bytes().length; cut += unit) {
      byte[] first = readChunk(sample.bytes(), 0, cut, sample.lineFeed(), 8192);
      if (first.length > 0 && first.length < sample.bytes().length) {
        byte[] last = Arrays.copyOfRange(first, first.length - unit, first.length);
        assertArrayEquals(sample.lineFeed(), last, "the first chunk ends with a line feed");
      }
    }
  }

  @org.junit.jupiter.api.Test
  void aChunkStartingRightAfterALineFeedKeepsItsFirstLine() throws IOException {
    byte[] file = "aa\nbb\ncc\n".getBytes(StandardCharsets.UTF_8);
    assertEquals("bb\ncc\n", new String(readChunk(file, 3, 9, LF, 8192), StandardCharsets.UTF_8));
  }

  @org.junit.jupiter.api.Test
  void aChunkStartingInsideALineSkipsIt() throws IOException {
    byte[] file = "aa\nbb\ncc\n".getBytes(StandardCharsets.UTF_8);
    assertEquals("cc\n", new String(readChunk(file, 4, 9, LF, 8192), StandardCharsets.UTF_8));
  }

  @org.junit.jupiter.api.Test
  void aChunkFinishesTheLineCrossingItsEnd() throws IOException {
    byte[] file = "aa\nbb\ncc\n".getBytes(StandardCharsets.UTF_8);
    assertEquals("aa\nbb\n", new String(readChunk(file, 0, 4, LF, 8192), StandardCharsets.UTF_8));
  }
}

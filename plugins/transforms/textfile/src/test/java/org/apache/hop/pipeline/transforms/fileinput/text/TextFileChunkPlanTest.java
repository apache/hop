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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.vfs.IRangeReadableFile;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileRecordSplitter.RecordFormat;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TextFileChunkPlanTest {

  private static final long MB = 1024L * 1024;

  @TempDir Path dir;

  @Test
  void splitSizeIsTheChunkSizeForLotsOfData() {
    assertEquals(128 * MB, TextFileChunkPlan.splitSize(128 * MB, 10_000 * MB, 4));
  }

  @Test
  void splitSizeShrinksSoEveryCopyGetsWork() {
    // 200 MB over 4 copies: 50 MB pieces rather than 128 MB ones
    assertEquals(50 * MB, TextFileChunkPlan.splitSize(128 * MB, 200 * MB, 4));
  }

  @Test
  void splitSizeDoesNotShrinkBelowFourMegabytes() {
    assertEquals(4 * MB, TextFileChunkPlan.splitSize(128 * MB, 6 * MB, 8));
  }

  @Test
  void anExplicitSmallChunkSizeIsKept() {
    assertEquals(1024, TextFileChunkPlan.splitSize(1024, 100 * MB, 4));
  }

  @Test
  void splitSizeIsEven() {
    assertEquals(0, TextFileChunkPlan.splitSize(1001, 100 * MB, 4) % 2);
  }

  @Test
  void parsesChunkSizes() throws HopException {
    assertEquals(TextFileChunkPlan.DEFAULT_CHUNK_SIZE, TextFileChunkPlan.parseChunkSize(null));
    assertEquals(TextFileChunkPlan.DEFAULT_CHUNK_SIZE, TextFileChunkPlan.parseChunkSize(" "));
    assertEquals(64 * MB, TextFileChunkPlan.parseChunkSize("64MB"));
    assertEquals(512 * 1024, TextFileChunkPlan.parseChunkSize("512 kb"));
    assertEquals(2 * 1024 * MB, TextFileChunkPlan.parseChunkSize("2GB"));
    assertEquals(4096, TextFileChunkPlan.parseChunkSize("4096"));
    assertEquals(100, TextFileChunkPlan.parseChunkSize("100B"));
  }

  @Test
  void refusesInvalidChunkSizes() {
    assertThrows(HopException.class, () -> TextFileChunkPlan.parseChunkSize("lots"));
    assertThrows(HopException.class, () -> TextFileChunkPlan.parseChunkSize("0"));
    assertThrows(HopException.class, () -> TextFileChunkPlan.parseChunkSize("-5MB"));
  }

  @Test
  void findsLinesByTheirLineFeedByte() {
    for (String name : new String[] {"UTF-8", "ISO-8859-1", "windows-1252", "Shift_JIS", "GBK"}) {
      assertArrayEquals(new byte[] {0x0A}, TextFileChunkPlan.lineFeed(Charset.forName(name)), name);
    }
  }

  @Test
  void findsUtf16LineFeeds() {
    assertArrayEquals(
        new byte[] {0x0A, 0x00}, TextFileChunkPlan.lineFeed(Charset.forName("UTF-16LE")));
    assertArrayEquals(
        new byte[] {0x00, 0x0A}, TextFileChunkPlan.lineFeed(Charset.forName("UTF-16BE")));
  }

  @Test
  void refusesEncodingsWhereLinesCannotBeFoundByBytes() {
    assertNull(TextFileChunkPlan.lineFeed(Charset.forName("UTF-32")));
    assertNull(TextFileChunkPlan.lineFeed(Charset.forName("ISO-2022-JP")));
    assertNull(TextFileChunkPlan.lineFeed(Charset.forName("IBM037")), "EBCDIC");
  }

  // ------------------------------------------------------------ splitting real files

  private FileObject write(String name, String text) throws Exception {
    Path path = dir.resolve(name);
    Files.writeString(path, text, StandardCharsets.UTF_8);
    return HopVfs.getFileObject(path.toString());
  }

  private static List<TextFileChunk> drain(TextFileChunkPlan plan) {
    List<TextFileChunk> chunks = new ArrayList<>();
    for (TextFileChunk chunk = plan.next(); chunk != null; chunk = plan.next()) {
      chunks.add(chunk);
    }
    return chunks;
  }

  /** The chunks follow each other without gap or overlap, from the start to the end of the file. */
  private static void assertTiles(List<TextFileChunk> chunks, long size) {
    long expected = 0;
    for (TextFileChunk chunk : chunks) {
      assertEquals(expected, chunk.start(), "chunk starts where the previous one ended");
      assertTrue(chunk.end() > chunk.start(), "no empty chunk");
      expected = chunk.end();
    }
    assertEquals(size, expected, "the last chunk ends at the end of the file");
  }

  private static String lines(int count, boolean enclosedBreaks) {
    StringBuilder text = new StringBuilder();
    for (int i = 1; i <= count; i++) {
      text.append(i).append(',');
      text.append(enclosedBreaks && i % 3 == 0 ? "\"spans\nlines " + i + "\"" : "row-" + i);
      text.append('\n');
    }
    return text.toString();
  }

  @Test
  void aLargeFileGivesMoreChunksThanCopies() throws Exception {
    String text = lines(5000, false);
    FileObject file = write("lines.csv", text);
    int copies = 4;

    TextFileChunkPlan plan =
        TextFileChunkPlan.build(
            List.of(file), 1024, copies, false, 0, "UTF-8", null, mock(ILogChannel.class));

    List<TextFileChunk> chunks = drain(plan);
    assertTrue(
        chunks.size() > copies * 10, chunks.size() + " chunks for " + text.length() + " bytes");
    assertEquals(plan.size(), chunks.size());
    assertTiles(chunks, text.length());
  }

  @Test
  void aLargeFileWithBreaksInEnclosedFieldsGivesMoreChunksThanCopies() throws Exception {
    String text = lines(5000, true);
    FileObject file = write("breaks.csv", text);
    int copies = 4;
    RecordFormat records = new RecordFormat("\"", "", TextFileLineUtil.FILE_FORMAT_UNIX, false);

    TextFileChunkPlan plan =
        TextFileChunkPlan.build(
            List.of(file), 1024, copies, false, 0, "UTF-8", records, mock(ILogChannel.class));

    List<TextFileChunk> chunks = drain(plan);
    assertTrue(
        chunks.size() > copies * 10, chunks.size() + " chunks for " + text.length() + " bytes");
    assertTiles(chunks, text.length());
    for (TextFileChunk chunk : chunks) {
      assertTrue(chunk.exact());
      // An exact chunk starts at a record: never right after the line feed inside "spans\nlines"
      if (chunk.start() > 0) {
        assertTrue(
            !text.startsWith("lines ", (int) chunk.start()), "split inside a record: " + chunk);
      }
    }
  }

  /**
   * Fetching ahead on storage other than local disk, here the in-memory file system, which reads at
   * an offset like object storage does: the chunks read in plan order give back the file, every
   * line exactly once, whatever the number of blocks in flight.
   */
  @Test
  void chunksFetchedAheadGiveBackTheFile() throws Exception {
    for (boolean enclosedBreaks : new boolean[] {false, true}) {
      String text = lines(30_000, enclosedBreaks);
      FileObject file =
          HopVfs.getFileObject("ram:///chunk-plan-test/read-ahead-" + enclosedBreaks + ".csv");
      file.getParent().createFolder();
      try (OutputStream out = HopVfs.getOutputStream(file, false)) {
        out.write(text.getBytes(StandardCharsets.UTF_8));
      }
      RecordFormat records =
          enclosedBreaks
              ? new RecordFormat("\"", "", TextFileLineUtil.FILE_FORMAT_UNIX, false)
              : null;
      List<TextFileChunk> chunks =
          drain(
              TextFileChunkPlan.build(
                  List.of(file),
                  150 * 1024,
                  3,
                  false,
                  0,
                  "UTF-8",
                  records,
                  mock(ILogChannel.class)));
      assertTrue(chunks.size() > 1, "the file is split: " + chunks);

      for (int blocks : new int[] {1, 3}) {
        TextFileReadAhead readAhead = new TextFileReadAhead(blocks);
        // Reading the file once first to find the record starts, fetched ahead too
        List<TextFileChunk> fetchedAhead =
            drain(
                TextFileChunkPlan.buildFromSources(
                    List.of(new TextFileChunkPlan.Source(file, null)),
                    150 * 1024,
                    3,
                    false,
                    0,
                    "UTF-8",
                    records,
                    readAhead,
                    mock(ILogChannel.class)));
        assertEquals(
            chunks.stream().map(c -> c.start() + "-" + c.end()).toList(),
            fetchedAhead.stream().map(c -> c.start() + "-" + c.end()).toList(),
            "the same chunks with " + blocks + " blocks in flight");
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        for (TextFileChunk chunk : chunks) {
          try (InputStream in = TextFileChunkPlan.open(chunk, readAhead)) {
            in.transferTo(all);
          }
        }
        assertEquals(text, all.toString(StandardCharsets.UTF_8), blocks + " blocks in flight");
      }
    }
  }

  @Test
  void parsesReadAheadSettings() throws HopException {
    // Blank: a pipeline saved before the setting existed reads as it always did
    assertEquals(TextFileReadAhead.NONE, TextFileReadAhead.parse(null));
    assertEquals(TextFileReadAhead.NONE, TextFileReadAhead.parse(" "));
    assertEquals(new TextFileReadAhead(0), TextFileReadAhead.parse("0"));
    assertEquals(new TextFileReadAhead(16), TextFileReadAhead.parse(" 16 "));
    assertThrows(HopException.class, () -> TextFileReadAhead.parse("-1"));
    assertThrows(HopException.class, () -> TextFileReadAhead.parse("65"));
    assertThrows(HopException.class, () -> TextFileReadAhead.parse("four"));
  }

  /**
   * A chunk that starts inside a line much longer than itself has no line of its own: it stops at
   * its end instead of reading on to the end of that line.
   */
  @Test
  void aChunkInsideALongLineReadsNoFurtherThanItsEnd() throws Exception {
    byte[] bytes = ("a\n" + "x".repeat(1_000_000) + "\nb\n").getBytes(StandardCharsets.UTF_8);
    long start = 100_000;
    long end = 200_000;
    long openedAt = start - 1;
    long[] read = {0};
    InputStream source =
        new java.io.FilterInputStream(
            new java.io.ByteArrayInputStream(bytes, (int) openedAt, bytes.length)) {
          @Override
          public int read(byte[] buffer, int offset, int length) throws IOException {
            int n = super.read(buffer, offset, length);
            read[0] += Math.max(0, n);
            return n;
          }

          @Override
          public int read() throws IOException {
            int b = super.read();
            read[0] += b < 0 ? 0 : 1;
            return b;
          }
        };

    try (InputStream chunk =
        new TextFileChunkInputStream(source, openedAt, start, end, new byte[] {0x0A})) {
      assertEquals(-1, chunk.read(new byte[8192]));
    }
    assertTrue(read[0] <= end - openedAt, read[0] + " bytes read for a chunk of " + (end - start));
  }

  /**
   * Chunks smaller than the block size the storage reads in would each download whole blocks: the
   * block size is used instead, and the chunk size is reported once as a warning, not an error.
   */
  @Test
  void usesTheStorageBlockSizeForSmallerChunks() throws Exception {
    String text = lines(20_000, false);
    FileObject file = write("block-size.csv", text);
    ILogChannel log = mock(ILogChannel.class);

    List<TextFileChunk> chunks =
        drain(
            TextFileChunkPlan.buildFromSources(
                List.of(new TextFileChunkPlan.Source(file, null)),
                1024,
                3,
                false,
                0,
                "UTF-8",
                null,
                TextFileReadAhead.NONE,
                log,
                f -> 64 * 1024));

    assertTiles(chunks, text.length());
    for (int i = 0; i < chunks.size() - 1; i++) {
      assertEquals(64 * 1024, chunks.get(i).end() - chunks.get(i).start(), "chunk " + i);
    }
    verify(log, times(1)).logBasic(contains("Warning: the chunk size of 1024 bytes"));
  }

  @Test
  void raisesTheSplitToTheBlockSizeAndWarnsOnlyForAConfiguredChunkSize() {
    FileObject file = mock(FileObject.class);
    FileName name = mock(FileName.class);
    when(name.getFriendlyURI()).thenReturn("s3://bucket/file.csv");
    when(file.getName()).thenReturn(name);
    ILogChannel log = mock(ILogChannel.class);
    java.util.Set<Integer> warned = new java.util.HashSet<>();
    long mb = 1024 * 1024;

    // Storage without a block size of its own, or a split at least that large: unchanged
    assertEquals(1024, TextFileChunkPlan.splitForStorage(1024, 1024, 0, file, warned, log));
    assertEquals(
        128 * mb,
        TextFileChunkPlan.splitForStorage(128 * mb, 128 * mb, (int) (8 * mb), file, warned, log));
    // Little data made the split small, the chunk size is fine: raised without a warning
    assertEquals(
        8 * mb,
        TextFileChunkPlan.splitForStorage(4 * mb, 128 * mb, (int) (8 * mb), file, warned, log));
    verify(log, never()).logBasic(anyString());
    // A configured chunk size below the block size: raised, warned about once
    assertEquals(
        8 * mb, TextFileChunkPlan.splitForStorage(mb, mb, (int) (8 * mb), file, warned, log));
    assertEquals(
        8 * mb, TextFileChunkPlan.splitForStorage(mb, mb, (int) (8 * mb), file, warned, log));
    verify(log, times(1)).logBasic(contains("Warning"));
    // Always even, for two-byte line feeds
    assertEquals(65_538, TextFileChunkPlan.splitForStorage(1024, 1024, 65_537, file, warned, log));
  }

  /**
   * A download that keeps failing, under a chunk read through the whole reader stack: the lines
   * read completely before it come out, then an error, and every read after it fails too. No line
   * cut short by the error is ever returned, and nothing after the failed block.
   */
  @Test
  void aFailedDownloadFailsTheReadWithoutAPartialLine() throws Exception {
    StringBuilder content = new StringBuilder();
    for (int i = 1; content.length() < 50_000; i++) {
      content.append(i).append(",row-").append(i).append('\n');
    }
    byte[] bytes = content.toString().getBytes(StandardCharsets.UTF_8);
    int blockSize = 10_000;
    FileObject file =
        mock(FileObject.class, withSettings().extraInterfaces(IRangeReadableFile.class));
    FileName name = mock(FileName.class);
    when(name.getFriendlyURI()).thenReturn("test:///failing.csv");
    when(file.getName()).thenReturn(name);
    IRangeReadableFile storage = (IRangeReadableFile) file;
    when(storage.getReadBlockSize()).thenReturn(blockSize);
    when(storage.getInputStream(anyLong(), anyLong()))
        .thenAnswer(
            call -> {
              long offset = call.getArgument(0);
              long length = call.getArgument(1);
              byte[] block =
                  Arrays.copyOfRange(
                      bytes, (int) offset, (int) Math.min(offset + length, bytes.length));
              if (offset != 2L * blockSize) {
                return new java.io.ByteArrayInputStream(block);
              }
              // The third block: half of it arrives, then the connection is lost, every time
              return new java.io.SequenceInputStream(
                  new java.io.ByteArrayInputStream(block, 0, block.length / 2),
                  new InputStream() {
                    @Override
                    public int read() throws IOException {
                      throw new IOException("Connection reset");
                    }
                  });
            });

    TextFileChunkInputStream chunk =
        new TextFileChunkInputStream(
            new org.apache.hop.core.vfs.ReadAheadInputStream(
                file, bytes.length, 0, bytes.length, blockSize, 2),
            0,
            0,
            bytes.length,
            new byte[] {0x0A});
    org.apache.hop.ui.pipeline.transform.common.TextFileLineScanner scanner =
        new org.apache.hop.ui.pipeline.transform.common.TextFileLineScanner(
            new java.io.InputStreamReader(chunk, StandardCharsets.UTF_8));
    StringBuilder line = new StringBuilder();
    List<String> lines = new ArrayList<>();
    HopException error = null;
    for (int call = 0; call < 10_000 && error == null; call++) {
      try {
        String next =
            scanner.getLine(
                org.apache.hop.core.file.EncodingType.SINGLE,
                TextFileLineUtil.FILE_FORMAT_UNIX,
                line,
                null,
                null,
                false);
        if (next == null) {
          break;
        }
        lines.add(next);
      } catch (HopException e) {
        error = e;
      }
    }

    assertNotNull(error, "the read failed");
    String readBefore = new String(bytes, 0, 2 * blockSize, StandardCharsets.UTF_8);
    List<String> complete =
        List.of(readBefore.substring(0, readBefore.lastIndexOf('\n')).split("\n"));
    // Only lines that are complete in the blocks before the failed one, every one of them whole
    assertTrue(lines.size() <= complete.size(), lines.size() + " lines");
    assertEquals(complete.subList(0, lines.size()), lines);
    assertThrows(
        HopException.class,
        () ->
            scanner.getLine(
                org.apache.hop.core.file.EncodingType.SINGLE,
                TextFileLineUtil.FILE_FORMAT_UNIX,
                line,
                null,
                null,
                false));
    scanner.close();
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileRecordSplitter.RecordFormat;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Splitting at the record starts found by scanning loses, duplicates and changes no record: reading
 * the chunks one by one, as Text File Input does with breaks in enclosed fields allowed, gives the
 * records of reading the whole file, wherever the cuts fall.
 */
class TextFileRecordSplitterTest {

  private static final String[] FORMATS = {"DOS", "UNIX", "MIXED"};
  private static final int[] FORMAT_NRS = {
    TextFileLineUtil.FILE_FORMAT_DOS,
    TextFileLineUtil.FILE_FORMAT_UNIX,
    TextFileLineUtil.FILE_FORMAT_MIXED
  };

  private static ExecutorService pool;

  @TempDir Path dir;

  @BeforeAll
  static void startPool() {
    pool = TextFileRecordSplitter.newPool(4);
  }

  @AfterAll
  static void stopPool() {
    pool.shutdownNow();
  }

  /** Records with plain and enclosed fields, the enclosed ones with line breaks and quotes. */
  private static String randomText(Random random, String enclosure, String escape, boolean dos) {
    String[] breaks = dos ? new String[] {"\r\n"} : new String[] {"\n", "\r\n", "\n\n"};
    StringBuilder text = new StringBuilder();
    int records = 20 + random.nextInt(60);
    for (int r = 0; r < records; r++) {
      int fields = random.nextInt(4);
      for (int f = 0; f < fields; f++) {
        if (f > 0) {
          text.append(',');
        }
        switch (random.nextInt(4)) {
          case 0 -> text.append("plain").append(random.nextInt(1000));
          case 1 -> text.append("δέλτα日本");
          default -> {
            if (enclosure.isEmpty()) {
              text.append("x").append(breaks[random.nextInt(breaks.length)]);
              break;
            }
            text.append(enclosure);
            int parts = 1 + random.nextInt(4);
            for (int p = 0; p < parts; p++) {
              switch (random.nextInt(4)) {
                case 0 -> text.append(breaks[random.nextInt(breaks.length)]);
                case 1 -> text.append(escape.isEmpty() ? "q" : escape + enclosure);
                case 2 -> text.append("a,b é");
                default -> text.append("text ").append(p);
              }
            }
            text.append(enclosure);
          }
        }
      }
      if (r < records - 1 || random.nextBoolean()) {
        text.append(breaks[random.nextInt(breaks.length)]);
      }
    }
    return text.toString();
  }

  private static List<String> records(
      byte[] bytes, Charset charset, int format, String enclosure, String escape) throws Exception {
    InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(bytes), charset);
    List<String> records = new ArrayList<>();
    StringBuilder line = new StringBuilder();
    String record;
    while ((record = TextFileLineUtil.getLine(null, reader, format, line, enclosure, escape, true))
        != null) {
      records.add(record);
    }
    return records;
  }

  private FileObject write(String name, byte[] bytes) throws Exception {
    Path path = dir.resolve(name);
    Files.write(path, bytes);
    return HopVfs.getFileObject(path.toString());
  }

  private void assertSplitsAtRecords(
      String name,
      byte[] bytes,
      Charset charset,
      byte[] lineFeed,
      int format,
      String enclosure,
      String escape,
      long split)
      throws Exception {
    FileObject file = write(name, bytes);
    RecordFormat records = new RecordFormat(enclosure, escape, format, false);
    List<Long> starts =
        TextFileRecordSplitter.recordStarts(
            file, 0, bytes.length, split, lineFeed, records, TextFileReadAhead.NONE, pool);

    List<String> chunked = new ArrayList<>();
    for (int s = 0; s < starts.size(); s++) {
      int from = starts.get(s).intValue();
      int to = s + 1 < starts.size() ? starts.get(s + 1).intValue() : bytes.length;
      assertTrue(from < to, name + ": empty chunk at " + from + " in " + starts);
      chunked.addAll(
          records(Arrays.copyOfRange(bytes, from, to), charset, format, enclosure, escape));
    }
    assertEquals(
        records(bytes, charset, format, enclosure, escape),
        chunked,
        name + " split every " + split + " at " + starts);
  }

  @Test
  void chunksHoldTheRecordsOfTheWholeFile() throws Exception {
    Random random = new Random(20261010);
    String[][] patterns = {{"\"", ""}, {"\"", "\\"}, {"\"", "\""}, {"'", "\\"}, {"", ""}};
    for (int run = 0; run < 300; run++) {
      int f = run % FORMATS.length;
      String[] pattern = patterns[random.nextInt(patterns.length)];
      String text = randomText(random, pattern[0], pattern[1], f == 0);
      boolean utf16 = random.nextInt(4) == 0;
      Charset charset = utf16 ? StandardCharsets.UTF_16LE : StandardCharsets.UTF_8;
      byte[] lineFeed = utf16 ? new byte[] {0x0A, 0x00} : new byte[] {0x0A};
      long split = 2L * (1 + random.nextInt(40));
      assertSplitsAtRecords(
          "run" + run + "-" + FORMATS[f] + (utf16 ? "-utf16" : "") + ".txt",
          text.getBytes(charset),
          charset,
          lineFeed,
          FORMAT_NRS[f],
          pattern[0],
          pattern[1],
          split);
    }
  }

  @Test
  void aLineFeedInAnEnclosedFieldNeverStartsAChunk() throws Exception {
    String text = "1,\"one\nline\nmore\"\n2,\"two\n\"\n3,three\n";
    FileObject file = write("enclosed.csv", text.getBytes(StandardCharsets.UTF_8));
    RecordFormat records = new RecordFormat("\"", "", TextFileLineUtil.FILE_FORMAT_UNIX, false);

    List<Long> starts =
        TextFileRecordSplitter.recordStarts(
            file, 0, text.length(), 2, new byte[] {0x0A}, records, TextFileReadAhead.NONE, pool);

    // Records start at 0, after "more\"\n", and after "two\n\"\n"
    assertEquals(List.of(0L, 18L, 27L), starts);
  }

  @Test
  void findsTheHeaderEndAcrossEnclosedLineBreaks() throws Exception {
    String text = "\"title\nspanning\",x\n\nid,name\n1,a\n";
    FileObject file = write("header.csv", text.getBytes(StandardCharsets.UTF_8));
    byte[] lf = {0x0A};

    RecordFormat keepEmpty = new RecordFormat("\"", "", TextFileLineUtil.FILE_FORMAT_UNIX, false);
    assertEquals(19, TextFileRecordSplitter.headerEnd(file, 0, lf, keepEmpty, 1, 1024));
    assertEquals(20, TextFileRecordSplitter.headerEnd(file, 0, lf, keepEmpty, 2, 1024));

    // Leaving out empty lines, the empty line is no header line
    RecordFormat noEmpty = new RecordFormat("\"", "", TextFileLineUtil.FILE_FORMAT_UNIX, true);
    assertEquals(28, TextFileRecordSplitter.headerEnd(file, 0, lf, noEmpty, 2, 1024));

    assertEquals(-1, TextFileRecordSplitter.headerEnd(file, 0, lf, keepEmpty, 2, 5));
  }

  @Test
  void splitsOnlyWhereBytesTellTheCharacters() {
    RecordFormat quoted = new RecordFormat("\"", "\\", TextFileLineUtil.FILE_FORMAT_UNIX, false);
    for (String name : new String[] {"UTF-8", "ISO-8859-1", "windows-1252", "UTF-16LE"}) {
      assertNull(TextFileRecordSplitter.problem(Charset.forName(name), quoted), name);
    }
    // A Shift_JIS character can have a backslash as its second byte
    assertNotNull(TextFileRecordSplitter.problem(Charset.forName("Shift_JIS"), quoted));
    assertNotNull(
        TextFileRecordSplitter.problem(
            StandardCharsets.UTF_8,
            new RecordFormat("«", "", TextFileLineUtil.FILE_FORMAT_UNIX, false)));
  }
}

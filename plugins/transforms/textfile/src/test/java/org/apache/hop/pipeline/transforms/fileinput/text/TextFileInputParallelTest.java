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

import static org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputHarness.addFile;
import static org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputHarness.csvMeta;
import static org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputHarness.run;
import static org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputHarness.runWithFilenames;
import static org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputHarness.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputHarness.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * "Running in parallel?": the copies share the files in chunks, and together output every row of a
 * sequential read exactly once. Small chunks make even small test files split into many chunks.
 */
class TextFileInputParallelTest {

  @TempDir Path dir;

  @BeforeAll
  static void initHop() throws Exception {
    TextFileInputHarness.initHop();
  }

  /** Lines "id,text" for ids from..to, with some multi-byte text and an empty field. */
  private static String lines(int from, int to, String lineEnd) {
    StringBuilder text = new StringBuilder();
    for (int i = from; i <= to; i++) {
      text.append(i).append(',');
      text.append(i % 7 == 0 ? "" : i % 5 == 0 ? "δέλτα-日本-" + i : "row-" + i);
      text.append(lineEnd);
    }
    return text.toString();
  }

  private TextFileInputMeta parallelMeta(String chunkSize) {
    TextFileInputMeta meta = csvMeta("id", "text");
    meta.getContent().setRunningInParallel(true);
    meta.getContent().setChunkSize(chunkSize);
    return meta;
  }

  private static TextFileInputMeta sequentialCopy(TextFileInputMeta parallel) {
    TextFileInputMeta meta = (TextFileInputMeta) parallel.clone();
    meta.getContent().setRunningInParallel(false);
    return meta;
  }

  private static List<String> sorted(List<String> lines) {
    List<String> copy = new ArrayList<>(lines);
    copy.sort(null);
    return copy;
  }

  /** Reads in parallel and sequentially, and checks both give the same rows. */
  private static Result assertSameRowsAsSequential(TextFileInputMeta parallel, int copies)
      throws Exception {
    Result sequential = run(sequentialCopy(parallel), 1);
    assertEquals(0, sequential.errors(), sequential.log());
    Result result = run(parallel, copies);
    assertEquals(0, result.errors(), result.log());
    assertEquals(sequential.lines().size(), result.lines().size(), "row count");
    assertEquals(sorted(sequential.lines()), sorted(result.lines()));
    return result;
  }

  private static int copiesWithRows(Result result) {
    int busy = 0;
    for (List<Object[]> rows : result.rowsByCopy()) {
      if (!rows.isEmpty()) {
        busy++;
      }
    }
    return busy;
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4})
  void copiesTogetherReadEveryLineOnce(int copies) throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    addFile(meta, write(dir, "big.csv", lines(1, 3000, "\n")));

    Result result = assertSameRowsAsSequential(meta, copies);

    assertTrue(copiesWithRows(result) > 1, "the chunks were shared: " + result.rowsByCopy());
  }

  @Test
  void skipsTheHeaderOnlyAtTheStartOfTheFile() throws Exception {
    TextFileInputMeta meta = parallelMeta("2KB");
    meta.getContent().setHeader(true);
    meta.getContent().setNrHeaderLines(2);
    addFile(meta, write(dir, "header.csv", "title\r\nid,text\r\n" + lines(1, 2000, "\r\n")));

    Result result = assertSameRowsAsSequential(meta, 3);

    assertEquals(2000, result.lines().size());
  }

  @Test
  void readsSmallFilesWholeAndSplitsLargeOnes() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    addFile(meta, write(dir, "a-small.csv", lines(1, 10, "\n")));
    addFile(meta, write(dir, "b-large.csv", lines(11, 2000, "\n")));
    addFile(meta, write(dir, "c-small.csv", lines(2001, 2005, "\n")));
    addFile(meta, write(dir, "d-empty.csv", ""));

    Result result = assertSameRowsAsSequential(meta, 4);

    assertEquals(2005, result.lines().size());
  }

  @Test
  void readsUtf16WithAByteOrderMarkInChunks() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    meta.getContent().setEncoding(null);
    byte[] body = lines(1, 1500, "\n").getBytes(StandardCharsets.UTF_16LE);
    byte[] bytes = new byte[body.length + 2];
    bytes[0] = (byte) 0xFF;
    bytes[1] = (byte) 0xFE;
    System.arraycopy(body, 0, bytes, 2, body.length);
    addFile(meta, write(dir, "utf16.csv", bytes));

    Result result = assertSameRowsAsSequential(meta, 3);

    assertEquals(1500, result.lines().size());
  }

  @Test
  void readsAConfiguredMultiByteEncodingInChunks() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    meta.getContent().setEncoding("UTF-8");
    addFile(meta, write(dir, "utf8.csv", lines(1, 2500, "\n"), Charset.forName("UTF-8")));

    assertSameRowsAsSequential(meta, 4);
  }

  @Test
  void aLastLineWithoutLineFeedIsReadOnce() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    String text = lines(1, 1999, "\n") + "2000,last";
    addFile(meta, write(dir, "no-final-newline.csv", text));

    Result result = assertSameRowsAsSequential(meta, 2);

    assertTrue(result.lines().contains("2000|last"), result.lines().toString());
  }

  @Test
  void oneCopyReadsAsWithoutParallel() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    addFile(meta, write(dir, "one-copy.csv", lines(1, 500, "\n")));

    Result sequential = run(sequentialCopy(meta), 1);
    Result result = run(meta, 1);

    assertEquals(0, result.errors(), result.log());
    assertEquals(sequential.lines(), result.lines(), "same rows, same order");
  }

  @Test
  void addsEachFileToTheResultOnce() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    meta.getContent().setIncludeFilename(true);
    meta.getContent().setFilenameField("file");
    addFile(meta, write(dir, "named.csv", lines(1, 2000, "\n")));

    Result result = run(meta, 3);

    assertEquals(0, result.errors(), result.log());
    int file = result.rowMeta().indexOfValue("file");
    for (Object[] row : result.rows()) {
      assertTrue(row[file].toString().endsWith("named.csv"));
    }
  }

  // ------------------------------------------------------------ breaks in enclosed fields

  /** Lines "id,text" where every third text is enclosed and spans several lines. */
  private static String linesWithBreaks(int from, int to, String lineEnd) {
    StringBuilder text = new StringBuilder();
    for (int i = from; i <= to; i++) {
      text.append(i).append(',');
      if (i % 3 == 0) {
        text.append("\"first ").append(i).append(lineEnd).append("second, \"\"x\"\"");
        text.append(lineEnd).append(lineEnd).append("third\"");
      } else {
        text.append(i % 5 == 0 ? "δέλτα-日本-" + i : "row-" + i);
      }
      text.append(lineEnd);
    }
    return text.toString();
  }

  private TextFileInputMeta breaksMeta(String fileFormat) {
    TextFileInputMeta meta = parallelMeta("1KB");
    meta.getContent().setBreakInEnclosureAllowed(true);
    meta.getContent().setFileFormat(fileFormat);
    return meta;
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4})
  void readsBreaksInEnclosedFieldsInChunks(int copies) throws Exception {
    TextFileInputMeta meta = breaksMeta("unix");
    addFile(meta, write(dir, "breaks.csv", linesWithBreaks(1, 2000, "\n")));

    Result result = assertSameRowsAsSequential(meta, copies);

    assertEquals(2000, result.lines().size());
    assertTrue(copiesWithRows(result) > 1, "the chunks were shared: " + result.rowsByCopy());
  }

  @Test
  void readsDosBreaksInEnclosedFieldsAfterAMultiLineHeader() throws Exception {
    TextFileInputMeta meta = breaksMeta("DOS");
    meta.getContent().setHeader(true);
    meta.getContent().setNrHeaderLines(2);
    meta.getContent().setNoEmptyLines(true);
    String header = "\"a title\r\nover lines\"\r\n\r\nid,text\r\n";
    addFile(meta, write(dir, "dos-breaks.csv", header + linesWithBreaks(1, 1500, "\r\n")));

    Result result = assertSameRowsAsSequential(meta, 3);

    assertEquals(1500, result.lines().size());
  }

  @Test
  void readsMixedBreaksInEnclosedFieldsInUtf16() throws Exception {
    TextFileInputMeta meta = breaksMeta("mixed");
    meta.getContent().setEncoding("UTF-16LE");
    addFile(
        meta,
        write(
            dir, "utf16-breaks.csv", linesWithBreaks(1, 1200, "\r\n"), StandardCharsets.UTF_16LE));

    Result result = assertSameRowsAsSequential(meta, 4);

    assertEquals(1200, result.lines().size());
  }

  // ------------------------------------------------------------ settings that rule it out

  private void assertRefused(Consumer<TextFileInputMeta> setting, String label) throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    setting.accept(meta);
    addFile(meta, write(dir, "refused.csv", lines(1, 100, "\n")));

    Result result = run(meta, 2);

    assertTrue(result.startFailure() != null, "must not start with " + label);
    assertTrue(result.log().contains(label), result.log());
  }

  @Test
  void refusesAFooter() throws Exception {
    assertRefused(meta -> meta.getContent().setFooter(true), "Footer");
  }

  @Test
  void refusesWrappedLines() throws Exception {
    assertRefused(meta -> meta.getContent().setLineWrapped(true), "Wrapped lines?");
  }

  @Test
  void refusesAPagedLayout() throws Exception {
    assertRefused(meta -> meta.getContent().setLayoutPaged(true), "Paged layout");
  }

  @Test
  void refusesRowNumbersByFile() throws Exception {
    assertRefused(
        meta -> {
          meta.getContent().setIncludeRowNumber(true);
          meta.getContent().setRowNumberField("nr");
          meta.getContent().setRowNumberByFile(true);
        },
        "Rownum by file?");
  }

  @Test
  void refusesCompression() throws Exception {
    assertRefused(meta -> meta.getContent().setFileCompression("GZip"), "Compression");
  }

  @Test
  void refusesAStopOnFilter() throws Exception {
    assertRefused(
        meta -> meta.getFilters().add(new TextFileFilter(0, "STOP", true, false)),
        "Stop on filter");
  }

  @Test
  void refusesRepeatedFields() throws Exception {
    assertRefused(
        meta -> {
          TextFileInputField repeated = new TextFileInputField("extra", -1, -1);
          repeated.setRepeated(true);
          meta.getInputFields().add(repeated);
        },
        "Repeat");
  }

  @Test
  void refusesAnInvalidChunkSize() throws Exception {
    assertRefused(meta -> meta.getContent().setChunkSize("lots"), "Invalid chunk size");
  }

  /** Large and small files named by a previous transform, as {@link #lines} in 1 KB chunks. */
  private List<Path> namedFiles() throws Exception {
    return List.of(
        write(dir, "a-small.csv", lines(1, 10, "\n")),
        write(dir, "b-large.csv", lines(11, 2000, "\n")),
        write(dir, "c-small.csv", lines(2001, 2005, "\n")),
        write(dir, "d-large.csv", lines(2006, 3500, "\n")));
  }

  /** The filenames read by one copy, as the transform does without parallel reading. */
  private static Result runSequentially(TextFileInputMeta parallel, List<Path> files)
      throws Exception {
    Result sequential = runWithFilenames(sequentialCopy(parallel), 1, files);
    assertEquals(0, sequential.errors(), sequential.log());
    return sequential;
  }

  /**
   * The copies each receive part of the filenames, but the files of all copies are split together:
   * the large files are read in chunks by several copies.
   */
  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4})
  void readsFilenamesFromAPreviousTransformInChunks(int copies) throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    List<Path> files = namedFiles();

    Result result = runWithFilenames(meta, copies, files);

    assertEquals(0, result.errors(), result.log());
    assertEquals(sorted(runSequentially(meta, files).lines()), sorted(result.lines()));
    assertTrue(copiesWithRows(result) > 1, "the chunks were shared: " + result.rowsByCopy());
  }

  /**
   * Every row read from a file carries the fields of the row that named it, also in a chunk read by
   * another copy, and in a copy that received no filename rows itself.
   */
  @Test
  void passesThroughTheFieldsOfTheRowThatNamedTheFile() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    meta.getFileInput().setPassingThruFields(true);
    // 2 filename rows for 3 copies: the third copy receives none
    List<Path> files =
        List.of(
            write(dir, "first.csv", lines(1, 1500, "\n")),
            write(dir, "second.csv", lines(1501, 3000, "\n")));

    Result result = runWithFilenames(meta, 3, files);

    assertEquals(0, result.errors(), result.log());
    List<String> expected = runSequentially(meta, files).lines();
    assertTrue(expected.get(0).contains("|first.csv-0|"), expected.get(0));
    assertEquals(sorted(expected), sorted(result.lines()));
    assertTrue(
        !result.rowsByCopy().get(2).isEmpty(), "the copy without filename rows read chunks too");
  }

  /** A file named twice is read twice, as it is without parallel reading. */
  @Test
  void readsAFileNamedTwiceTwice() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    Path file = write(dir, "twice.csv", lines(1, 1000, "\n"));
    List<Path> files = List.of(file, file);

    Result result = runWithFilenames(meta, 3, files);

    assertEquals(0, result.errors(), result.log());
    assertEquals(2000, result.lines().size());
    assertEquals(sorted(runSequentially(meta, files).lines()), sorted(result.lines()));
  }

  /**
   * A previous transform that copies its rows sends every filename to every copy. The files are
   * still read once, not once per copy.
   */
  @Test
  void readsCopiedFilenameRowsOnce() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    List<Path> files = namedFiles();

    Result result = runWithFilenames(meta, 3, files, false);

    assertEquals(0, result.errors(), result.log());
    assertEquals(sorted(runSequentially(meta, files).lines()), sorted(result.lines()));
  }

  /** A missing file fails the copy that opens it, and the other copies don't wait forever. */
  @Test
  @Timeout(60)
  void reportsAMissingNamedFile() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    List<Path> files = new ArrayList<>(namedFiles());
    files.add(dir.resolve("missing.csv"));

    Result result = runWithFilenames(meta, 3, files);

    assertTrue(result.errors() > 0, result.log());
  }

  /** With "Skip bad files", a missing file is skipped and every other file is read. */
  @Test
  void skipsAMissingNamedFile() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    meta.getErrorHandling().setErrorIgnored(true);
    meta.getErrorHandling().setSkipBadFiles(true);
    List<Path> files = namedFiles();
    List<Path> withMissing = new ArrayList<>(files);
    withMissing.add(1, dir.resolve("missing.csv"));

    Result result = runWithFilenames(meta, 3, withMissing);

    assertEquals(0, result.errors(), result.log());
    assertEquals(sorted(runSequentially(meta, files).lines()), sorted(result.lines()));
  }

  /** Copies that fail while collecting their filenames stop the copy waiting for them. */
  @Test
  @Timeout(60)
  void stopsWaitingWhenACopyFails() throws Exception {
    TextFileInputMeta meta = parallelMeta("1KB");
    // 2 filename rows for 3 copies: the third copy has nothing to fail on, and waits
    List<Path> files = namedFiles().subList(0, 2);

    Result result = runWithFilenames(meta, 3, files, true, "no-such-field");

    assertTrue(result.errors() > 0, result.log());
  }

  @Test
  void refusesAnInvalidReadAhead() throws Exception {
    assertRefused(meta -> meta.getContent().setReadAheadBlocks("many"), "read-ahead blocks");
  }

  /** About 10 MB of text for one field, much longer than the chunks. */
  private static String longValue(String lineBreak) {
    StringBuilder value = new StringBuilder(10 * 1024 * 1024 + 1024);
    int piece = 0;
    while (value.length() < 10 * 1024 * 1024) {
      value.append("long-").append(piece++);
      if (lineBreak != null && piece % 5000 == 0) {
        // A line feed and an escaped enclosure now and then, inside the enclosed field
        value.append(lineBreak).append("\"\"quoted\"\"");
      }
      value.append(' ');
    }
    return value.toString();
  }

  /**
   * One line of about 10 MB among short ones, read in 512 KB chunks: it spans about 20 chunks. It
   * is read whole by the copy whose chunk holds its first byte; the chunks inside it give no rows,
   * and every other line is read once.
   */
  @Test
  void readsALineMuchLongerThanTheChunks() throws Exception {
    TextFileInputMeta meta = parallelMeta("512KB");
    String text = lines(1, 2000, "\n") + "5000," + longValue(null) + "\n" + lines(2001, 4000, "\n");
    addFile(meta, write(dir, "long-line.csv", text));

    Result result = assertSameRowsAsSequential(meta, 4);

    assertEquals(4001, result.lines().size());
    assertTrue(
        result.lines().stream().anyMatch(line -> line.length() > 10 * 1024 * 1024),
        "the long line is one row");
  }

  /** The same with breaks in enclosed fields: a 10 MB enclosed field with line feeds in it. */
  @Test
  void readsAnEnclosedFieldMuchLongerThanTheChunks() throws Exception {
    TextFileInputMeta meta = breaksMeta("unix");
    meta.getContent().setChunkSize("512KB");
    String text =
        linesWithBreaks(1, 2000, "\n")
            + "5000,\""
            + longValue("\n")
            + "\"\n"
            + linesWithBreaks(2001, 4000, "\n");
    addFile(meta, write(dir, "long-field.csv", text));

    Result result = assertSameRowsAsSequential(meta, 4);

    assertEquals(4001, result.lines().size());
  }
}

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputHarness.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.xerial.snappy.SnappyOutputStream;

/**
 * Records what sequential Text File Input produces today, on the real engine, so that reading files
 * in chunks cannot change it unnoticed. Each expectation is the current behaviour; where that
 * behaviour looks wrong the test says so.
 */
class TextFileInputCharacterizationTest {

  @TempDir Path dir;

  @BeforeAll
  static void initHop() throws Exception {
    TextFileInputHarness.initHop();
  }

  private static void assertRows(Result result, String... expected) {
    assertEquals(0, result.errors(), result.log());
    assertEquals(List.of(expected), result.lines());
  }

  private static List<String> column(Result result, String field) {
    int index = result.rowMeta().indexOfValue(field);
    List<String> values = new ArrayList<>();
    for (Object[] row : result.rows()) {
      values.add(row[index] == null ? null : row[index].toString());
    }
    return values;
  }

  private TextFileInputMeta oneFile(String content, String... fields) throws IOException {
    TextFileInputMeta meta = csvMeta(fields);
    addFile(meta, write(dir, "input.csv", content));
    return meta;
  }

  @Test
  void readsEveryLineOfOneFile() throws Exception {
    assertRows(run(oneFile("1,x\n2,y\n3,z\n", "a", "b"), 1), "1|x", "2|y", "3|z");
  }

  @Test
  void keepsALastLineWithoutNewline() throws Exception {
    assertRows(run(oneFile("1\n2\n3", "a"), 1), "1", "2", "3");
  }

  @Test
  void readsFilesInListOrder() throws Exception {
    TextFileInputMeta meta = csvMeta("a");
    addFile(meta, write(dir, "first.csv", "1\n2\n"));
    addFile(meta, write(dir, "second.csv", "3\n4\n"));

    assertRows(run(meta, 1), "1", "2", "3", "4");
  }

  @Nested
  class HeadersAndFooters {

    @Test
    void skipsTwoHeaderLines() throws Exception {
      TextFileInputMeta meta = oneFile("h1\nh2\n1\n2\n", "a");
      meta.getContent().setHeader(true);
      meta.getContent().setNrHeaderLines(2);

      assertRows(run(meta, 1), "1", "2");
    }

    @Test
    void skipsTheHeaderOfEveryFile() throws Exception {
      TextFileInputMeta meta = csvMeta("a");
      meta.getContent().setHeader(true);
      addFile(meta, write(dir, "first.csv", "h\n1\n"));
      addFile(meta, write(dir, "second.csv", "h\n2\n"));

      assertRows(run(meta, 1), "1", "2");
    }

    @Test
    void dropsTwoFooterLines() throws Exception {
      TextFileInputMeta meta = oneFile("1\n2\n3\nf1\nf2\n", "a");
      meta.getContent().setFooter(true);
      meta.getContent().setNrFooterLines(2);

      assertRows(run(meta, 1), "1", "2", "3");
    }

    @Test
    void dropsAFooterWithoutTrailingNewline() throws Exception {
      TextFileInputMeta meta = oneFile("1\n2\nf", "a");
      meta.getContent().setFooter(true);
      meta.getContent().setNrFooterLines(1);

      assertRows(run(meta, 1), "1", "2");
    }

    @Test
    void skipsHeaderAndFooter() throws Exception {
      TextFileInputMeta meta = oneFile("h\n1\n2\nf\n", "a");
      meta.getContent().setHeader(true);
      meta.getContent().setNrHeaderLines(1);
      meta.getContent().setFooter(true);
      meta.getContent().setNrFooterLines(1);

      assertRows(run(meta, 1), "1", "2");
    }
  }

  @Nested
  class WrappedAndPaged {

    @Test
    void joinsOneWrappedLineAfterAHeader() throws Exception {
      TextFileInputMeta meta = oneFile("h\n1,a\nb\n2,c\nd\n", "n", "v");
      meta.getContent().setHeader(true);
      meta.getContent().setLineWrapped(true);
      meta.getContent().setNrWraps(1);

      assertRows(run(meta, 1), "1|ab", "2|cd");
    }

    @Test
    void joinsTwoWrappedLines() throws Exception {
      TextFileInputMeta meta = oneFile("1,a\nb\nc\n2,d\ne\nf\n", "n", "v");
      meta.getContent().setLineWrapped(true);
      meta.getContent().setNrWraps(2);

      assertRows(run(meta, 1), "1|abc", "2|def");
    }

    /** "Lines per page" counts the data lines only; page header and footer come on top. */
    @Test
    void readsPagesWithDocumentHeaderPageHeaderAndPageFooter() throws Exception {
      TextFileInputMeta meta = oneFile("DOC\nH\n1\n2\nF\nH\n3\n4\nF\n", "a");
      meta.getContent().setLayoutPaged(true);
      meta.getContent().setNrLinesDocHeader(1);
      meta.getContent().setNrLinesPerPage(2);
      meta.getContent().setHeader(true);
      meta.getContent().setNrHeaderLines(1);
      meta.getContent().setFooter(true);
      meta.getContent().setNrFooterLines(1);

      assertRows(run(meta, 1), "1", "2", "3", "4");
    }
  }

  @Nested
  class RowNumbersAndLimits {

    private TextFileInputMeta twoFiles(String... fields) throws IOException {
      TextFileInputMeta meta = csvMeta(fields);
      addFile(meta, write(dir, "first.csv", "a\nb\n"));
      addFile(meta, write(dir, "second.csv", "c\nd\n"));
      return meta;
    }

    @Test
    void numbersRowsAcrossFiles() throws Exception {
      TextFileInputMeta meta = twoFiles("v");
      meta.getContent().setIncludeRowNumber(true);
      meta.getContent().setRowNumberField("nr");

      Result result = run(meta, 1);

      assertEquals(0, result.errors(), result.log());
      assertEquals(List.of("1", "2", "3", "4"), column(result, "nr"));
    }

    @Test
    void restartsRowNumbersPerFile() throws Exception {
      TextFileInputMeta meta = twoFiles("v");
      meta.getContent().setIncludeRowNumber(true);
      meta.getContent().setRowNumberField("nr");
      meta.getContent().setRowNumberByFile(true);

      Result result = run(meta, 1);

      assertEquals(0, result.errors(), result.log());
      assertEquals(List.of("1", "2", "1", "2"), column(result, "nr"));
    }

    @Test
    void stopsAtTheRowLimitInOneFile() throws Exception {
      TextFileInputMeta meta = oneFile("1\n2\n3\n4\n5\n", "a");
      meta.getContent().setRowLimit(3);

      assertRows(run(meta, 1), "1", "2", "3");
    }

    @Test
    void stopsAtTheRowLimitAcrossFiles() throws Exception {
      TextFileInputMeta meta = twoFiles("v");
      meta.getContent().setRowLimit(3);

      assertRows(run(meta, 1), "a", "b", "c");
    }

    @Test
    void addsTheFilenameOfEachRow() throws Exception {
      TextFileInputMeta meta = twoFiles("v");
      meta.getContent().setIncludeFilename(true);
      meta.getContent().setFilenameField("file");

      Result result = run(meta, 1);

      assertEquals(0, result.errors(), result.log());
      List<String> files = column(result, "file");
      assertEquals(4, files.size());
      assertTrue(files.get(1).endsWith("first.csv"), files.toString());
      assertTrue(files.get(2).endsWith("second.csv"), files.toString());
    }

    @Test
    void addsShortFilenameExtensionAndSize() throws Exception {
      TextFileInputMeta meta = oneFile("1\n2\n", "a");
      meta.getAdditionalOutputFields().setShortFilenameField("short");
      meta.getAdditionalOutputFields().setExtensionField("ext");
      meta.getAdditionalOutputFields().setSizeField("size");

      Result result = run(meta, 1);

      assertEquals(0, result.errors(), result.log());
      assertEquals(List.of("input.csv", "input.csv"), column(result, "short"));
      assertEquals(List.of("csv", "csv"), column(result, "ext"));
      assertEquals(List.of("4", "4"), column(result, "size"));
    }
  }

  @Nested
  class Filters {

    @Test
    void keepsOnlyLinesMatchingAPositiveFilter() throws Exception {
      TextFileInputMeta meta = oneFile("1,A\n2,B\n3,A\n", "n", "v");
      meta.getFilters().add(new TextFileFilter(2, "A", false, true));

      assertRows(run(meta, 1), "1|A", "3|A");
    }

    @Test
    void dropsLinesMatchingANegativeFilter() throws Exception {
      TextFileInputMeta meta = oneFile("1,A\n2,B\n3,A\n", "n", "v");
      meta.getFilters().add(new TextFileFilter(2, "A", false, false));

      assertRows(run(meta, 1), "2|B");
    }

    @Test
    void stopsReadingTheFileAtAStopFilter() throws Exception {
      TextFileInputMeta meta = oneFile("1\n2\nSTOP\n3\n", "a");
      meta.getFilters().add(new TextFileFilter(0, "STOP", true, false));

      assertRows(run(meta, 1), "1", "2");
    }
  }

  @Nested
  class LineEndsAndEmptyLines {

    private TextFileInputMeta crlf(String format) throws IOException {
      TextFileInputMeta meta = oneFile("1,x\r\n2,y\r\n", "a", "b");
      meta.getContent().setFileFormat(format);
      return meta;
    }

    @Test
    void readsCrlfInDosFormat() throws Exception {
      assertRows(run(crlf("DOS"), 1), "1|x", "2|y");
    }

    @Test
    void readsCrlfInMixedFormat() throws Exception {
      assertRows(run(crlf("mixed"), 1), "1|x", "2|y");
    }

    @Test
    void readsCrlfInUnixFormatSkippingTheEmptyLines() throws Exception {
      assertRows(run(crlf("unix"), 1), "1|x", "2|y");
    }

    @Test
    void keepsEmptyLinesWhenAsked() throws Exception {
      TextFileInputMeta meta = oneFile("1\n\n2\n", "a");
      meta.getContent().setNoEmptyLines(false);

      assertRows(run(meta, 1), "1", "<null>", "2");
    }

    @Test
    void skipsEmptyLinesByDefault() throws Exception {
      assertRows(run(oneFile("1\n\n2\n", "a"), 1), "1", "2");
    }

    @Test
    void keepsALineBreakInsideAnEnclosedField() throws Exception {
      TextFileInputMeta meta = oneFile("1,\"a\nb\"\n2,c\n", "n", "v");
      meta.getContent().setBreakInEnclosureAllowed(true);

      assertRows(run(meta, 1), "1|a\nb", "2|c");
    }
  }

  @Nested
  class Encodings {

    private Result readEncoded(Charset charset, byte[] bom, String encoding) throws Exception {
      byte[] body = "1,é\n2,日本\n".getBytes(charset);
      byte[] bytes = new byte[bom.length + body.length];
      System.arraycopy(bom, 0, bytes, 0, bom.length);
      System.arraycopy(body, 0, bytes, bom.length, body.length);
      TextFileInputMeta meta = csvMeta("n", "v");
      meta.getContent().setEncoding(encoding);
      addFile(meta, write(dir, "encoded.csv", bytes));
      return run(meta, 1);
    }

    @Test
    void readsUtf8() throws Exception {
      assertRows(readEncoded(StandardCharsets.UTF_8, new byte[0], "UTF-8"), "1|é", "2|日本");
    }

    @Test
    void readsUtf8WithBomAndNoEncodingSet() throws Exception {
      byte[] bom = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
      assertRows(readEncoded(StandardCharsets.UTF_8, bom, null), "1|é", "2|日本");
    }

    @Test
    void readsUtf16LeWithBomAndNoEncodingSet() throws Exception {
      byte[] bom = {(byte) 0xFF, (byte) 0xFE};
      assertRows(readEncoded(StandardCharsets.UTF_16LE, bom, null), "1|é", "2|日本");
    }

    @Test
    void readsUtf16LeWithoutBom() throws Exception {
      assertRows(readEncoded(StandardCharsets.UTF_16LE, new byte[0], "UTF-16LE"), "1|é", "2|日本");
    }

    @Test
    void readsUtf16BeWithoutBom() throws Exception {
      assertRows(readEncoded(StandardCharsets.UTF_16BE, new byte[0], "UTF-16BE"), "1|é", "2|日本");
    }
  }

  @Nested
  class Compression {

    private static final String CONTENT = "1,x\n2,y\n";

    private Result readCompressed(String provider, String name, byte[] bytes) throws Exception {
      TextFileInputMeta meta = csvMeta("a", "b");
      meta.getContent().setFileCompression(provider);
      addFile(meta, write(dir, name, bytes));
      return run(meta, 1);
    }

    @Test
    void readsGzip() throws Exception {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
        gzip.write(CONTENT.getBytes(StandardCharsets.UTF_8));
      }
      assertRows(readCompressed("GZip", "input.csv.gz", out.toByteArray()), "1|x", "2|y");
    }

    @Test
    void readsTheFirstEntryOfAZip() throws Exception {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ZipOutputStream zip = new ZipOutputStream(out)) {
        zip.putNextEntry(new ZipEntry("input.csv"));
        zip.write(CONTENT.getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
        zip.putNextEntry(new ZipEntry("ignored.csv"));
        zip.write("9,z\n".getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
      }
      assertRows(readCompressed("Zip", "input.zip", out.toByteArray()), "1|x", "2|y");
    }

    @Test
    void readsSnappy() throws Exception {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (SnappyOutputStream snappy = new SnappyOutputStream(out)) {
        snappy.write(CONTENT.getBytes(StandardCharsets.UTF_8));
      }
      assertRows(readCompressed("Snappy", "input.snappy", out.toByteArray()), "1|x", "2|y");
    }
  }

  @Nested
  class Copies {

    /**
     * Several copies reading the same files would output every row once per copy: without "Running
     * in parallel?" the transform refuses to start instead.
     */
    @Test
    void refusesSeveralCopiesWithoutParallelReading() throws Exception {
      Result result = run(oneFile("1\n2\n", "a"), 2);

      assertTrue(result.startFailure() != null, "the pipeline must not start");
      assertTrue(result.log().contains("Running in parallel?"), result.log());
    }

    @Test
    void readsFilenamesFromAPreviousTransform() throws Exception {
      Path first = write(dir, "first.csv", "1\n2\n");
      Path second = write(dir, "second.csv", "3\n4\n");

      assertRows(runWithFilenames(csvMeta("a"), 1, List.of(first, second)), "1", "2", "3", "4");
    }

    /** With filenames from a previous transform, several copies share the files between them. */
    @Test
    void sharesFilenamesFromAPreviousTransformBetweenCopies() throws Exception {
      Path first = write(dir, "first.csv", "1\n2\n");
      Path second = write(dir, "second.csv", "3\n4\n");

      Result result = runWithFilenames(csvMeta("a"), 2, List.of(first, second));

      assertEquals(0, result.errors(), result.log());
      List<String> lines = new ArrayList<>(result.lines());
      lines.sort(null);
      assertEquals(List.of("1", "2", "3", "4"), lines);
    }
  }

  @Nested
  class Errors {

    @Test
    void skipsALineThatFailsConversionWhenErrorsAreIgnored() throws Exception {
      TextFileInputMeta meta = csvMeta();
      TextFileInputField number = new TextFileInputField("n", -1, -1);
      number.setType(IValueMeta.TYPE_INTEGER);
      meta.getInputFields().add(number);
      addFile(meta, write(dir, "input.csv", "1\nnot-a-number\n3\n"));
      meta.getErrorHandling().setErrorIgnored(true);
      meta.setErrorLineSkipped(true);

      assertRows(run(meta, 1), "1", "3");
    }

    @Test
    void failsOnAMissingRequiredFile() throws Exception {
      TextFileInputMeta meta = csvMeta("a");
      addFile(meta, dir.resolve("missing.csv"));

      Result result = run(meta, 1);

      assertTrue(result.errors() > 0, "a missing required file must fail the transform");
    }
  }
}

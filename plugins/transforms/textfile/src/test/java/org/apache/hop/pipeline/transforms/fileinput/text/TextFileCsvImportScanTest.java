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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.ProgressNullMonitorListener;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Get Fields samples a CSV once and reports the types it actually found. These guard the two ways
 * that scan used to go wrong on a CRLF customers file: a remote file listing on every sample line,
 * and a summary that still printed the old date-format guess (empty string min/max, and 2199/1900
 * sentinels).
 */
class TextFileCsvImportScanTest {
  private static final List<String> FIELD_NAMES =
      List.of(
          "id",
          "name",
          "firstname",
          "zip",
          "city",
          "birthdate",
          "street",
          "housenr",
          "stateCode",
          "state");

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUp() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void samplesTheRequestedRowsAndResolvesTheFileOnce() throws Exception {
    Scan scan = scan(fixture(), "mixed", 100);
    assertEquals(1, scan.dialog.resolves);
    assertTrue(
        scan.message.contains("Result after scanning 100 lines."),
        () -> "expected to stop at 100 data rows, was:\n" + scan.message);
    assertFalse(scan.message.contains("2199"), scan.message);
    assertFalse(scan.message.contains("More then 1 date format"), scan.message);
    assertTrue(scan.message.contains("name-0001"), scan.message);

    assertEquals(IValueMeta.TYPE_INTEGER, field(scan.meta, "id").getType());
    assertEquals(" #", field(scan.meta, "id").getFormat());
    assertEquals(15, field(scan.meta, "id").getLength());
    assertEquals(0, field(scan.meta, "id").getPrecision());
    assertEquals(IValueMeta.TYPE_STRING, field(scan.meta, "name").getType());
    assertEquals(9, field(scan.meta, "name").getLength());
    assertEquals(IValueMeta.TYPE_DATE, field(scan.meta, "birthdate").getType());
    assertEquals("yyyy/MM/dd", field(scan.meta, "birthdate").getFormat());
  }

  @Test
  void firstSampleIsTheFirstDataRowForDosUnixAndMixed() throws Exception {
    Path file = fixture();
    for (String format : List.of("DOS", "unix", "mixed")) {
      Scan scan = scan(file, format, 1);
      assertTrue(
          scan.message.contains("Result after scanning 1 lines."),
          () -> format + " scanned more than the first data row:\n" + scan.message);
      // "name-0001" is the first data row. The header would be the 4-character word "name", and a
      // blank Unix leftover line would be length 0.
      assertEquals(IValueMeta.TYPE_INTEGER, field(scan.meta, "id").getType(), format);
      assertEquals(9, field(scan.meta, "name").getLength(), format + "\n" + scan.message);
      assertTrue(scan.message.contains("name-0001"), format + "\n" + scan.message);
    }
  }

  /**
   * The progress dialog closes itself when the monitor is done. If the scan does that before
   * returning, the caller sees a null result and leaves the grid on the header names.
   */
  @Test
  void resultIsReturnedBeforeTheProgressDialogCloses() throws Exception {
    TextFileInputMeta meta = newMeta("mixed");
    InputFile inputFile = new InputFile();
    inputFile.setFileName(fixture().toAbsolutePath().toString());
    inputFile.setFileRequired(true);
    meta.getFileInput().getInputFiles().add(inputFile);
    Variables variables = new Variables();
    variables.initializeFrom(null);
    try (InputStreamReader reader =
        new InputStreamReader(Files.newInputStream(fixture()), StandardCharsets.UTF_8)) {
      CountingDialog dialog = new CountingDialog(variables, meta, reader, 20);
      IProgressMonitor monitor =
          new ProgressNullMonitorListener() {
            @Override
            public void done() {
              throw new AssertionError(
                  "closing the dialog from the scan drops the result and keeps only header names");
            }
          };
      String message = dialog.doScan(monitor, false);
      assertTrue(message.contains("Result after scanning 20 lines."));
      assertEquals(IValueMeta.TYPE_INTEGER, field(meta, "id").getType());
      assertEquals(" #", field(meta, "id").getFormat());
      assertEquals(15, field(meta, "id").getLength());
      assertEquals(0, field(meta, "id").getPrecision());
      assertEquals("yyyy/MM/dd", field(meta, "birthdate").getFormat());
      assertEquals(9, field(meta, "name").getLength());
    }
  }

  @Test
  void missingFileFailsOnceInsteadOfPerLine() throws Exception {
    TextFileInputMeta meta = newMeta("mixed");
    Variables variables = new Variables();
    variables.initializeFrom(null);
    try (InputStreamReader reader =
        new InputStreamReader(new ByteArrayInputStream(new byte[0]), StandardCharsets.UTF_8)) {
      CountingDialog dialog = new CountingDialog(variables, meta, reader, 100);
      HopException exception =
          assertThrows(
              HopException.class, () -> dialog.doScan(new ProgressNullMonitorListener(), false));
      assertTrue(exception.getMessage().contains("valid file"), exception.getMessage());
      assertEquals(1, dialog.resolves);
    }
  }

  private static Scan scan(Path file, String format, int samples) throws Exception {
    TextFileInputMeta meta = newMeta(format);
    InputFile inputFile = new InputFile();
    inputFile.setFileName(file.toAbsolutePath().toString());
    inputFile.setFileRequired(true);
    meta.getFileInput().getInputFiles().add(inputFile);

    Variables variables = new Variables();
    variables.initializeFrom(null);
    try (InputStreamReader reader =
        new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8)) {
      CountingDialog dialog = new CountingDialog(variables, meta, reader, samples);
      String message = dialog.doScan(new ProgressNullMonitorListener(), false);
      return new Scan(meta, dialog, message);
    }
  }

  private static TextFileInputMeta newMeta(String format) {
    TextFileInputMeta meta = new TextFileInputMeta();
    meta.getContent().setFileType("CSV");
    meta.getContent().setFileFormat(format);
    meta.getContent().setSeparator(";");
    meta.getContent().setEnclosure("\"");
    meta.getContent().setEscapeCharacter("\\");
    meta.getContent().setHeader(true);
    meta.getContent().setNrHeaderLines(1);
    meta.getContent().setNoEmptyLines(true);
    for (String name : FIELD_NAMES) {
      TextFileInputField field = new TextFileInputField();
      field.setName(name);
      meta.getInputFields().add(field);
    }
    return meta;
  }

  private static TextFileInputField field(TextFileInputMeta meta, String name) {
    return meta.getInputFields().stream()
        .filter(candidate -> name.equals(candidate.getName()))
        .findFirst()
        .orElseThrow();
  }

  /**
   * Checkout may store this fixture with LF (core.autocrlf). The scan under test has to see CRLF,
   * because Unix mode then leaves a blank line after every row.
   */
  private static Path fixture() throws Exception {
    var resource = TextFileCsvImportScanTest.class.getResource("files/customers-crlf.csv");
    if (resource == null) {
      throw new IllegalStateException("customers-crlf.csv fixture is missing");
    }
    String text =
        Files.readString(Path.of(resource.toURI()), StandardCharsets.UTF_8)
            .replace("\r\n", "\n")
            .replace("\n", "\r\n");
    if (!text.contains("\r\n")) {
      throw new IllegalStateException("customers fixture has no lines to scan");
    }
    Path copy = Files.createTempFile("customers-crlf", ".csv");
    Files.writeString(copy, text, StandardCharsets.UTF_8);
    return copy;
  }

  private record Scan(TextFileInputMeta meta, CountingDialog dialog, String message) {}

  private static final class CountingDialog
      extends TextFileCSVImportProgressDialog<TextFileInputField> {
    private int resolves;

    private CountingDialog(
        Variables variables, TextFileInputMeta meta, InputStreamReader reader, int samples) {
      super(null, variables, meta, new PipelineMeta(), reader, samples, true);
    }

    @Override
    protected String resolveSampleFileName() throws HopException {
      resolves++;
      return super.resolveSampleFileName();
    }
  }
}

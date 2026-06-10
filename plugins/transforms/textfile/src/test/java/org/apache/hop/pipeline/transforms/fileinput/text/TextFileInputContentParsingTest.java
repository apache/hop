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

package org.apache.hop.pipeline.transforms.fileinput.text;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TextFileInputContentParsingTest extends BaseTextParsingTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  /**
   * Regression guard: the "Rownum in output" option must actually populate the row-number column.
   * It regressed (commit 6bdc5fd095) because TextFileInputMeta.isIncludeRowNumber() was stubbed to
   * return false, so getFields added the column but convertLineToRow never filled it (null on every
   * row).
   */
  @Test
  void testIncludeRowNumberIsPopulated() throws Exception {
    meta.getContent().setFileFormat("unix");
    meta.getContent().setIncludeRowNumber(true);
    meta.getContent().setRowNumberField("rownr");

    initByFile("default.csv");
    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f3", -1, -1));

    process();

    int idx = data.outputRowMeta.indexOfValue("rownr");
    assertTrue(idx >= 0, "rownr column should be present");
    assertEquals(3, rows.size());
    for (int i = 0; i < rows.size(); i++) {
      assertNotNull(rows.get(i)[idx], "rownr must not be null on row " + i);
      assertEquals((long) (i + 1), ((Number) rows.get(i)[idx]).longValue());
    }
  }

  /**
   * Regression guard for the "include filename in output" option, broken the same way as rownum
   * (TextFileInputMeta.isIncludeFilename() was stubbed to return false).
   */
  @Test
  void testIncludeFilenameIsPopulated() throws Exception {
    meta.getContent().setFileFormat("unix");
    meta.getContent().setIncludeFilename(true);
    meta.getContent().setFilenameField("fname");

    initByFile("default.csv");
    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f3", -1, -1));

    process();

    int idx = data.outputRowMeta.indexOfValue("fname");
    assertTrue(idx >= 0, "fname column should be present");
    assertEquals(3, rows.size());
    for (int i = 0; i < rows.size(); i++) {
      Object value = rows.get(i)[idx];
      assertNotNull(value, "filename must not be null on row " + i);
      assertTrue(value.toString().endsWith("default.csv"), "unexpected filename: " + value);
    }
  }

  /**
   * Regression guard for the additional-output-field misalignment: getFields adds these columns
   * with StringUtils.isNotBlank(...) but the runtime used to add/shift them with a plain != null
   * check. An empty-string field name (how the UI serializes an unused field) would then be written
   * by the runtime even though getFields skipped it, shifting every following column by one. Here a
   * blank short-filename field precedes a real extension field, so the extension column must hold
   * the file extension - not the misaligned short filename.
   */
  @Test
  void testBlankAdditionalFieldDoesNotMisalignColumns() throws Exception {
    meta.getContent().setFileFormat("unix");
    // Unused field serialized as an empty string (not null), preceding a real one.
    meta.getAdditionalOutputFields().setShortFilenameField("");
    meta.getAdditionalOutputFields().setExtensionField("theext");

    initByFile("default.csv");
    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f3", -1, -1));

    process();

    // The blank short-filename field must NOT have produced a column.
    assertTrue(
        data.outputRowMeta.indexOfValue("") < 0, "a blank field name must not create a column");
    int idx = data.outputRowMeta.indexOfValue("theext");
    assertTrue(idx >= 0, "extension column should be present");
    assertEquals(3, rows.size());
    for (int i = 0; i < rows.size(); i++) {
      assertEquals("csv", rows.get(i)[idx], "extension column misaligned on row " + i);
    }
  }

  @Test
  void testDefaultOptions() throws Exception {

    meta.getContent().setFileFormat("unix");

    initByFile("default.csv");

    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"first", "1", "1.1"}, {"second", "2", "2.2"}, {"third", "3", "3.3"}});
  }

  @Test
  void testSeparator() throws Exception {

    meta.getContent().setSeparator(",");
    meta.getContent().setFileFormat("unix");

    initByFile("separator.csv");

    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f2", -1, -1));
    meta.getInputFields().get(2).setDecimalSymbol(".");

    process();

    check(
        new Object[][] {
          {"first", "1", "1.1"}, {"second", "2", "2.2"}, {"third;third", "3", "3.3"}
        });
  }

  @Test
  void testEscape() throws Exception {

    meta.getContent().setEscapeCharacter("\\");
    meta.getContent().setFileFormat("unix");

    initByFile("escape.csv");

    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f2", -1, -1));

    process();

    check(
        new Object[][] {
          {"first", "1", "1.1"}, {"second", "2", "2.2"}, {"third;third", "3", "3.3"}
        });
  }

  @Test
  void testHeader() throws Exception {

    meta.getContent().setHeader(false);
    meta.getContent().setFileFormat("unix");

    initByFile("default.csv");

    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f2", -1, -1));

    process();

    check(
        new Object[][] {
          {"Field 1", "Field 2", "Field 3"},
          {"first", "1", "1.1"},
          {"second", "2", "2.2"},
          {"third", "3", "3.3"}
        });
  }

  @Test
  void testGzipCompression() throws Exception {

    meta.getContent().setFileCompression("GZip");
    initByFile("default.csv.gz");

    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"first", "1", "1.1"}, {"second", "2", "2.2"}, {"third", "3", "3.3"}});
  }

  @Test
  void testVfsGzipCompression() throws Exception {

    meta.getContent().setFileCompression("None");
    String url = "gz:" + this.getClass().getResource(inPrefix + "default.csv.gz");
    initByURL(url);

    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"first", "1", "1.1"}, {"second", "2", "2.2"}, {"third", "3", "3.3"}});
  }

  @Test
  void testVfsBzip2Compression() throws Exception {

    meta.getContent().setFileCompression("None");
    String url = "bz2:" + this.getClass().getResource(inPrefix + "default.csv.bz2");
    initByURL(url);

    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"first", "1", "1.1"}, {"second", "2", "2.2"}, {"third", "3", "3.3"}});
  }

  @Test
  void testFixedWidth() throws Exception {
    meta.getContent().setFileType("Fixed");
    meta.getContent().setFileFormat("unix");

    initByFile("fixed.csv");

    setFields(
        new TextFileInputField("f1", 0, 7),
        new TextFileInputField("f2", 8, 7),
        new TextFileInputField("f3", 16, 7));

    process();

    check(
        new Object[][] {
          {"first  ", "1      ", "1.1"},
          {"second ", "2      ", "2.2"},
          {"third  ", "3      ", "3.3"}
        });
  }

  @Test
  void testFixedWidthBytes() throws Exception {

    meta.getContent().setHeader(false);
    meta.getContent().setFileType("Fixed");
    meta.getContent().setFileFormat("Unix");
    meta.getContent().setEncoding("Shift_JIS");
    meta.getContent().setLength("Bytes");
    initByFile("test-fixed-length-bytes.txt");

    setFields(
        new TextFileInputField("f1", 0, 5),
        new TextFileInputField("f2", 5, 3),
        new TextFileInputField("f3", 8, 1),
        new TextFileInputField("f4", 9, 3));

    process();

    check(new Object[][] {{"1.000", "個 ", "T", "1.0"}, {"2.000", "M  ", "Z", "1.0"}});
  }

  @Test
  void testFixedWidthCharacters() throws Exception {
    meta.getContent().setHeader(false);
    meta.getContent().setFileType("Fixed");
    meta.getContent().setFileFormat("DOS");
    meta.getContent().setEncoding("ISO-8859-1");
    meta.getContent().setLength("Characters");
    meta.getContent().setFileFormat("unix");

    initByFile("test-fixed-length-characters.txt");

    setFields(
        new TextFileInputField("f1", 0, 3),
        new TextFileInputField("f2", 3, 2),
        new TextFileInputField("f3", 5, 2),
        new TextFileInputField("f4", 7, 4));

    process();
    check(new Object[][] {{"ABC", "DE", "FG", "HIJK"}, {"LmN", "oP", "qR", "sTuV"}});
  }

  @Test
  void testFilterEmptyBacklog5381() throws Exception {
    meta.getContent().setHeader(false);
    meta.getContent().setFileType("Fixed");
    meta.getContent().setNoEmptyLines(true);
    meta.getContent().setFileFormat("mixed");
    initByFile("filterempty-BACKLOG-5381.csv");

    setFields(new TextFileInputField("f", 0, 100));

    process();

    check(
        new Object[][] {
          {"FirstLine => FirstLine "},
          {"ThirdLine => SecondLine"},
          {"SixthLine => ThirdLine"},
          {"NinthLine => FourthLine"},
          {""}
        });
  }

  @Test
  void testFilterVariables() throws Exception {

    meta.getContent().setFileFormat("unix");

    initByFile("default.csv");

    Variables vars = new Variables();
    vars.setVariable("VAR_TEST", "second");
    data.filterProcessor =
        new TextFileFilterProcessor(
            List.of(new TextFileFilter(0, "${VAR_TEST}", false, false)), vars);

    setFields(
        new TextFileInputField("f1", -1, -1),
        new TextFileInputField("f2", -1, -1),
        new TextFileInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"first", "1", "1.1"}, {"third", "3", "3.3"}});
  }

  @Test
  void testBOM_UTF8() throws Exception {

    meta.getContent().setEncoding("UTF-32LE");
    meta.getContent().setHeader(false);
    initByFile("test-BOM-UTF-8.txt");

    setFields(new TextFileInputField("f1", -1, -1), new TextFileInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"data", "1"}});
  }

  @Test
  void testBOM_UTF16BE() throws Exception {

    meta.getContent().setEncoding("UTF-32LE");
    meta.getContent().setHeader(false);
    initByFile("test-BOM-UTF-16BE.txt");

    setFields(new TextFileInputField("f1", -1, -1), new TextFileInputField("f2", -1, -1));

    process();

    check(new Object[][] {{"data", "1"}});
  }
}

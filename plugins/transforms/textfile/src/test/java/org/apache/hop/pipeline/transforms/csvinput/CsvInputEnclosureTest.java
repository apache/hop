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

package org.apache.hop.pipeline.transforms.csvinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class CsvInputEnclosureTest extends CsvInputUnitTestBase {
  private static final String QUOTATION_AND_EXCLAMATION_MARK = "\"!";
  private static final String QUOTATION_MARK = "\"";
  private static final String SEMICOLON = ";";

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private CsvInput csvInput;
  private TransformMockHelper<CsvInputMeta, CsvInputData> transformMockHelper;

  @BeforeEach
  void setUp() throws Exception {
    transformMockHelper =
        TransformMockUtil.getTransformMockHelper(
            CsvInputMeta.class, CsvInputData.class, "CsvInputEnclosureTest");
  }

  @AfterEach
  void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Test
  void hasEnclosures_HasNewLine() throws Exception {
    doTest("\"value1\";\"value2\"\n", QUOTATION_MARK);
  }

  @Test
  void hasEnclosures_HasNotNewLine() throws Exception {
    doTest("\"value1\";\"value2\"", QUOTATION_MARK);
  }

  @Test
  void hasNotEnclosures_HasNewLine() throws Exception {
    doTest("value1;value2\n", QUOTATION_MARK);
  }

  @Test
  void hasNotEnclosures_HasNotNewLine() throws Exception {
    doTest("value1;value2", QUOTATION_MARK);
  }

  @Test
  void hasMultiSymbolsEnclosureWithoutEnclosureAndEndFile() throws Exception {
    doTest("value1;value2", QUOTATION_AND_EXCLAMATION_MARK);
  }

  @Test
  void hasMultiSymbolsEnclosureWithEnclosureAndWithoutEndFile() throws Exception {
    doTest("\"!value1\"!;value2", QUOTATION_AND_EXCLAMATION_MARK);
  }

  @Test
  void hasMultiSymbolsEnclosurewithEnclosureInBothfield() throws Exception {
    doTest("\"!value1\"!;\"!value2\"!", QUOTATION_AND_EXCLAMATION_MARK);
  }

  @Test
  void hasMultiSymbolsEnclosureWithoutEnclosureAndWithEndfileRN() throws Exception {
    doTest("value1;value2\r\n", QUOTATION_AND_EXCLAMATION_MARK);
  }

  @Test
  void hasMultiSymbolsEnclosureWithEnclosureAndWithEndfileRN() throws Exception {
    doTest("value1;\"!value2\"!\r\n", QUOTATION_AND_EXCLAMATION_MARK);
  }

  @Test
  void hasMultiSymbolsEnclosureWithoutEnclosureAndWithEndfileN() throws Exception {
    doTest("value1;value2\n", QUOTATION_AND_EXCLAMATION_MARK);
  }

  @Test
  void hasMultiSymbolsEnclosureWithEnclosureAndWithEndfileN() throws Exception {
    doTest("value1;\"!value2\"!\n", QUOTATION_AND_EXCLAMATION_MARK);
  }

  public void doTest(String content, String enclosure) throws Exception {
    IRowSet output = new QueueRowSet();

    File tmp = createTestFile("utf-8", content);
    try {
      CsvInputMeta meta = createMeta(tmp, createInputFileFields("f1", "f2"), enclosure);
      CsvInputData data = new CsvInputData();
      csvInput =
          new CsvInput(
              transformMockHelper.transformMeta,
              meta,
              data,
              0,
              transformMockHelper.pipelineMeta,
              transformMockHelper.pipeline);
      csvInput.init();

      csvInput.addRowSetToOutputRowSets(output);

      try {
        csvInput.processRow();
      } finally {
        csvInput.dispose();
      }
    } finally {
      tmp.delete();
    }

    Object[] row = output.getRowImmediate();
    assertNotNull(row);
    assertEquals("value1", row[0]);
    assertEquals("value2", row[1]);

    assertNull(output.getRowImmediate());
  }

  private CsvInputMeta createMeta(File file, TextFileInputField[] fields, String enclosure) {
    CsvInputMeta meta = createMeta(file, fields);
    meta.setDelimiter(SEMICOLON);
    meta.setEnclosure(enclosure);
    return meta;
  }
}

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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CsvInputTest extends CsvInputUnitTestBase {

  private TransformMockHelper<CsvInputMeta, CsvInputData> transformMockHelper;
  private ILogChannel logChannelInterface;
  private CsvInputMeta csvInputMeta;

  @BeforeEach
  public void setUp() throws Exception {
    logChannelInterface = mock(ILogChannel.class);
    transformMockHelper =
        TransformMockUtil.getTransformMockHelper(
            CsvInputMeta.class, CsvInputData.class, "CsvInputTest");
    csvInputMeta = mock(CsvInputMeta.class);
  }

  @AfterEach
  public void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void guessStringsFromLineWithEmptyLine() throws Exception {
    // This only validates that, given a null 'line', a null is returned!
    String[] saData =
        TextFileLineUtil.guessStringsFromLine(
            logChannelInterface,
            null,
            csvInputMeta.getDelimiter(),
            csvInputMeta.getEnclosure(),
            csvInputMeta.getEscapeCharacter());

    assertNull(saData);
  }

  @Test
  public void testFileIsReleasedAfterProcessing() throws Exception {
    // Create a file with some content to be processed
    TextFileInputField[] inputFileFields = createInputFileFields("f1", "f2", "f3");
    String fileContents = "Something" + DELIMITER + "" + DELIMITER + "The former was empty!";
    File tmpFile = createTestFile(ENCODING, fileContents);

    // Create and configure the transform
    CsvInputMeta meta = createMeta(tmpFile, inputFileFields);
    CsvInputData data = new CsvInputData();
    IRowSet output = new QueueRowSet();
    CsvInput csvInput =
        new CsvInput(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    csvInput.init();
    csvInput.addRowSetToOutputRowSets(output);

    // Start processing
    csvInput.processRow();

    // Finish processing
    csvInput.dispose();

    // And now the file must be free to be deleted
    assertTrue(tmpFile.delete());
    assertFalse(tmpFile.exists());
  }
}

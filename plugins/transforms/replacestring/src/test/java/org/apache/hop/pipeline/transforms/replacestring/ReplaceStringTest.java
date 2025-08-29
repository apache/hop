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

package org.apache.hop.pipeline.transforms.replacestring;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** User: Dzmitry Stsiapanau Date: 1/31/14 Time: 11:19 AM */
class ReplaceStringTest {

  private static final String LITERAL_STRING = "[a-z]{2,7}";

  private static final String INPUT_STRING = "This is String This Is String THIS IS STRING";

  private Object[] row = new Object[] {"some data", "another data"};

  private Object[] expectedRow =
      new Object[] {
        "some data",
        "1nother d1t1",
        "1no2her d121",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
      };

  private TransformMockHelper<ReplaceStringMeta, ReplaceStringData> transformMockHelper;

  @BeforeEach
  void setUp() throws Exception {
    transformMockHelper =
        new TransformMockHelper<>(
            "REPLACE STRING TEST", ReplaceStringMeta.class, ReplaceStringData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    verify(transformMockHelper.iLogChannel, never()).logError(anyString());
    verify(transformMockHelper.iLogChannel, never()).logError(anyString(), any(Object[].class));
    verify(transformMockHelper.iLogChannel, never()).logError(anyString(), (Throwable) any());
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() throws Exception {
    transformMockHelper.cleanUp();
  }

  @Test
  void testGetOneRow() throws Exception {
    ReplaceStringData data = new ReplaceStringData();
    ReplaceStringMeta meta = new ReplaceStringMeta();

    ReplaceString replaceString =
        new ReplaceString(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(0, new ValueMetaString("SomeDataMeta"));
    inputRowMeta.addValueMeta(1, new ValueMetaString("AnotherDataMeta"));
    replaceString.init();
    replaceString.setInputRowMeta(inputRowMeta);
    data.outputRowMeta = inputRowMeta;
    data.outputRowMeta.addValueMeta(new ValueMetaString("AnotherDataMeta"));
    data.inputFieldsNr = 2;
    data.numFields = 2;
    data.inStreamNrs = new int[] {1, 1};
    data.patterns = new Pattern[] {Pattern.compile("a"), Pattern.compile("t")};
    data.replaceFieldIndex = new int[] {-1, -1};
    data.outStreamNrs = new String[] {"", "1"};
    data.replaceByString = new String[] {"1", "2"};
    data.setEmptyString = new boolean[] {false, false};
    // when( inputRowMeta.size() ).thenReturn( 3 );
    // when( inputRowMeta.getString( anyObject(), 1 ) ).thenReturn((String) row[1]);

    Object[] output = replaceString.handleOneRow(inputRowMeta, row);
    assertArrayEquals(expectedRow, output, "Output varies");
  }

  @Test
  void testBuildPatternWithLiteralParsingAndWholeWord() throws Exception {
    Pattern actualPattern = ReplaceString.buildPattern(true, true, true, LITERAL_STRING, false);
    Matcher matcher = actualPattern.matcher(INPUT_STRING);
    String actualString = matcher.replaceAll("are");
    assertEquals(INPUT_STRING, actualString);
  }

  @Test
  void testBuildPatternWithNonLiteralParsingAndWholeWord() throws Exception {
    Pattern actualPattern = ReplaceString.buildPattern(false, true, true, LITERAL_STRING, false);
    Matcher matcher = actualPattern.matcher(INPUT_STRING);
    String actualString = matcher.replaceAll("are");
    assertEquals("This are String This Is String THIS IS STRING", actualString);
  }
}

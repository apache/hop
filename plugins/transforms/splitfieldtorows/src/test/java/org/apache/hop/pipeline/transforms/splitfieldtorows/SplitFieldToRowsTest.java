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

package org.apache.hop.pipeline.transforms.splitfieldtorows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SplitFieldToRowsTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<SplitFieldToRowsMeta, SplitFieldToRowsData> transformMockHelper;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setup() {
    transformMockHelper =
        new TransformMockHelper<>(
            "Test SplitFieldToRows", SplitFieldToRowsMeta.class, SplitFieldToRowsData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  void interpretsNullDelimiterAsEmpty() throws Exception {
    SplitFieldToRows transform =
        new SplitFieldToRows(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    transform.init();

    SplitFieldToRowsMeta meta = new SplitFieldToRowsMeta();
    meta.setDelimiter(null);
    meta.setIsDelimiterRegex(false);

    transform.init();

    // empty string should be quoted --> \Q\E
    assertEquals("\\Q\\E", transform.getData().delimiterPattern.pattern());
  }

  @Test
  void splitsWithoutEnclosure() throws Exception {
    List<Object[]> rows = executeSplit("a,b,c", ",", null, false);
    assertEquals(List.of("a", "b", "c"), values(rows));
  }

  @Test
  void splitsQuotedValuesWithEnclosure() throws Exception {
    List<Object[]> rows = executeSplit("hi,\"hello, world\",\"hey\"", ",", "\"", false);
    assertEquals(List.of("hi", "hello, world", "hey"), values(rows));
  }

  @Test
  void removesEnclosureFromSimpleQuotedValues() throws Exception {
    List<Object[]> rows = executeSplit("\"a\",\"b\",\"c\"", ",", "\"", false);
    assertEquals(List.of("a", "b", "c"), values(rows));
  }

  @Test
  void keepsDelimiterInsideEnclosure() throws Exception {
    List<Object[]> rows = executeSplit("x,\"y,z\",w", ",", "\"", false);
    assertEquals(List.of("x", "y,z", "w"), values(rows));
  }

  @Test
  void splitsQuotedValuesIntoFourRowsWithoutEnclosure() throws Exception {
    List<Object[]> rows = executeSplit("hi,\"hello, world\",\"hey\"", ",", null, false);
    assertEquals(List.of("hi", "\"hello", " world\"", "\"hey\""), values(rows));
  }

  @Test
  void splitsWithRegexDelimiter() throws Exception {
    List<Object[]> rows = executeSplit("a, b,c", ",\\s*", null, true);
    assertEquals(List.of("a", "b", "c"), values(rows));
  }

  @Test
  void ignoresEnclosureWhenDelimiterIsRegex() throws Exception {
    List<Object[]> rows = executeSplit("hi,\"hello, world\",\"hey\"", ",", "\"", true);
    assertEquals(List.of("hi", "\"hello", " world\"", "\"hey\""), values(rows));
  }

  @Test
  void includesResetRowNumbers() throws Exception {
    SplitFieldToRowsMeta meta = createMeta(",", "\"", false);
    meta.setIncludeRowNumber(true);
    meta.setRowNumberField("rowNr");
    meta.setResetRowNumber(true);

    List<Object[]> rows = executeSplit(meta, "hi,\"hello, world\",\"hey\"");
    assertEquals(3, rows.size());
    assertEquals("hi", rows.get(0)[1]);
    assertEquals(1L, rows.get(0)[2]);
    assertEquals("hello, world", rows.get(1)[1]);
    assertEquals(2L, rows.get(1)[2]);
    assertEquals("hey", rows.get(2)[1]);
    assertEquals(3L, rows.get(2)[2]);
  }

  private List<Object[]> executeSplit(
      String value, String delimiter, String enclosure, boolean delimiterIsRegex) throws Exception {
    return executeSplit(createMeta(delimiter, enclosure, delimiterIsRegex), value);
  }

  private List<Object[]> executeSplit(SplitFieldToRowsMeta meta, String value) throws Exception {
    SplitFieldToRowsData data = new SplitFieldToRowsData();
    when(transformMockHelper.transformMeta.getTransform()).thenReturn(meta);

    SplitFieldToRows transform =
        new SplitFieldToRows(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    transform.init();

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("csv"));
    transform.setInputRowMeta(input);

    BlockingRowSet output = new BlockingRowSet(20);
    transform.setOutputRowSets(Collections.singletonList(output));

    SplitFieldToRows spyTransform = spy(transform);
    doReturn(new Object[] {value}).doReturn(null).when(spyTransform).getRow();

    assertTrue(spyTransform.processRow());
    assertFalse(spyTransform.processRow());

    List<Object[]> result = new ArrayList<>();
    Object[] row;
    while ((row = output.getRowImmediate()) != null) {
      result.add(row);
    }
    return result;
  }

  private static SplitFieldToRowsMeta createMeta(
      String delimiter, String enclosure, boolean delimiterIsRegex) {
    SplitFieldToRowsMeta meta = new SplitFieldToRowsMeta();
    meta.setSplitField("csv");
    meta.setDelimiter(delimiter);
    meta.setEnclosure(enclosure);
    meta.setNewFieldname("value");
    meta.setIsDelimiterRegex(delimiterIsRegex);
    meta.setIncludeRowNumber(false);
    return meta;
  }

  private static List<String> values(List<Object[]> rows) {
    List<String> values = new ArrayList<>();
    for (Object[] row : rows) {
      values.add((String) row[1]);
    }
    return values;
  }
}

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
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
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

  private IRowMeta lastOutputRowMeta;

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
  void interpretsNullDelimiterAsEmpty() {
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
  void preservesTrailingEmptyValuesWithoutEnclosure() throws Exception {
    List<Object[]> rows = executeSplit("a,b,,", ",", null, false);
    assertEquals(List.of("a", "b", "", ""), values(rows));
  }

  @Test
  void preservesTrailingEmptyValuesWithEnclosure() throws Exception {
    List<Object[]> rows = executeSplit("a,b,,", ",", "\"", false);
    assertEquals(List.of("a", "b", "", ""), values(rows));
  }

  @Test
  void splitsLoneDelimiterIntoTwoEmptyValuesWithEnclosure() throws Exception {
    List<Object[]> rows = executeSplit(",", ",", "\"", false);
    assertEquals(List.of("", ""), values(rows));
  }

  @Test
  void keepsRemainderAndLogsUnterminatedEnclosure() throws Exception {
    List<Object[]> rows = executeSplit("a,\"b", ",", "\"", false);
    assertEquals(List.of("a", "b"), values(rows));
    verify(transformMockHelper.iLogChannel).logError(contains("Unterminated enclosure"));
  }

  @Test
  void doesNotDropRowOnStrayEnclosure() throws Exception {
    List<Object[]> rows = executeSplit("a\"b,c", ",", "\"", false);
    assertEquals(List.of("ab,c"), values(rows));
    verify(transformMockHelper.iLogChannel).logError(contains("Unterminated enclosure"));
  }

  @Test
  void unescapesDoubledEnclosureInsideQuotedValue() throws Exception {
    List<Object[]> rows = executeSplit("\"a,b\",\"c\"\"d\"", ",", "\"", false);
    assertEquals(List.of("a,b", "c\"d"), values(rows));
  }

  @Test
  void resolvesEnclosureFromVariable() throws Exception {
    List<Object[]> rows =
        executeSplit(
            createMeta(",", "${ENCL}", false), "hi,\"hello, world\",\"hey\"", Map.of("ENCL", "\""));
    assertEquals(List.of("hi", "hello, world", "hey"), values(rows));
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

  @Test
  void keepsSurroundingFieldsAndAppendsSplitValue() throws Exception {
    SplitFieldToRowsMeta meta = createMeta(",", null, false);
    List<Object[]> rows =
        executeSplit(meta, row("id", "csv", "name"), new Object[] {"1", "a,b", "n"}, Map.of());

    assertEquals(List.of("1", "a,b", "n", "a"), prefix(rows.get(0), 4));
    assertEquals(List.of("1", "a,b", "n", "b"), prefix(rows.get(1), 4));
    assertEquals(List.of("id", "csv", "name", "value"), fieldNames());
  }

  @Test
  void excludesSplitFieldFromOutput() throws Exception {
    SplitFieldToRowsMeta meta = createMeta(",", null, false);
    meta.setExcludeSplitField(true);
    List<Object[]> rows =
        executeSplit(meta, row("id", "csv", "name"), new Object[] {"1", "a,b", "n"}, Map.of());

    assertEquals(List.of("1", "n", "a"), prefix(rows.get(0), 3));
    assertEquals(List.of("1", "n", "b"), prefix(rows.get(1), 3));
    assertEquals(List.of("id", "name", "value"), fieldNames());
  }

  @Test
  void excludesSplitFieldAndKeepsRowNumbers() throws Exception {
    SplitFieldToRowsMeta meta = createMeta(",", null, false);
    meta.setExcludeSplitField(true);
    meta.setIncludeRowNumber(true);
    meta.setRowNumberField("rowNr");
    meta.setResetRowNumber(true);
    List<Object[]> rows =
        executeSplit(meta, row("id", "csv", "name"), new Object[] {"1", "a,b", "n"}, Map.of());

    assertEquals(List.of("1", "n", "a", 1L), prefix(rows.get(0), 4));
    assertEquals(List.of("1", "n", "b", 2L), prefix(rows.get(1), 4));
    assertEquals(List.of("id", "name", "value", "rowNr"), fieldNames());
  }

  @Test
  void excludesSplitFieldResolvedFromVariable() throws Exception {
    SplitFieldToRowsMeta meta = createMeta(",", null, false);
    meta.setSplitField("${COL}");
    meta.setExcludeSplitField(true);
    List<Object[]> rows =
        executeSplit(meta, row("id", "csv"), new Object[] {"1", "a,b"}, Map.of("COL", "csv"));

    assertEquals(List.of("1", "a"), prefix(rows.get(0), 2));
    assertEquals(List.of("1", "b"), prefix(rows.get(1), 2));
    assertEquals(List.of("id", "value"), fieldNames());
  }

  private List<Object[]> executeSplit(
      String value, String delimiter, String enclosure, boolean delimiterIsRegex) throws Exception {
    return executeSplit(createMeta(delimiter, enclosure, delimiterIsRegex), value);
  }

  private List<Object[]> executeSplit(SplitFieldToRowsMeta meta, String value) throws Exception {
    return executeSplit(meta, value, Map.of());
  }

  private List<Object[]> executeSplit(
      SplitFieldToRowsMeta meta, String value, Map<String, String> variables) throws Exception {
    return executeSplit(meta, row("csv"), new Object[] {value}, variables);
  }

  private List<Object[]> executeSplit(
      SplitFieldToRowsMeta meta, RowMeta input, Object[] inputRow, Map<String, String> variables)
      throws Exception {
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
    variables.forEach(transform::setVariable);
    transform.init();

    transform.setInputRowMeta(input);

    BlockingRowSet output = new BlockingRowSet(20);
    transform.setOutputRowSets(Collections.singletonList(output));

    SplitFieldToRows spyTransform = spy(transform);
    doReturn(inputRow).doReturn(null).when(spyTransform).getRow();

    assertTrue(spyTransform.processRow());
    assertFalse(spyTransform.processRow());

    lastOutputRowMeta = data.outputRowMeta;
    List<Object[]> result = new ArrayList<>();
    Object[] row;
    while ((row = output.getRowImmediate()) != null) {
      result.add(row);
    }
    return result;
  }

  private static RowMeta row(String... names) {
    RowMeta input = new RowMeta();
    for (String name : names) {
      input.addValueMeta(new ValueMetaString(name));
    }
    return input;
  }

  private static List<Object> prefix(Object[] row, int size) {
    return Arrays.asList(row).subList(0, size);
  }

  private List<String> fieldNames() {
    return List.of(lastOutputRowMeta.getFieldNames());
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

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

package org.apache.hop.pipeline.transforms.jsoninput.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Option;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputField;
import org.apache.hop.pipeline.transforms.jsoninput.exception.JsonInputException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FastJsonReaderTest {
  private static final Option[] DEFAULT_OPTIONS = {
    Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
  };
  private static final Option[] OPTIONS_WO_DEFAULT_PATH_LEAF_TO_NULL = {
    Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST
  };
  private final EnumSet<Option> expectedOptions = EnumSet.noneOf(Option.class);
  private JsonInputField[] fields;

  private FastJsonReader fJsonReader;
  private final ILogChannel logMock = mock(ILogChannel.class);

  @BeforeEach
  void setUp() {
    fields = new JsonInputField[] {};
  }

  @Test
  void testFastJsonReaderCreated_Default() {
    fJsonReader = new FastJsonReader(logMock);
    expectedOptions.addAll(Arrays.asList(DEFAULT_OPTIONS));
    assertNotNull(fJsonReader);
    assertFalse(fJsonReader.isIgnoreMissingPath());
    assertTrue(fJsonReader.isDefaultPathLeafToNull());
    assertEquals(expectedOptions, fJsonReader.getJsonConfiguration().getOptions());
  }

  @Test
  void testFastJsonReaderCreated_WithInputFields() throws HopException {
    expectedOptions.addAll(Arrays.asList(DEFAULT_OPTIONS));
    fJsonReader = new FastJsonReader(fields, logMock);
    assertNotNull(fJsonReader);
    assertFalse(fJsonReader.isIgnoreMissingPath());
    assertTrue(fJsonReader.isDefaultPathLeafToNull());
    assertEquals(expectedOptions, fJsonReader.getJsonConfiguration().getOptions());
  }

  @Test
  void testFastJsonReaderCreated_WithDefaultPathLeafToNullFalse() throws HopException {
    expectedOptions.addAll(Arrays.asList(OPTIONS_WO_DEFAULT_PATH_LEAF_TO_NULL));
    fJsonReader = new FastJsonReader(fields, false, logMock);
    assertNotNull(fJsonReader);
    assertFalse(fJsonReader.isIgnoreMissingPath());
    assertFalse(fJsonReader.isDefaultPathLeafToNull());
    assertEquals(expectedOptions, fJsonReader.getJsonConfiguration().getOptions());
  }

  @Test
  void testFastJsonReaderCreated_WithDefaultPathLeafToNullTrue() throws HopException {
    expectedOptions.addAll(Arrays.asList(DEFAULT_OPTIONS));
    fJsonReader = new FastJsonReader(fields, true, logMock);
    assertNotNull(fJsonReader);
    assertFalse(fJsonReader.isIgnoreMissingPath());
    assertTrue(fJsonReader.isDefaultPathLeafToNull());
    assertEquals(expectedOptions, fJsonReader.getJsonConfiguration().getOptions());
  }

  @Test
  void testFastJsonReaderGetMaxRowSize() {
    List<List<Integer>> mainList = new ArrayList<>();
    List<Integer> l1 = new ArrayList<>();
    List<Integer> l2 = new ArrayList<>();
    List<Integer> l3 = new ArrayList<>();
    l1.add(1);
    l2.add(1);
    l2.add(2);
    l3.add(1);
    l3.add(2);
    l3.add(3);
    mainList.add(l1);
    mainList.add(l2);
    mainList.add(l3);
    assertEquals(3, FastJsonReader.getMaxRowSize(Collections.singletonList(mainList)));
  }

  private static final String ARRAY_JSON = "{\"someArray\":[{\"x\":1},{\"x\":2}]}";

  private static IRowSet readString(String json, String path) throws HopException {
    JsonInputField field = new JsonInputField("value");
    field.setPath(path);
    FastJsonReader reader =
        new FastJsonReader(new JsonInputField[] {field}, mock(ILogChannel.class));
    return reader.parseStringValue(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void testLengthFunctionPathReturnsNumberOfArrayElements() throws Exception {
    IRowSet rowSet = readString(ARRAY_JSON, "$.someArray.length()");
    Object[] row = rowSet.getRow();
    assertNotNull(row);
    assertEquals(2, ((Number) row[0]).intValue());
  }

  @Test
  void testSumFunctionPathIsEvaluated() throws Exception {
    IRowSet rowSet = readString("{\"values\":[1,2,4]}", "$.values.sum()");
    Object[] row = rowSet.getRow();
    assertNotNull(row);
    assertEquals(7, ((Number) row[0]).intValue());
  }

  @Test
  void testFunctionPathReturnsSingleRow() throws Exception {
    IRowSet rowSet = readString(ARRAY_JSON, "$.someArray.length()");
    assertNotNull(rowSet.getRow());
    assertNull(rowSet.getRow());
  }

  @Test
  void testLengthFunctionPathOnJsonNodeInput() throws Exception {
    JsonInputField field = new JsonInputField("value");
    field.setPath("$.someArray.length()");
    FastJsonReader reader =
        new FastJsonReader(new JsonInputField[] {field}, mock(ILogChannel.class));
    IRowSet rowSet = reader.parseJsonNodeValue(new ObjectMapper().readTree(ARRAY_JSON));
    Object[] row = rowSet.getRow();
    assertNotNull(row);
    assertEquals(2, ((Number) row[0]).intValue());
  }

  @Test
  void testRegularPathStillReturnsOneRowPerMatch() throws Exception {
    IRowSet rowSet = readString(ARRAY_JSON, "$.someArray[*].x");
    assertNotNull(rowSet.getRow());
    assertNotNull(rowSet.getRow());
    assertNull(rowSet.getRow());
  }

  @Test
  void testFailingFunctionPathIsSuppressedAsNullValue() throws Exception {
    JsonInputField field = new JsonInputField("value");
    field.setPath("$.emptyList.sum()");
    FastJsonReader reader =
        new FastJsonReader(new JsonInputField[] {field}, mock(ILogChannel.class));
    reader.setIgnoreMissingPath(true);
    IRowSet rowSet =
        reader.parseStringValue(
            new ByteArrayInputStream("{\"emptyList\":[]}".getBytes(StandardCharsets.UTF_8)));
    Object[] row = rowSet.getRow();
    assertNotNull(row);
    assertNull(row[0]);
  }

  private static final String ITEMS_JSON =
      "{\"items\":[{\"name\":\"ab\",\"p\":1,\"tags\":[1,2]},"
          + "{\"name\":\"abc\",\"p\":2,\"tags\":[1,2,3]},"
          + "{\"name\":\"abcd\",\"p\":4,\"tags\":[]}],\"empty\":[],\"matrix\":[[1,2],[3]],"
          + "\"obj\":{\"a\":1,\"b\":2}}";

  private static FastJsonReader reader(boolean ignoreMissingPath, String... paths)
      throws HopException {
    JsonInputField[] fields = new JsonInputField[paths.length];
    for (int i = 0; i < paths.length; i++) {
      fields[i] = new JsonInputField("value" + i);
      fields[i].setPath(paths[i]);
    }
    FastJsonReader reader = new FastJsonReader(fields, mock(ILogChannel.class));
    reader.setIgnoreMissingPath(ignoreMissingPath);
    return reader;
  }

  private static List<Object[]> rows(IRowSet rowSet) {
    List<Object[]> rows = new ArrayList<>();
    Object[] row;
    while ((row = rowSet.getRow()) != null) {
      rows.add(row);
    }
    return rows;
  }

  private static List<Object[]> readRows(String json, boolean ignoreMissingPath, String... paths)
      throws HopException {
    return rows(
        reader(ignoreMissingPath, paths)
            .parseStringValue(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))));
  }

  private static List<Object[]> readNodeRows(
      String json, boolean ignoreMissingPath, String... paths) throws Exception {
    return rows(
        reader(ignoreMissingPath, paths).parseJsonNodeValue(new ObjectMapper().readTree(json)));
  }

  @Test
  void testFunctionBehindWildcardGivesOneValuePerMatch() throws Exception {
    List<Object[]> rows = readRows(ITEMS_JSON, false, "$.items[*].tags.length()");
    assertEquals(3, rows.size());
    assertEquals(2, ((Number) rows.get(0)[0]).intValue());
    assertEquals(3, ((Number) rows.get(1)[0]).intValue());
    assertEquals(0, ((Number) rows.get(2)[0]).intValue());
  }

  @Test
  void testFunctionBehindWildcardOnJsonNodeInput() throws Exception {
    List<Object[]> rows = readNodeRows(ITEMS_JSON, false, "$.items[*].tags.length()");
    assertEquals(3, rows.size());
    assertEquals(0, ((JsonNode) rows.get(2)[0]).intValue());
  }

  @Test
  void testFunctionBehindWildcardCombinesWithRegularField() throws Exception {
    List<Object[]> rows =
        readRows(ITEMS_JSON, false, "$.items[*].name", "$.items[*].tags.length()");
    assertEquals(3, rows.size());
    assertEquals("abc", rows.get(1)[0]);
    assertEquals(3, ((Number) rows.get(1)[1]).intValue());
  }

  @Test
  void testAggregationOverDeepScan() throws Exception {
    List<Object[]> rows = readRows(ITEMS_JSON, false, "$..p.sum()");
    assertEquals(1, rows.size());
    assertEquals(7, ((Number) rows.get(0)[0]).intValue());
  }

  @Test
  void testAggregationWithPathParameter() throws Exception {
    List<Object[]> rows = readRows(ITEMS_JSON, false, "$.max($.items[*].p)");
    assertEquals(1, rows.size());
    assertEquals(4, ((Number) rows.get(0)[0]).intValue());
  }

  @Test
  void testFunctionReturningArrayGivesOneRow() throws Exception {
    List<Object[]> rows = readRows(ITEMS_JSON, false, "$.matrix.first()");
    assertEquals(1, rows.size());
    assertEquals(List.of(1, 2), rows.get(0)[0]);
  }

  @Test
  void testKeysGivesOneRowForStringAndJsonNodeInput() throws Exception {
    List<Object[]> stringRows = readRows(ITEMS_JSON, false, "$.obj.keys()");
    List<Object[]> nodeRows = readNodeRows(ITEMS_JSON, false, "$.obj.keys()");
    assertEquals(1, stringRows.size());
    assertEquals(1, nodeRows.size());
  }

  @Test
  void testFirstOnEmptyArrayIsMissingValueWhenIgnoringMissingPath() throws Exception {
    List<Object[]> rows = readRows(ITEMS_JSON, true, "$.empty.first()");
    assertEquals(1, rows.size());
    assertNull(rows.get(0)[0]);
  }

  @Test
  void testIndexOutOfRangeIsMissingValueWhenIgnoringMissingPath() throws Exception {
    List<Object[]> rows = readRows(ITEMS_JSON, true, "$.matrix.index(5)");
    assertEquals(1, rows.size());
    assertNull(rows.get(0)[0]);
  }

  @Test
  void testFailingFunctionReportsCauseWhenNotIgnoringMissingPath() {
    JsonInputException e =
        assertThrows(JsonInputException.class, () -> readRows(ITEMS_JSON, false, "$.empty.sum()"));
    assertTrue(e.getMessage().contains("$.empty.sum()"), e.getMessage());
    assertTrue(e.getMessage().contains("empty array"), e.getMessage());
  }

  @Test
  void testFirstOnEmptyArrayFailsWhenNotIgnoringMissingPath() {
    assertThrows(JsonInputException.class, () -> readRows(ITEMS_JSON, false, "$.empty.first()"));
  }

  @Test
  void testMissingFunctionValueNextToMultiRowField() throws Exception {
    List<Object[]> rows = readRows(ITEMS_JSON, true, "$.items[*].name", "$.absent.length()");
    assertEquals(3, rows.size());
    assertEquals("abcd", rows.get(2)[0]);
    assertNull(rows.get(2)[1]);
  }

  @Test
  void testFunctionPathWithoutDefaultPathLeafToNull() throws Exception {
    JsonInputField field = new JsonInputField("value");
    field.setPath("$.items.length()");
    FastJsonReader reader =
        new FastJsonReader(new JsonInputField[] {field}, false, mock(ILogChannel.class));
    List<Object[]> rows =
        rows(
            reader.parseStringValue(
                new ByteArrayInputStream(ITEMS_JSON.getBytes(StandardCharsets.UTF_8))));
    assertEquals(1, rows.size());
    assertEquals(3, ((Number) rows.get(0)[0]).intValue());
  }
}

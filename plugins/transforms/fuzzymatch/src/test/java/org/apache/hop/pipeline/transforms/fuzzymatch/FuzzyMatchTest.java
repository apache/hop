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

package org.apache.hop.pipeline.transforms.fuzzymatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;

/** Unit test for {@link FuzzyMatch} */
class FuzzyMatchTest {
  @InjectMocks private FuzzyMatchHandler fuzzyMatch;
  private TransformMockHelper<FuzzyMatchMeta, FuzzyMatchData> mockHelper;

  private final Object[] row = new Object[] {"Catrine"};
  private final Object[] rowB = new Object[] {"Catrine".getBytes()};
  private final Object[] row2 = new Object[] {"John"};
  private final Object[] row2B = new Object[] {"John".getBytes()};
  private final Object[] row3 = new Object[] {"Catriny"};
  private final Object[] row3B = new Object[] {"Catriny".getBytes()};
  private final List<Object[]> rows = new ArrayList<>();
  private final List<Object[]> binaryRows = new ArrayList<>();
  private final List<Object[]> lookupRows = new ArrayList<>();
  private final List<Object[]> binaryLookupRows = new ArrayList<>();

  {
    rows.add(row);
    binaryRows.add(rowB);
    lookupRows.add(row2);
    lookupRows.add(row3);
    binaryLookupRows.add(row2B);
    binaryLookupRows.add(row3B);
  }

  private static class FuzzyMatchHandler extends FuzzyMatch {
    private Object[] resultRow = null;
    private final List<Object[]> resultRows = new ArrayList<>();
    private IRowSet rowset = null;

    public FuzzyMatchHandler(
        TransformMeta transformMeta,
        FuzzyMatchMeta meta,
        FuzzyMatchData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    @Override
    public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      resultRow = row;
      resultRows.add(row);
    }

    @Override
    public IRowSet findInputRowSet(String sourceTransformName) throws HopTransformException {
      return rowset;
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    mockHelper =
        new TransformMockHelper<>("Fuzzy Match", FuzzyMatchMeta.class, FuzzyMatchData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() throws Exception {
    mockHelper.cleanUp();
  }

  private FuzzyMatchHandler createHandler(FuzzyMatchMeta meta, FuzzyMatchData data) {
    return new FuzzyMatchHandler(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  private FuzzyMatchData prepareDistanceData(
      FuzzyMatchMeta meta, boolean addMeasure, Object[]... lookupValues) throws Exception {
    FuzzyMatchData data = new FuzzyMatchData();
    data.readLookupValues = false;
    data.indexOfMainField = 0;
    data.minimalDistance =
        Integer.parseInt(meta.getMinimalValue() == null ? "0" : meta.getMinimalValue());
    data.maximalDistance =
        Integer.parseInt(meta.getMaximalValue() == null ? "5" : meta.getMaximalValue());
    data.maxMatches =
        Integer.parseInt(
            meta.getMaxMatches() == null
                ? String.valueOf(FuzzyMatchMeta.DEFAULT_MAX_MATCHES)
                : meta.getMaxMatches());
    data.addValueFieldName = addMeasure;
    data.valueSeparator = meta.getSeparator() == null ? "," : meta.getSeparator();
    data.look = new HashSet<>();
    Collections.addAll(data.look, lookupValues);

    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("name"));
    fuzzyMatch = createHandler(meta, data);
    fuzzyMatch.setInputRowMeta(inputMeta);
    data.outputRowMeta = inputMeta.clone();
    meta.getFields(data.outputRowMeta, "FuzzyMatch", null, null, fuzzyMatch, null);
    fuzzyMatch.first = false;
    return data;
  }

  @Test
  void testProcessRow() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.SOUNDEX);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");

    FuzzyMatchData data = new FuzzyMatchData();
    data.readLookupValues = false;
    data.indexOfMainField = 0;
    data.addValueFieldName = true;
    data.look = new HashSet<>();
    data.look.add(new Object[] {"John"});
    data.look.add(new Object[] {"Catriny"});

    fuzzyMatch = createHandler(meta, data);
    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("name"));
    fuzzyMatch.setInputRowMeta(inputMeta);
    data.outputRowMeta = inputMeta.clone();
    meta.getFields(data.outputRowMeta, "FuzzyMatch", null, null, fuzzyMatch, null);
    fuzzyMatch.first = false;
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"Catrine"}));

    assertTrue(fuzzyMatch.processRow());
    assertEquals(1, fuzzyMatch.resultRows.size());
    // main stream value + phonetic match (Soundex of Catrine ~= Catriny)
    assertEquals("Catrine", fuzzyMatch.resultRows.getFirst()[0]);
    assertEquals("Catriny", fuzzyMatch.resultRows.getFirst()[1]);
    assertNotNull(fuzzyMatch.resultRows.getFirst()[2]); // soundex code
  }

  @Test
  void testClosestDistanceReturnsBestMatch() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMinimalValue("0");
    meta.setMaximalValue("5");

    prepareDistanceData(meta, true, new Object[] {"Catriny"}, new Object[] {"John"});
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"Catrine"}));

    assertTrue(fuzzyMatch.processRow());
    assertEquals(1, fuzzyMatch.resultRows.size());
    assertEquals("Catriny", fuzzyMatch.resultRows.getFirst()[1]);
    assertEquals(1L, fuzzyMatch.resultRows.getFirst()[2]);
  }

  @Test
  void testAllRowsTopKEmitsSeparateRowsWithDistance() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMinimalValue("0");
    meta.setMaximalValue("5");
    meta.setMaxMatches("2");
    meta.setCaseSensitive(false);

    prepareDistanceData(
        meta, true, new Object[] {"Catrine"}, new Object[] {"Catriny"}, new Object[] {"John"});
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"Catrine"}));

    assertTrue(fuzzyMatch.processRow());
    assertEquals(2, fuzzyMatch.resultRows.size());
    assertEquals("Catrine", fuzzyMatch.resultRows.get(0)[1]);
    assertEquals(0L, fuzzyMatch.resultRows.get(0)[2]);
    assertEquals("Catriny", fuzzyMatch.resultRows.get(1)[1]);
    assertEquals(1L, fuzzyMatch.resultRows.get(1)[2]);
  }

  @Test
  void testAllConcatReturnsJoinedMatchesAndMeasures() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_CONCAT);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMinimalValue("0");
    meta.setMaximalValue("5");
    meta.setMaxMatches("10");
    meta.setSeparator("|");

    prepareDistanceData(meta, true, new Object[] {"Catrine"}, new Object[] {"Catriny"});
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"Catrine"}));

    assertTrue(fuzzyMatch.processRow());
    assertEquals(1, fuzzyMatch.resultRows.size());
    assertEquals("Catrine|Catriny", fuzzyMatch.resultRows.getFirst()[1]);
    assertEquals("0|1", fuzzyMatch.resultRows.getFirst()[2]);
  }

  @Test
  void testAllRowsRespectsDistanceThreshold() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMinimalValue("0");
    meta.setMaximalValue("1");
    meta.setMaxMatches("10");

    prepareDistanceData(meta, true, new Object[] {"Catriny"}, new Object[] {"John"});
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"Catrine"}));

    assertTrue(fuzzyMatch.processRow());
    assertEquals(1, fuzzyMatch.resultRows.size());
    assertEquals("Catriny", fuzzyMatch.resultRows.getFirst()[1]);
  }

  @Test
  void testNoMatchStillEmitsRowWithNullMatch() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMinimalValue("0");
    meta.setMaximalValue("1");
    meta.setMaxMatches("5");

    prepareDistanceData(meta, true, new Object[] {"zzzz"});
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"Catrine"}));

    assertTrue(fuzzyMatch.processRow());
    assertEquals(1, fuzzyMatch.resultRows.size());
    assertEquals("Catrine", fuzzyMatch.resultRows.getFirst()[0]);
    assertNull(fuzzyMatch.resultRows.getFirst()[1]);
  }

  @Test
  void testCaseSensitiveDistance() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMinimalValue("0");
    meta.setMaximalValue("0");
    meta.setCaseSensitive(true);

    prepareDistanceData(meta, true, new Object[] {"catrine"});
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"Catrine"}));

    assertTrue(fuzzyMatch.processRow());
    assertNull(fuzzyMatch.resultRows.getFirst()[1]);
  }

  @Test
  void testJaroClosestReturnsSimilarityMeasure() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.JARO);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMinimalValue("0");
    meta.setMaximalValue("1");

    FuzzyMatchData data = new FuzzyMatchData();
    data.readLookupValues = false;
    data.indexOfMainField = 0;
    data.minimalSimilarity = 0;
    data.maximalSimilarity = 1;
    data.maxMatches = 1;
    data.addValueFieldName = true;
    data.look = new HashSet<>();
    data.look.add(new Object[] {"Martha"});
    data.look.add(new Object[] {"Marhta"});

    fuzzyMatch = createHandler(meta, data);
    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("name"));
    fuzzyMatch.setInputRowMeta(inputMeta);
    data.outputRowMeta = inputMeta.clone();
    meta.getFields(data.outputRowMeta, "FuzzyMatch", null, null, fuzzyMatch, null);
    fuzzyMatch.first = false;
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"Martha"}));

    assertTrue(fuzzyMatch.processRow());
    assertEquals(1, fuzzyMatch.resultRows.size());
    assertEquals("Martha", fuzzyMatch.resultRows.getFirst()[1]);
    assertEquals(1.0, ((Number) fuzzyMatch.resultRows.getFirst()[2]).doubleValue(), 1e-9);
  }

  @Test
  void testInitCapsMaxMatchesAndRequiresFields() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setMaxMatches("999");
    meta.setMinimalValue("0");
    meta.setMaximalValue("5");

    FuzzyMatchData data = new FuzzyMatchData();
    fuzzyMatch = createHandler(meta, data);
    assertTrue(fuzzyMatch.init());
    assertEquals(FuzzyMatchMeta.HARD_MAX_MATCHES, data.maxMatches);
    assertTrue(data.addValueFieldName);

    FuzzyMatchMeta missing = new FuzzyMatchMeta();
    missing.setOutputMatchField("match");
    FuzzyMatchData data2 = new FuzzyMatchData();
    FuzzyMatchHandler handler2 = createHandler(missing, data2);
    assertFalse(handler2.init());
  }

  @Test
  void testInitRejectsEmptyOutputMatchField() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("");
    FuzzyMatchHandler handler = createHandler(meta, new FuzzyMatchData());
    assertFalse(handler.init());
  }

  @Test
  void testProcessRowEndsWhenInputExhausted() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");

    FuzzyMatchData data = prepareDistanceData(meta, true, new Object[] {"Catrine"});
    // empty input
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet());

    assertFalse(fuzzyMatch.processRow());
    assertTrue(fuzzyMatch.resultRows.isEmpty());
    assertNotNull(data);
  }

  @Test
  void testPairSimilarityAllRows() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.PAIR_SIMILARITY);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMinimalValue("0.1");
    meta.setMaximalValue("1");
    meta.setMaxMatches("2");

    FuzzyMatchData data = new FuzzyMatchData();
    data.readLookupValues = false;
    data.indexOfMainField = 0;
    data.minimalSimilarity = 0.1;
    data.maximalSimilarity = 1;
    data.maxMatches = 2;
    data.addValueFieldName = true;
    data.look = new HashSet<>();
    data.look.add(new Object[] {"France"});
    data.look.add(new Object[] {"French"});
    data.look.add(new Object[] {"zzzz"});

    fuzzyMatch = createHandler(meta, data);
    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("name"));
    fuzzyMatch.setInputRowMeta(inputMeta);
    data.outputRowMeta = inputMeta.clone();
    meta.getFields(data.outputRowMeta, "FuzzyMatch", null, null, fuzzyMatch, null);
    fuzzyMatch.first = false;
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"France"}));

    assertTrue(fuzzyMatch.processRow());
    assertEquals(2, fuzzyMatch.resultRows.size());
    assertEquals("France", fuzzyMatch.resultRows.getFirst()[1]);
    assertTrue(((Number) fuzzyMatch.resultRows.getFirst()[2]).doubleValue() >= 0.1);
  }

  @Test
  void testAllRowsWithAdditionalLookupFields() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setMainStreamField("name");
    meta.setLookupField("name");
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMinimalValue("0");
    meta.setMaximalValue("5");
    meta.setMaxMatches("2");
    meta.setLookupValues(List.of(new FuzzyMatchMeta.FMLookupValue("id", "lookup_id")));

    FuzzyMatchData data = new FuzzyMatchData();
    data.readLookupValues = false;
    data.indexOfMainField = 0;
    data.minimalDistance = 0;
    data.maximalDistance = 5;
    data.maxMatches = 2;
    data.addValueFieldName = true;
    data.addAdditionalFields = true;
    data.look = new HashSet<>();
    data.look.add(new Object[] {"Catrine", "1"});
    data.look.add(new Object[] {"Catriny", "2"});

    fuzzyMatch = createHandler(meta, data);
    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("name"));
    fuzzyMatch.setInputRowMeta(inputMeta);

    IRowMeta infoMeta = new RowMeta();
    infoMeta.addValueMeta(new ValueMetaString("id"));
    data.outputRowMeta = inputMeta.clone();
    meta.getFields(
        data.outputRowMeta, "FuzzyMatch", new IRowMeta[] {infoMeta}, null, fuzzyMatch, null);
    fuzzyMatch.first = false;
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(new Object[] {"Catrine"}));

    assertTrue(fuzzyMatch.processRow());
    assertEquals(2, fuzzyMatch.resultRows.size());
    assertEquals("Catrine", fuzzyMatch.resultRows.getFirst()[1]);
    assertEquals(0L, fuzzyMatch.resultRows.get(0)[2]);
    assertEquals("1", fuzzyMatch.resultRows.get(0)[3]);
    assertEquals("Catriny", fuzzyMatch.resultRows.get(1)[1]);
    assertEquals("2", fuzzyMatch.resultRows.get(1)[3]);
  }

  @Test
  void testReadLookupValues() throws Exception {
    FuzzyMatchData data = spy(new FuzzyMatchData());
    data.indexOfCachedFields = new int[2];
    data.minimalDistance = 0;
    data.maximalDistance = 5;
    FuzzyMatchMeta meta = spy(new FuzzyMatchMeta());
    meta.setOutputMatchField("I don't want NPE here!");
    data.readLookupValues = true;
    fuzzyMatch =
        new FuzzyMatchHandler(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    fuzzyMatch.init();
    IRowSet lookupRowSet = mockHelper.getMockInputRowSet(binaryLookupRows);
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(binaryRows));
    fuzzyMatch.addRowSetToInputRowSets(lookupRowSet);
    fuzzyMatch.rowset = lookupRowSet;

    IRowMeta iRowMeta = new RowMeta();
    IValueMeta valueMeta = new ValueMetaString("field1");
    valueMeta.setStorageMetadata(new ValueMetaString("field1"));
    valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    iRowMeta.addValueMeta(valueMeta);
    when(lookupRowSet.getRowMeta()).thenReturn(iRowMeta);
    when(meta.getLookupField()).thenReturn("field1");
    when(meta.getMainStreamField()).thenReturn("field1");
    fuzzyMatch.setInputRowMeta(iRowMeta.clone());

    when(meta.getAlgorithm()).thenReturn(FuzzyMatchMeta.Algorithm.DAMERAU_LEVENSHTEIN);
    ITransformIOMeta transformIOMetaInterface = mock(ITransformIOMeta.class);
    when(meta.getTransformIOMeta()).thenReturn(transformIOMetaInterface);
    IStream streamInterface = mock(IStream.class);
    List<IStream> streamInterfaceList = new ArrayList<>();
    streamInterfaceList.add(streamInterface);
    when(streamInterface.getTransformMeta()).thenReturn(mockHelper.transformMeta);

    when(transformIOMetaInterface.getInfoStreams()).thenReturn(streamInterfaceList);

    fuzzyMatch.processRow();
    assertEquals(
        iRowMeta.getString(row3B, 0), data.outputRowMeta.getString(fuzzyMatch.resultRow, 1));
  }

  @Test
  void testLookupValuesWhenMainFieldIsNull() throws Exception {
    FuzzyMatchData data = spy(new FuzzyMatchData());
    FuzzyMatchMeta meta = spy(new FuzzyMatchMeta());
    data.readLookupValues = false;
    fuzzyMatch =
        new FuzzyMatchHandler(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    fuzzyMatch.init();
    fuzzyMatch.first = false;
    data.indexOfMainField = 1;
    Object[] inputRow = {"test input", null};
    IRowSet lookupRowSet = mockHelper.getMockInputRowSet(new Object[] {"test lookup"});
    fuzzyMatch.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));
    fuzzyMatch.addRowSetToInputRowSets(lookupRowSet);
    fuzzyMatch.rowset = lookupRowSet;

    IRowMeta iRowMeta = new RowMeta();
    IValueMeta valueMeta = new ValueMetaString("field1");
    valueMeta.setStorageMetadata(new ValueMetaString("field1"));
    valueMeta.setStorageType(IValueMeta.TYPE_STRING);
    iRowMeta.addValueMeta(valueMeta);
    when(lookupRowSet.getRowMeta()).thenReturn(iRowMeta);
    fuzzyMatch.setInputRowMeta(iRowMeta.clone());
    data.outputRowMeta = iRowMeta.clone();

    fuzzyMatch.processRow();
    assertEquals(inputRow[0], fuzzyMatch.resultRow[0]);
    assertNull(fuzzyMatch.resultRow[1]);
    assertTrue(
        Arrays.stream(fuzzyMatch.resultRow, 3, fuzzyMatch.resultRow.length)
            .allMatch(Objects::isNull));
  }
}

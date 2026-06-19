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

package org.apache.hop.pipeline.transforms.tablecompare;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Unit tests for {@link TableCompare} merge-compare logic and validation paths. Uses mocked {@link
 * Database} streams to avoid JDBC and to assert left/right/inner error counts.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class TableCompareTests {

  private TransformMockHelper<TableCompareMeta, TableCompareData> helper;
  private TableCompareMeta meta;
  private TableCompareData data;

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("TableCompare", TableCompareMeta.class, TableCompareData.class);
    when(helper.pipeline.isRunning()).thenReturn(true);
    when(helper.pipeline.isStopped()).thenReturn(false);
    doReturn(helper.iLogChannel)
        .when(helper.logChannelFactory)
        .create(any(), any(ILoggingObject.class));

    DatabaseMeta dbMeta = mock(DatabaseMeta.class);
    when(dbMeta.quoteField(anyString())).thenAnswer(inv -> "\"" + inv.getArgument(0) + "\"");
    when(dbMeta.getQuotedSchemaTableCombination(any(IVariables.class), anyString(), anyString()))
        .thenAnswer(
            inv ->
                "\""
                    + inv.getArgument(1, String.class)
                    + "\".\""
                    + inv.getArgument(2, String.class)
                    + "\"");

    when(helper.pipelineMeta.findDatabase(anyString(), any(IVariables.class))).thenReturn(dbMeta);

    meta = new TableCompareMeta();
    meta.setDefault();
    meta.setReferenceConnection("unit_ref");
    meta.setCompareConnection("unit_cmp");

    data = new TableCompareData();
    data.refSchemaIndex = 0;
    data.refTableIndex = 1;
    data.cmpSchemaIndex = 2;
    data.cmpTableIndex = 3;
    data.keyFieldsIndex = 4;
    data.excludeFieldsIndex = 5;
    data.refCteIndex = -1;
    data.cmpCteIndex = -1;
    data.keyDescIndex = 6;
    data.valueReferenceIndex = 7;
    data.valueCompareIndex = 8;
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  private TransformMeta transformMeta() {
    TransformMeta tm = helper.transformMeta;
    when(tm.isDoingErrorHandling()).thenReturn(false);
    when(tm.getName()).thenReturn("TableCompare");
    return tm;
  }

  /** Issue #6984 scenario: left-only, right-only, and inner value mismatch. */
  @Test
  void compare_merge_counts_match_merge_rows_semantics_issue6984() throws Exception {
    // Reference: id 1,2,3 — Compare: 2,3,4,5 — id 3 text differs
    List<Object[]> refRows =
        List.of(
            new Object[] {1L, "First"}, new Object[] {2L, "Second"}, new Object[] {3L, "Third"});
    List<Object[]> cmpRows =
        List.of(
            new Object[] {2L, "Second"},
            new Object[] {3L, "3."},
            new Object[] {4L, "Fourth"},
            new Object[] {5L, "Fith"});

    Object[] result = runCompare(refRows, cmpRows, twoColTableMeta(), "id", "name");

    assertEquals(4L, n(result[0]), "nrErrors");
    assertEquals(3L, n(result[1]), "nrRecordsReference");
    assertEquals(4L, n(result[2]), "nrRecordsCompare");
    assertEquals(1L, n(result[3]), "nrErrorsLeftJoin (id 1 only in reference)");
    assertEquals(1L, n(result[4]), "nrErrorsInnerJoin (id 3 value diff)");
    assertEquals(2L, n(result[5]), "nrErrorsRightJoin (id 4,5 only in compare)");
  }

  @Test
  void compare_identical_tables_no_errors() throws Exception {
    List<Object[]> rows = List.of(new Object[] {1L, "a"}, new Object[] {2L, "b"});
    Object[] result = runCompare(rows, rows, twoColTableMeta(), "id", "");
    assertEquals(0L, n(result[0]));
    assertEquals(2L, n(result[1]));
    assertEquals(2L, n(result[2]));
    assertEquals(0L, n(result[3]));
    assertEquals(0L, n(result[4]));
    assertEquals(0L, n(result[5]));
  }

  @Test
  void compare_only_in_reference_until_end() throws Exception {
    List<Object[]> refRows = List.<Object[]>of(new Object[] {1L, "a"});
    List<Object[]> cmpRows = List.of();
    Object[] result = runCompare(refRows, cmpRows, twoColTableMeta(), "id", "");
    assertEquals(1L, n(result[0]));
    assertEquals(1L, n(result[3]), "left");
    assertEquals(0L, n(result[5]), "right");
  }

  @Test
  void compare_only_in_compare_until_end() throws Exception {
    List<Object[]> refRows = List.of();
    List<Object[]> cmpRows = List.<Object[]>of(new Object[] {9L, "z"});
    Object[] result = runCompare(refRows, cmpRows, twoColTableMeta(), "id", "");
    assertEquals(1L, n(result[0]));
    assertEquals(0L, n(result[3]), "left");
    assertEquals(1L, n(result[5]), "right");
  }

  @Test
  void compare_key_only_tables_no_value_columns() throws Exception {
    IRowMeta keyOnly = new RowMeta();
    keyOnly.addValueMeta(new ValueMetaInteger("id"));

    List<Object[]> refRows = List.of(new Object[] {1L}, new Object[] {2L});
    List<Object[]> cmpRows = List.of(new Object[] {2L}, new Object[] {3L});
    Object[] result = runCompare(refRows, cmpRows, keyOnly, "id", "");
    assertEquals(2L, n(result[0]));
    assertEquals(1L, n(result[3]));
    assertEquals(1L, n(result[5]));
    assertEquals(0L, n(result[4]));
  }

  @Test
  void compare_field_count_mismatch_increments_error() throws Exception {
    IRowMeta refMeta = twoColTableMeta();
    IRowMeta cmpMeta = new RowMeta();
    cmpMeta.addValueMeta(new ValueMetaInteger("id"));
    cmpMeta.addValueMeta(new ValueMetaString("text"));
    cmpMeta.addValueMeta(new ValueMetaString("extra"));

    List<Object[]> refRows = List.<Object[]>of(new Object[] {1L, "a"});
    List<Object[]> cmpRows = List.<Object[]>of(new Object[] {1L, "a", "x"});
    Object[] result = runCompare(refRows, cmpRows, refMeta, cmpMeta, "id", "");
    assertEquals(1L, n(result[0]));
  }

  @Test
  void compare_empty_reference_table_early_exit() throws Exception {
    Object[] r = inputRow("public", "", "public", "t", "id", "");
    TableCompare tc = newTableCompare();
    doNothing().when(tc).putError(any(), any(), anyLong(), any(), any(), any());
    Object[] result =
        invokeCompareTables(tc, rowMetaForInput(), r, "public", "", "public", "t", "id", "");
    assertEquals(1L, n(result[0]));
    verify(tc).putError(any(), any(), eq(1L), anyString(), any(), eq("TAC008"));
  }

  @Test
  void compare_empty_compare_table_early_exit() throws Exception {
    Object[] r = inputRow("", "t", "", "", "id", "");
    TableCompare tc = newTableCompare();
    doNothing().when(tc).putError(any(), any(), anyLong(), any(), any(), any());
    Object[] result = invokeCompareTables(tc, rowMetaForInput(), r, "", "t", "", "", "id", "");
    assertEquals(1L, n(result[0]));
  }

  @Test
  void compare_empty_key_fields_early_exit() throws Exception {
    Object[] r = inputRow("", "t", "", "t", "", "");
    TableCompare tc = newTableCompare();
    doNothing().when(tc).putError(any(), any(), anyLong(), any(), any(), any());
    Object[] result = invokeCompareTables(tc, rowMetaForInput(), r, "", "t", "", "t", "", "");
    assertEquals(1L, n(result[0]));
  }

  /** Composite key: one row only on reference, one only on compare (sorted merge). */
  @Test
  void compare_composite_key_left_and_right() throws Exception {
    IRowMeta m = new RowMeta();
    m.addValueMeta(new ValueMetaInteger("region"));
    m.addValueMeta(new ValueMetaInteger("id"));
    m.addValueMeta(new ValueMetaString("text"));

    List<Object[]> refRows = List.<Object[]>of(new Object[] {1L, 1L, "a"});
    List<Object[]> cmpRows = List.<Object[]>of(new Object[] {1L, 2L, "b"});
    Object[] result = runCompare(refRows, cmpRows, m, "region, id", "");
    assertEquals(2L, n(result[0]));
    assertEquals(1L, n(result[3]));
    assertEquals(1L, n(result[5]));
  }

  @Test
  void compare_value_mismatch_emits_tac006_when_error_handling() throws Exception {
    List<Object[]> refRows = List.<Object[]>of(new Object[] {1L, "old"});
    List<Object[]> cmpRows = List.<Object[]>of(new Object[] {1L, "new"});
    ResultSet refRs = mock(ResultSet.class);
    ResultSet cmpRs = mock(ResultSet.class);
    Database refDb = mock(Database.class);
    Database cmpDb = mock(Database.class);
    when(refDb.openQuery(anyString())).thenReturn(refRs);
    when(cmpDb.openQuery(anyString())).thenReturn(cmpRs);
    when(refDb.getTableFieldsMeta(anyString(), anyString())).thenReturn(twoColTableMeta());
    when(cmpDb.getTableFieldsMeta(anyString(), anyString())).thenReturn(twoColTableMeta());

    Iterator<Object[]> refIt = new ArrayList<>(refRows).iterator();
    Iterator<Object[]> cmpIt = new ArrayList<>(cmpRows).iterator();
    when(refDb.getRow(same(refRs))).thenAnswer(inv -> refIt.hasNext() ? refIt.next() : null);
    when(cmpDb.getRow(same(cmpRs))).thenAnswer(inv -> cmpIt.hasNext() ? cmpIt.next() : null);
    when(refDb.getReturnRowMeta()).thenReturn(twoColTableMeta().clone());
    when(cmpDb.getReturnRowMeta()).thenReturn(twoColTableMeta().clone());

    Object[] r = inputRow("", "T_OLD", "", "T_NEW", "id", "");
    TransformMeta tm = transformMeta();
    when(tm.isDoingErrorHandling()).thenReturn(true);
    TableCompare tc =
        spy(new TableCompare(tm, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    doNothing().when(tc).putError(any(), any(), anyLong(), any(), any(), any());
    data.referenceDb = refDb;
    data.compareDb = cmpDb;

    Object[] result =
        invokeCompareTables(tc, rowMetaForInput(), r, "", "T_OLD", "", "T_NEW", "id", "");
    assertEquals(1L, n(result[0]));
    assertEquals(1L, n(result[4]), "inner");
    verify(tc, times(1)).putError(any(), any(), eq(1L), anyString(), eq("text"), eq("TAC006"));
  }

  @Test
  void compare_key_field_missing_emits_tac002_and_tac003_when_error_handling() throws Exception {
    IRowMeta refMeta = new RowMeta();
    refMeta.addValueMeta(new ValueMetaInteger("id"));
    refMeta.addValueMeta(new ValueMetaString("text"));
    IRowMeta cmpMeta = twoColTableMeta();

    ResultSet refRs = mock(ResultSet.class);
    ResultSet cmpRs = mock(ResultSet.class);
    Database refDb = mock(Database.class);
    Database cmpDb = mock(Database.class);
    when(refDb.openQuery(anyString())).thenReturn(refRs);
    when(cmpDb.openQuery(anyString())).thenReturn(cmpRs);
    when(refDb.getTableFieldsMeta(anyString(), anyString())).thenReturn(refMeta);
    when(cmpDb.getTableFieldsMeta(anyString(), anyString())).thenReturn(cmpMeta);

    Object[] r = inputRow("", "T_OLD", "", "T_NEW", "unknown_key", "");
    TransformMeta tm = transformMeta();
    when(tm.isDoingErrorHandling()).thenReturn(true);
    TableCompare tc =
        spy(new TableCompare(tm, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    doNothing().when(tc).putError(any(), any(), anyLong(), any(), any(), any());
    data.referenceDb = refDb;
    data.compareDb = cmpDb;

    Object[] result =
        invokeCompareTables(tc, rowMetaForInput(), r, "", "T_OLD", "", "T_NEW", "unknown_key", "");
    assertEquals(2L, n(result[0]));
    verify(tc, times(1)).putError(any(), any(), eq(1L), anyString(), any(), eq("TAC002"));
    verify(tc, times(1)).putError(any(), any(), eq(1L), anyString(), any(), eq("TAC003"));
  }

  private static long n(Object o) {
    return ((Number) o).longValue();
  }

  private Object[] runCompare(
      List<Object[]> refRows,
      List<Object[]> cmpRows,
      IRowMeta tableFieldsMeta,
      String keys,
      String exclude)
      throws Exception {
    return runCompare(refRows, cmpRows, tableFieldsMeta, tableFieldsMeta, keys, exclude);
  }

  private Object[] runCompare(
      List<Object[]> refRows,
      List<Object[]> cmpRows,
      IRowMeta refTableMeta,
      IRowMeta cmpTableMeta,
      String keys,
      String exclude)
      throws Exception {
    ResultSet refRs = mock(ResultSet.class);
    ResultSet cmpRs = mock(ResultSet.class);
    Database refDb = mock(Database.class);
    Database cmpDb = mock(Database.class);

    when(refDb.openQuery(anyString())).thenReturn(refRs);
    when(cmpDb.openQuery(anyString())).thenReturn(cmpRs);
    when(refDb.getTableFieldsMeta(anyString(), anyString())).thenReturn(refTableMeta);
    when(cmpDb.getTableFieldsMeta(anyString(), anyString())).thenReturn(cmpTableMeta);

    Iterator<Object[]> refIt = new ArrayList<>(refRows).iterator();
    Iterator<Object[]> cmpIt = new ArrayList<>(cmpRows).iterator();
    when(refDb.getRow(same(refRs))).thenAnswer(inv -> refIt.hasNext() ? refIt.next() : null);
    when(cmpDb.getRow(same(cmpRs))).thenAnswer(inv -> cmpIt.hasNext() ? cmpIt.next() : null);

    IRowMeta returnMeta = refTableMeta.clone();
    when(refDb.getReturnRowMeta()).thenReturn(returnMeta);
    when(cmpDb.getReturnRowMeta()).thenReturn(cmpTableMeta.clone());

    Object[] r = inputRow("", "T_OLD", "", "T_NEW", keys, exclude);
    TableCompare tc = newTableCompare();
    doNothing().when(tc).putError(any(), any(), anyLong(), any(), any(), any());
    data.referenceDb = refDb;
    data.compareDb = cmpDb;

    return invokeCompareTables(tc, rowMetaForInput(), r, "", "T_OLD", "", "T_NEW", keys, exclude);
  }

  private TableCompare newTableCompare() {
    return spy(
        new TableCompare(transformMeta(), meta, data, 0, helper.pipelineMeta, helper.pipeline));
  }

  private static IRowMeta twoColTableMeta() {
    IRowMeta m = new RowMeta();
    m.addValueMeta(new ValueMetaInteger("id"));
    m.addValueMeta(new ValueMetaString("text"));
    return m;
  }

  private static IRowMeta rowMetaForInput() {
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("ref_schema"));
    rm.addValueMeta(new ValueMetaString("ref_table"));
    rm.addValueMeta(new ValueMetaString("cmp_schema"));
    rm.addValueMeta(new ValueMetaString("cmp_table"));
    rm.addValueMeta(new ValueMetaString("keys"));
    rm.addValueMeta(new ValueMetaString("exclude"));
    rm.addValueMeta(new ValueMetaString("key_desc"));
    rm.addValueMeta(new ValueMetaString("ref_val"));
    rm.addValueMeta(new ValueMetaString("cmp_val"));
    return rm;
  }

  private static Object[] inputRow(
      String refSchema,
      String refTable,
      String cmpSchema,
      String cmpTable,
      String keys,
      String exclude) {
    return new Object[] {refSchema, refTable, cmpSchema, cmpTable, keys, exclude, null, null, null};
  }

  private static Object[] invokeCompareTables(
      TableCompare transform,
      IRowMeta rowMeta,
      Object[] r,
      String referenceSchema,
      String referenceTable,
      String compareSchema,
      String compareTable,
      String keyFields,
      String excludeFields)
      throws Exception {
    Method m =
        TableCompare.class.getDeclaredMethod(
            "compareTables",
            IRowMeta.class,
            Object[].class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class);
    m.setAccessible(true);
    return (Object[])
        m.invoke(
            transform,
            rowMeta,
            r,
            referenceSchema,
            referenceTable,
            "",
            compareSchema,
            compareTable,
            "",
            keyFields,
            excludeFields);
  }
}

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

package org.apache.hop.pipeline.transforms.tableoutput;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.Map;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableOutputTest {
  private DatabaseMeta databaseMeta;

  private TransformMeta transformMeta;

  private TableOutput tableOutput, tableOutputSpy;
  private TableOutputMeta tableOutputMeta;
  private TableOutputData tableOutputData;
  private Database db;
  private PipelineMeta pipelineMeta;

  @BeforeEach
  void setUp() {

    databaseMeta = mock(DatabaseMeta.class);
    doReturn("").when(databaseMeta).quoteField(anyString());

    tableOutputMeta = mock(TableOutputMeta.class);
    doReturn("TestConnection1").when(tableOutputMeta).getConnection();

    transformMeta = mock(TransformMeta.class);
    doReturn("transform").when(transformMeta).getName();
    doReturn(mock(TransformPartitioningMeta.class))
        .when(transformMeta)
        .getTargetTransformPartitioningMeta();
    doReturn(tableOutputMeta).when(transformMeta).getTransform();

    db = mock(Database.class);
    doReturn(mock(Connection.class)).when(db).getConnection();

    tableOutputData = mock(TableOutputData.class);
    tableOutputData.db = db;
    tableOutputData.tableName = "sas";
    tableOutputData.preparedStatements = mock(Map.class);
    tableOutputData.commitCounterMap = mock(Map.class);

    pipelineMeta = mock(PipelineMeta.class);
    doReturn(transformMeta).when(pipelineMeta).findTransform(anyString());

    setupTableOutputSpy();
  }

  private void setupTableOutputSpy() {

    tableOutput =
        new TableOutput(
            transformMeta,
            tableOutputMeta,
            tableOutputData,
            1,
            pipelineMeta,
            spy(new LocalPipelineEngine()));
    tableOutputSpy = spy(tableOutput);
    doReturn(transformMeta).when(tableOutputSpy).getTransformMeta();
    doReturn(false).when(tableOutputSpy).isRowLevel();
    doReturn(false).when(tableOutputSpy).isDebug();
    doNothing().when(tableOutputSpy).logDetailed(anyString());
  }

  @Test
  void testWriteToTable() throws Exception {
    tableOutputSpy.writeToTable(mock(IRowMeta.class), new Object[] {});
    verify(tableOutputSpy, times(1)).writeToTable(any(), any());
  }

  @Test
  void testTruncateTableOff() throws Exception {
    tableOutputSpy.truncateTable();
    verify(db, never()).truncateTable(anyString(), anyString());
  }

  @Test
  void testTruncateTable_on() throws Exception {
    when(tableOutputMeta.isTruncateTable()).thenReturn(true);
    when(tableOutputSpy.getCopy()).thenReturn(0);

    tableOutputSpy.truncateTable();
    verify(db).truncateTable(nullable(String.class), nullable(String.class));
  }

  @Test
  void testTruncateTable_on_PartitionId() throws Exception {
    when(tableOutputMeta.isTruncateTable()).thenReturn(true);
    when(tableOutputSpy.getCopy()).thenReturn(1);
    when(tableOutputSpy.getPartitionId()).thenReturn("partition id");

    tableOutputSpy.truncateTable();
    verify(db).truncateTable(nullable(String.class), nullable(String.class));
  }

  @Test
  void testProcessRow_truncatesIfNoRowsAvailable() throws Exception {
    when(tableOutputMeta.isTruncateTable()).thenReturn(true);

    doReturn(null).when(tableOutputSpy).getRow();

    boolean result = tableOutputSpy.processRow();

    assertFalse(result);
    verify(tableOutputSpy).truncateTable();
  }

  @Test
  void testProcessRow_doesNotTruncateIfNoRowsAvailableAndTruncateIsOff() throws Exception {
    when(tableOutputMeta.isTruncateTable()).thenReturn(false);

    doReturn(null).when(tableOutputSpy).getRow();

    boolean result = tableOutputSpy.processRow();

    assertFalse(result);
    verify(tableOutputSpy, never()).truncateTable();
  }

  @Test
  void testProcessRow_truncatesOnFirstRow() throws Exception {
    when(tableOutputMeta.isTruncateTable()).thenReturn(true);
    Object[] row = new Object[] {};
    doReturn(row).when(tableOutputSpy).getRow();

    try {
      tableOutputSpy.processRow();
    } catch (NullPointerException npe) {
      // not everything is set up to process an entire row, but we don't need that for this test
    }
    verify(tableOutputSpy, times(1)).truncateTable();
  }

  @Test
  void testProcessRow_doesNotTruncateOnOtherRows() throws Exception {
    when(tableOutputMeta.isTruncateTable()).thenReturn(true);
    Object[] row = new Object[] {};
    doReturn(row).when(tableOutputSpy).getRow();
    tableOutputSpy.first = false;
    doReturn(null).when(tableOutputSpy).writeToTable(any(IRowMeta.class), any(row.getClass()));

    boolean result = tableOutputSpy.processRow();

    assertTrue(result);
    verify(tableOutputSpy, never()).truncateTable();
  }

  @Test
  void testInit_unsupportedConnection() {

    IDatabase dbInterface = mock(IDatabase.class);

    doNothing().when(tableOutputSpy).logError(anyString());

    when(tableOutputMeta.getCommitSize()).thenReturn("1");
    when(tableOutputSpy.getPipelineMeta().findDatabase(any(String.class), any(IVariables.class)))
        .thenReturn(databaseMeta);
    when(databaseMeta.getIDatabase()).thenReturn(dbInterface);

    String unsupportedTableOutputMessage = "unsupported exception";
    when(dbInterface.getUnsupportedTableOutputMessage()).thenReturn(unsupportedTableOutputMessage);

    // Will cause the Hop Exception
    when(dbInterface.supportsStandardTableOutput()).thenReturn(false);

    tableOutputSpy.init();

    HopException ke = new HopException(unsupportedTableOutputMessage);
    verify(tableOutputSpy, times(1))
        .logError("An error occurred initializing this transform: " + ke.getMessage());
  }

  // ==================== DDL Feature Tests ====================

  @Test
  void testUpdateTableStructure_disabled() throws Exception {
    when(tableOutputMeta.isAutoUpdateTableStructure()).thenReturn(false);

    tableOutputSpy.updateTableStructure();

    // updateTableStructure should return early when disabled
    // No need to verify anything - just checking it doesn't throw exception
  }

  @Test
  void testUpdateTableStructure_partitioningEnabled() throws Exception {
    when(tableOutputMeta.isAutoUpdateTableStructure()).thenReturn(true);
    when(tableOutputMeta.isPartitioningEnabled()).thenReturn(true);

    tableOutputSpy.updateTableStructure();

    // updateTableStructure should return early when partitioning is enabled
  }

  @Test
  void testUpdateTableStructure_tableNameInField() throws Exception {
    when(tableOutputMeta.isAutoUpdateTableStructure()).thenReturn(true);
    when(tableOutputMeta.isPartitioningEnabled()).thenReturn(false);
    when(tableOutputMeta.isTableNameInField()).thenReturn(true);

    tableOutputSpy.updateTableStructure();

    // updateTableStructure should return early when table name is in field
  }

  @Test
  void testUpdateTableStructure_notFirstCopy() throws Exception {
    when(tableOutputMeta.isAutoUpdateTableStructure()).thenReturn(true);
    when(tableOutputMeta.isPartitioningEnabled()).thenReturn(false);
    when(tableOutputMeta.isTableNameInField()).thenReturn(false);
    when(tableOutputSpy.getCopy()).thenReturn(1);
    when(tableOutputSpy.getPartitionId()).thenReturn("");

    tableOutputSpy.updateTableStructure();

    // updateTableStructure should return early when not first copy and no partition ID
  }

  @Test
  void testTypesAreCompatible_sameTypeInteger() {
    IValueMeta tableField = new ValueMetaInteger("age");
    IValueMeta streamField = new ValueMetaInteger("age");

    assertTrue(tableOutputSpy.typesAreCompatible(tableField, streamField));
  }

  @Test
  void testTypesAreCompatible_differentTypes() {
    IValueMeta tableField = new ValueMetaString("age");
    IValueMeta streamField = new ValueMetaInteger("age");

    assertFalse(tableOutputSpy.typesAreCompatible(tableField, streamField));
  }

  @Test
  void testTypesAreCompatible_stringLengthCompatible() {
    IValueMeta tableField = new ValueMetaString("name");
    tableField.setLength(100);
    IValueMeta streamField = new ValueMetaString("name");
    streamField.setLength(50);

    assertTrue(tableOutputSpy.typesAreCompatible(tableField, streamField));
  }

  @Test
  void testTypesAreCompatible_stringLengthIncompatible() {
    IValueMeta tableField = new ValueMetaString("name");
    tableField.setLength(50);
    IValueMeta streamField = new ValueMetaString("name");
    streamField.setLength(100);

    assertFalse(tableOutputSpy.typesAreCompatible(tableField, streamField));
  }

  @Test
  void testTypesAreCompatible_stringUndefinedLengthIncompatible() {
    IValueMeta tableField = new ValueMetaString("name");
    tableField.setLength(50);
    IValueMeta streamField = new ValueMetaString("name");
    streamField.setLength(-1); // Undefined length

    assertFalse(tableOutputSpy.typesAreCompatible(tableField, streamField));
  }

  @Test
  void testTypesAreCompatible_numberPrecisionIncompatible() {
    IValueMeta tableField = new ValueMetaNumber("amount");
    tableField.setLength(10);
    tableField.setPrecision(2);
    IValueMeta streamField = new ValueMetaNumber("amount");
    streamField.setLength(15);
    streamField.setPrecision(5);

    assertFalse(tableOutputSpy.typesAreCompatible(tableField, streamField));
  }

  @Test
  void testTypesAreCompatible_numberPrecisionCompatible() {
    IValueMeta tableField = new ValueMetaNumber("amount");
    tableField.setLength(10);
    tableField.setPrecision(2);
    IValueMeta streamField = new ValueMetaNumber("amount");
    streamField.setLength(10);
    streamField.setPrecision(2);

    assertTrue(tableOutputSpy.typesAreCompatible(tableField, streamField));
  }
}

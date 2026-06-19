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

package org.apache.hop.pipeline.transforms.dynamicsqlrow;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link DynamicSqlRow}. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class DynamicSqlRowTest {

  private TransformMockHelper<DynamicSqlRowMeta, DynamicSqlRowData> transformMockHelper;
  private DynamicSqlRowMeta meta;
  private DynamicSqlRowData data;
  private DynamicSqlRow transform;

  @BeforeEach
  void setUp() {
    transformMockHelper =
        new TransformMockHelper<>(
            "DYNAMIC_SQL_ROW", DynamicSqlRowMeta.class, DynamicSqlRowData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);

    meta = new DynamicSqlRowMeta();
    data = new DynamicSqlRowData();
    transform =
        new DynamicSqlRow(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  private static IRowSet inputRowSet(Object[] row, IRowMeta rowMeta) {
    QueueRowSet rowSet = new QueueRowSet();
    rowSet.putRow(rowMeta, row);
    rowSet.setDone();
    return rowSet;
  }

  private static RowMeta rowMetaWithField(String fieldName) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString(fieldName));
    return rowMeta;
  }

  @Test
  void initReturnsFalseWhenDatabaseMetaIsMissing() {
    meta.setConnection("missing-connection");
    when(transformMockHelper.pipelineMeta.findDatabase(anyString(), any())).thenReturn(null);

    assertFalse(transform.init());
  }

  @Test
  void initReturnsFalseWhenConnectionIsEmpty() {
    meta.setConnection(null);

    assertFalse(transform.init());
  }

  @Test
  void disposeDisconnectsDatabase() {
    data.db = mock(Database.class);
    transform.dispose();

    verify(data.db).disconnect();
  }

  @Test
  void stopRunningDoesNothingWhenAlreadyCanceled() throws HopException {
    data.db = mock(Database.class);
    data.isCanceled = true;

    transform.stopRunning();

    verify(data.db, never()).cancelQuery();
  }

  @Test
  void stopRunningCancelsQueryWhenDatabaseIsActive() throws HopException {
    data.db = mock(Database.class);
    data.isCanceled = false;

    transform.stopRunning();

    verify(data.db).cancelQuery();
    assertTrue(data.isCanceled);
    assertTrue(transform.isStopped());
  }

  @Test
  void stopRunningDoesNothingWhenDatabaseIsNull() throws HopException {
    data.db = null;
    transform.stopRunning();

    assertFalse(data.isCanceled);
  }

  @Test
  void processRowReturnsFalseWhenNoMoreInput() throws HopException {
    QueueRowSet rowSet = new QueueRowSet();
    rowSet.setDone();
    transform.addRowSetToInputRowSets(rowSet);

    assertFalse(transform.processRow());
  }

  @Test
  void processRowFailsWhenSqlFieldNameIsEmpty() {
    meta.setSql("SELECT 1");
    meta.setSqlFieldName(null);
    transform.addRowSetToInputRowSets(
        inputRowSet(new Object[] {"SELECT 1"}, rowMetaWithField("sql_field")));

    assertThrows(HopException.class, () -> transform.processRow());
  }

  @Test
  void processRowFailsWhenTemplateSqlIsEmpty() {
    meta.setSql("");
    meta.setSqlFieldName("sql_field");
    transform.addRowSetToInputRowSets(
        inputRowSet(new Object[] {"SELECT 1"}, rowMetaWithField("sql_field")));

    assertThrows(HopException.class, () -> transform.processRow());
  }

  @Test
  void processRowFailsWhenSqlFieldIsNotFound() {
    meta.setSql("SELECT 1");
    meta.setSqlFieldName("sql_field");
    // Row meta does not contain "sql_field", so indexOfValue returns -1.
    transform.addRowSetToInputRowSets(
        inputRowSet(new Object[] {"SELECT 1"}, rowMetaWithField("other_field")));

    HopException exception = assertThrows(HopException.class, () -> transform.processRow());
    assertTrue(exception.getMessage().contains("sql_field"));
  }

  @Test
  void processRowUsesErrorHandlingWhenConfigured() throws HopException {
    meta.setSql("SELECT 1");
    meta.setSqlFieldName("sql_field");
    when(transformMockHelper.transformMeta.isDoingErrorHandling()).thenReturn(true);

    data.db = mock(Database.class);
    when(data.db.openQuery(anyString())).thenThrow(new HopDatabaseException("Query failed"));

    DynamicSqlRow spyTransform = spy(transform);
    doNothing()
        .when(spyTransform)
        .putError(any(), any(), anyLong(), anyString(), any(), anyString());

    spyTransform.addRowSetToInputRowSets(
        inputRowSet(new Object[] {"SELECT 1"}, rowMetaWithField("sql_field")));

    assertTrue(spyTransform.processRow());
    assertFalse(spyTransform.isStopped());
  }
}

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

package org.apache.hop.pipeline.transforms.tableinput;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the Table Input query/parameter path.
 *
 * <p>These tests reproduce the regression where a hop into a Table Input whose SQL has no
 * placeholders was still consumed as parameter rows in 2.20 (Issue #2722 / PR #8034): the incoming
 * row was bound to a PreparedStatement without parameters, which fails on Oracle with ORA-17003 and
 * on H2 with "Parameter index out of range".
 *
 * <p>The fix keeps the 2.20 "optional lookup" feature (collect parameter rows from any incoming
 * hop, named parameters) while restoring the legacy behavior for SQL without placeholders: no
 * collection, no bind, and the incoming hops are still drained so the sequence (header transform
 * then extraction) keeps working.
 */
class TableInputTest {

  private static final String CONNECTION_NAME = "table-input-test";

  private Variables variables;
  private TableInput tableInput;
  private TableInputMeta meta;
  private TableInputData data;
  private Database db;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    org.apache.hop.core.database.DatabasePluginType.getInstance()
        .registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    variables = new Variables();

    DatabaseMeta databaseMeta = connectionTo("mem:" + UUID.randomUUID());
    // Force the H2 driver to load and validate the connection name before the transform uses it.
    try (Database ignored =
        new Database(
            new org.apache.hop.core.logging.LoggingObject("TableInputTest"),
            variables,
            databaseMeta)) {
      // connecting is enough
    }

    meta = new TableInputMeta();
    meta.setConnection(CONNECTION_NAME);
    meta.setUseNamedParameters(false);
    meta.setSql("SELECT 1");

    data = new TableInputData();
    db = mock(Database.class);
    data.db = db;
    doReturn(new RowMeta()).when(db).getReturnRowMeta();
    doReturn(null).when(db).getRow(any(ResultSet.class));
    doReturn(databaseMeta).when(db).getDatabaseMeta();
    // The no-placeholder path calls openQuery(sql, null, null, ...) so the stubs must match null:
    // on Mockito 5, any(IRowMeta.class) / any(Object[].class) (InstanceOf) reject null; the
    // nullable() variants accept it.
    doReturn(mock(ResultSet.class))
        .when(db)
        .openQuery(
            anyString(),
            nullable(IRowMeta.class),
            nullable(Object[].class),
            anyInt(),
            anyBoolean());

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);
    TransformMeta transformMeta = new TransformMeta("table input", meta);
    pipelineMeta.addTransform(transformMeta);
    // dispatch() in the BaseTransform constructor walks previous/next transforms; none exist here.

    tableInput =
        spy(
            new TableInput(
                transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine())));
    doReturn(transformMeta).when(tableInput).getTransformMeta();
    doReturn(false).when(tableInput).isRowLevel();
    doReturn(false).when(tableInput).isDebug();
    doNothing().when(tableInput).logDetailed(any());
    doNothing().when(tableInput).logBasic(any());
    doNothing().when(tableInput).logDebug(any());
    doNothing().when(tableInput).logError(any());
    // doQuery() reads through data.db; make the query "open" successfully with no rows.
    // (single stub above already matches both the null-params and bound-params paths)
  }

  private DatabaseMeta connectionTo(String database) {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            CONNECTION_NAME, "H2", "Native", "", database + ";DB_CLOSE_DELAY=-1", "", "", "");
    databaseMeta.setSupportsTimestampDataType(true);
    return databaseMeta;
  }

  private void stubHeaderRow() throws Exception {
    // One incoming row (the "header" row), then end of input.
    IRowMeta headerRowMeta = new RowMeta();
    headerRowMeta.addValueMeta(new ValueMetaString("header_flag"));
    doReturn(new Object[] {"H"}, (Object) null).when(tableInput).getRow();
    doReturn(headerRowMeta).when(tableInput).getInputRowMeta();
  }

  private static void assertQueryWithoutBind(Database db, String sql) throws Exception {
    // The query must run with no parameter metadata/data at all:
    verify(db, times(1))
        .openQuery(eq(sql), isNull(IRowMeta.class), isNull(Object[].class), anyInt(), eq(false));
    // ... and no attempt to bind values to a statement:
    verify(db, never())
        .setValues(any(IRowMeta.class), any(Object[].class), any(PreparedStatement.class));
  }

  /**
   * The transform must succeed: an empty result set makes processRow() return false immediately
   * (done), so the meaningful assertion is that no error was recorded and the pipeline was not
   * stopped.
   */
  private static void assertSucceeded(TableInput tableInput) {
    org.junit.jupiter.api.Assertions.assertEquals(0, tableInput.getErrors());
    verify(tableInput, never()).stopAll();
  }

  /**
   * Regression test: an incoming hop into a Table Input whose SQL has NO placeholder (e.g. a
   * header/sequencing hop) must NOT be bound to the statement. Before the fix, 2.20 collected the
   * incoming row (optional lookup) and passed it to the PreparedStatement, which fails with
   * ORA-17003 / H2 "Parameter index out of range" because the SQL has no bind variables.
   */
  @Test
  void sqlWithoutParametersDoesNotBindIncomingRows() throws Exception {
    stubHeaderRow();

    tableInput.processRow();

    assertQueryWithoutBind(db, "SELECT 1");
    // The header row must still be drained so upstream transforms can complete (sequencing).
    verify(tableInput, times(2)).getRow(); // one header row + the null terminator
    assertSucceeded(tableInput);
  }

  /**
   * The 2.20 feature must keep working: named parameters {field} are still bound even when the
   * lookup (Insert data from transform) is empty, as long as the SQL declares them.
   */
  @Test
  void namedParametersStillBindEvenWithoutLookup() throws Exception {
    meta.setUseNamedParameters(true);
    meta.setSql("SELECT ID, NAME FROM T WHERE NAME = {name}");
    stubHeaderRow();
    IRowMeta incoming = new RowMeta();
    incoming.addValueMeta(new ValueMetaString("name"));
    doReturn(incoming).when(tableInput).getInputRowMeta();

    tableInput.processRow();

    verify(db, times(1))
        .openQuery(
            eq("SELECT ID, NAME FROM T WHERE NAME = ?"),
            notNull(IRowMeta.class),
            notNull(Object[].class),
            anyInt(),
            eq(false));
    assertSucceeded(tableInput);
  }

  /**
   * Legacy positional placeholders (IN (?,?,?)) must still bind values from the collected incoming
   * rows even with no lookup and named parameters off.
   */
  @Test
  void positionalQuestionMarksStillBindWithoutLookup() throws Exception {
    meta.setUseNamedParameters(false);
    meta.setSql("SELECT ID FROM T WHERE BAR IN (?,?,?)");
    stubHeaderRow();
    IRowMeta incoming = new RowMeta();
    incoming.addValueMeta(new ValueMetaString("bar"));
    incoming.addValueMeta(new ValueMetaString("bar"));
    incoming.addValueMeta(new ValueMetaString("bar"));
    doReturn(incoming).when(tableInput).getInputRowMeta();

    tableInput.processRow();

    verify(db, times(1))
        .openQuery(
            eq("SELECT ID FROM T WHERE BAR IN (?,?,?)"),
            notNull(IRowMeta.class),
            notNull(Object[].class),
            anyInt(),
            eq(false));
    assertSucceeded(tableInput);
  }

  /** A '?' inside a string literal is not a bind placeholder and must not trigger a bind. */
  @Test
  void literalQuestionMarksAreNotPlaceholders() throws Exception {
    stubHeaderRow();
    meta.setSql("SELECT '?' AS Q FROM T WHERE X = 1");

    tableInput.processRow();

    assertQueryWithoutBind(db, "SELECT '?' AS Q FROM T WHERE X = 1");
    assertSucceeded(tableInput);
  }
}

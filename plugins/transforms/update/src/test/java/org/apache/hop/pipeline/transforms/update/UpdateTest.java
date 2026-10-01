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

package org.apache.hop.pipeline.transforms.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Covers the SQL the Update transform builds and the decisions it makes per row. The transform is
 * driven with a mocked {@link Database} so no server is needed: {@code data.db} is injected
 * directly rather than through {@code init()}, which would open a real connection.
 */
class UpdateTest {

  private static final String SCHEMA_TABLE = "\"db_name\".\"table_name\"";

  private TransformMockHelper<UpdateMeta, UpdateData> helper;
  private Database db;
  private Connection connection;
  private PreparedStatement lookupStatement;
  private PreparedStatement updateStatement;

  @BeforeEach
  void setUp() throws Exception {
    helper = new TransformMockHelper<>("update", UpdateMeta.class, UpdateData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.quoteField(anyString())).thenAnswer(i -> "\"" + i.getArgument(0) + "\"");
    when(databaseMeta.stripCR(any(StringBuilder.class)))
        .thenAnswer(i -> i.getArgument(0).toString().replace("\n", " ").replace("\r", " "));
    when(helper.pipelineMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);

    lookupStatement = mock(PreparedStatement.class);
    updateStatement = mock(PreparedStatement.class);
    connection = mock(Connection.class);
    // Keyed on the statement itself rather than call order: with "skip lookup" on, prepareUpdate()
    // is the only statement prepared.
    when(connection.prepareStatement(anyString()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(0, String.class).startsWith("SELECT")
                    ? lookupStatement
                    : updateStatement);

    db = mock(Database.class);
    when(db.getConnection()).thenReturn(connection);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  // ---------------------------------------------------------------- SQL generation

  @Test
  void lookupSqlUsesEveryKeyCondition() throws Exception {
    Update update = updateTransform(metaWith(false, allConditionKeys()), newData());

    update.setLookup(wideRowMeta());

    assertEquals(
        "SELECT \"name\" FROM "
            + SCHEMA_TABLE
            + " WHERE  ( ( \"id\" = ?  ) )  AND  ( ( \"day\" BETWEEN ? AND ?  ) ) "
            + " AND  ( ( \"deleted\" IS NULL  ) )  AND  ( ( \"created\" IS NOT NULL  ) ) "
            + " AND  ( ( \"note\" IS NULL AND ? IS NULL ) OR ( \"note\" = ? ) ) ",
        capturePreparedSql());
  }

  @Test
  void updateSqlUsesEveryKeyCondition() throws Exception {
    Update update = updateTransform(metaWith(false, allConditionKeys()), newData());

    update.prepareUpdate(wideRowMeta());

    assertEquals(
        "UPDATE "
            + SCHEMA_TABLE
            + " SET \"name\" = ? WHERE  ( ( \"id\" = ?  ) ) AND    ( ( \"day\" BETWEEN ? AND ?  ) ) "
            + "AND    ( ( \"deleted\" IS NULL  ) ) AND    ( ( \"created\" IS NOT NULL  ) ) "
            + "AND    ( ( \"note\" IS NULL AND ? IS NULL ) OR ( \"note\" = ? ) ) ",
        capturePreparedSql());
  }

  /** Every bound key contributes a parameter; IS NULL / IS NOT NULL bind nothing. */
  @Test
  void parameterRowMetaMatchesTheBoundPlaceholders() throws Exception {
    UpdateData data = newData();
    Update update = updateTransform(metaWith(false, allConditionKeys()), data);

    update.setLookup(wideRowMeta());
    update.prepareUpdate(wideRowMeta());

    // "= ~NULL" binds its stream field twice (IS NULL check plus equality check); the second copy
    // is a clone, which the row meta auto-renames to keep the names unique.
    assertEquals(
        List.of("id", "day", "to_day", "note", "note_1"),
        data.lookupParameterRowMeta.getValueMetaList().stream().map(v -> v.getName()).toList());
    assertEquals(
        List.of("name", "id", "day", "to_day", "note", "note_1"),
        data.updateParameterRowMeta.getValueMetaList().stream().map(v -> v.getName()).toList());
  }

  // ------------------------------------------------------- issue #4772: no keys configured

  /**
   * The lookup keys build the WHERE clause of both statements. An empty key grid used to produce
   * SQL ending in a dangling WHERE, which the database rejected with a confusing syntax error. See
   * <a href="https://github.com/apache/hop/issues/4772">issue #4772</a>.
   */
  @Test
  void prepareUpdateWithoutKeysFails() {
    Update update = updateTransform(metaWith(true, List.of()), newData());

    assertThrows(HopTransformException.class, () -> update.prepareUpdate(simpleRowMeta()));
  }

  /** Not specific to "skip lookup": the lookup SELECT is just as broken without keys. */
  @Test
  void setLookupWithoutKeysFails() {
    Update update = updateTransform(metaWith(false, List.of()), newData());

    assertThrows(HopTransformException.class, () -> update.setLookup(simpleRowMeta()));
  }

  /** Skipping the lookup skips the SELECT, not the WHERE clause of the UPDATE. */
  @Test
  void skippingTheLookupStillFiltersOnTheKeys() throws Exception {
    Update update = updateTransform(metaWith(true, List.of(idKey())), newData());

    update.prepareUpdate(simpleRowMeta());

    assertEquals(
        "UPDATE " + SCHEMA_TABLE + " SET \"name\" = ? WHERE  ( ( \"id\" = ?  ) ) ",
        capturePreparedSql());
  }

  // ---------------------------------------------------------------- per-row behaviour

  @Test
  void skippingTheLookupUpdatesWithoutSelecting() throws Exception {
    Update update = runRows(metaWith(true, List.of(idKey())), row("1", "Alice"));

    verify(db, never()).getLookup(any(PreparedStatement.class));
    verify(db).insertRow(updateStatement, false, true);
    assertEquals(1, update.getLinesUpdated());
    assertEquals(0, update.getLinesSkipped());
  }

  @Test
  void unchangedValuesAreSkippedInsteadOfUpdated() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(new Object[] {"Alice"});
    when(db.getReturnRowMeta()).thenReturn(returnRowMeta());

    Update update = runRows(metaWith(false, List.of(idKey())), row("1", "Alice"));

    verify(db, never()).insertRow(any(PreparedStatement.class), anyBoolean(), anyBoolean());
    assertEquals(0, update.getLinesUpdated());
    assertEquals(1, update.getLinesSkipped());
  }

  @Test
  void changedValuesAreUpdated() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(new Object[] {"Bob"});
    when(db.getReturnRowMeta()).thenReturn(returnRowMeta());

    Update update = runRows(metaWith(false, List.of(idKey())), row("1", "Alice"));

    verify(db).insertRow(updateStatement, false, true);
    assertEquals(1, update.getLinesUpdated());
    assertEquals(0, update.getLinesSkipped());
  }

  /** With "ignore lookup failure" the row survives, flagged as not found. */
  @Test
  void missingKeyIsFlaggedWhenLookupFailureIsIgnored() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(null);

    UpdateMeta meta = metaWith(false, List.of(idKey()));
    meta.setErrorIgnored(true);
    meta.setIgnoreFlagField("was_found");

    QueueRowSet output = new QueueRowSet();
    Update update = runRows(meta, output, row("1", "Alice"));

    verify(db, never()).insertRow(any(PreparedStatement.class), anyBoolean(), anyBoolean());
    assertEquals(0, update.getLinesUpdated());
    Object[] emitted = output.getRow();
    assertEquals(Boolean.FALSE, emitted[2], "the flag field must report the key was not found");
  }

  /** Without that option a missing key is an error, not a silent no-op. */
  @Test
  void missingKeyIsAnErrorByDefault() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(null);

    Update update = runRows(metaWith(false, List.of(idKey())), row("1", "Alice"));

    verify(db, never()).insertRow(any(PreparedStatement.class), anyBoolean(), anyBoolean());
    assertEquals(1, update.getErrors());
  }

  @Test
  void aKeyFieldMissingFromTheInputIsRejected() {
    UpdateKeyField key = new UpdateKeyField("not_in_stream", "id", "=", "");
    Update update =
        transformFor(metaWith(false, List.of(key)), row("1", "Alice"), new QueueRowSet());

    assertThrows(HopTransformException.class, update::processRow);
  }

  @Test
  void anUpdateFieldMissingFromTheInputIsRejected() {
    UpdateMeta meta = metaWith(false, List.of(idKey()));
    meta.getLookupField().setUpdateFields(List.of(new UpdateField("name", "not_in_stream")));
    Update update = transformFor(meta, row("1", "Alice"), new QueueRowSet());

    assertThrows(HopTransformException.class, update::processRow);
  }

  @Test
  void noInputEndsTheTransform() throws Exception {
    QueueRowSet input = new QueueRowSet();
    input.setDone();
    Update update = updateTransform(metaWith(true, List.of(idKey())), newData());
    update.setInputRowMeta(simpleRowMeta());
    update.addRowSetToInputRowSets(input);
    update.addRowSetToOutputRowSets(new QueueRowSet());

    assertFalse(update.processRow());
  }

  // ---------------------------------------------------------------- lifecycle

  @Test
  void initFailsWithoutAConnection() {
    UpdateMeta meta = metaWith(true, List.of(idKey()));
    meta.setConnection(null);

    assertFalse(updateTransform(meta, newData()).init());
  }

  @Test
  void disposeCommitsAndReleasesTheStatements() throws Exception {
    UpdateData data = newData();
    data.db = db;
    data.prepStatementLookup = lookupStatement;
    data.prepStatementUpdate = updateStatement;
    when(db.isAutoCommit()).thenReturn(false);

    updateTransform(metaWith(true, List.of(idKey())), data).dispose();

    verify(db).emptyAndCommit(updateStatement, false);
    verify(db).closePreparedStatement(lookupStatement);
    verify(db).disconnect();
  }

  @Test
  void batchCompleteCommitsWithoutClosingTheStatements() throws Exception {
    UpdateData data = newData();
    data.db = db;
    data.prepStatementUpdate = updateStatement;
    when(db.isAutoCommit()).thenReturn(false);

    updateTransform(metaWith(true, List.of(idKey())), data).batchComplete();

    verify(db).commit();
    verify(db, never()).disconnect();
  }

  @Test
  void aFailedRowRollsBackOnDispose() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(null);
    when(db.isAutoCommit()).thenReturn(false);

    Update update = runRows(metaWith(false, List.of(idKey())), row("1", "Alice"));
    assertTrue(update.getErrors() > 0, "precondition: the row must have failed");

    update.dispose();

    verify(db).rollback();
    verify(db, never()).emptyAndCommit(any(PreparedStatement.class), anyBoolean());
  }

  // ---------------------------------------------------------------- fixture

  private Update runRows(UpdateMeta meta, Object[] row) throws HopException {
    return runRows(meta, new QueueRowSet(), row);
  }

  private Update runRows(UpdateMeta meta, QueueRowSet output, Object[] row) throws HopException {
    Update update = transformFor(meta, row, output);
    while (update.processRow()) {
      // drain the input
    }
    return update;
  }

  private Update transformFor(UpdateMeta meta, Object[] row, QueueRowSet output) {
    UpdateData data = newData();

    QueueRowSet input = new QueueRowSet();
    input.putRow(simpleRowMeta(), row);
    input.setDone();

    Update update = updateTransform(meta, data);
    update.setInputRowMeta(simpleRowMeta());
    update.addRowSetToInputRowSets(input);
    update.addRowSetToOutputRowSets(output);
    return update;
  }

  private Update updateTransform(UpdateMeta meta, UpdateData data) {
    return new Update(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
  }

  private UpdateData newData() {
    UpdateData data = new UpdateData();
    data.schemaTable = SCHEMA_TABLE;
    data.db = db;
    return data;
  }

  private String capturePreparedSql() throws Exception {
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(connection, times(1)).prepareStatement(sql.capture());
    return sql.getValue();
  }

  private static UpdateMeta metaWith(boolean skipLookup, List<UpdateKeyField> keys) {
    UpdateMeta meta = new UpdateMeta();
    meta.setDefault();
    meta.setConnection("db");
    meta.setSkipLookup(skipLookup);
    UpdateLookupField lookupField = new UpdateLookupField();
    lookupField.setTableName("table_name");
    lookupField.setLookupKeys(keys);
    lookupField.setUpdateFields(List.of(new UpdateField("name", "name")));
    meta.setLookupField(lookupField);
    return meta;
  }

  private static UpdateKeyField idKey() {
    return new UpdateKeyField("id", "id", "=", "");
  }

  private static List<UpdateKeyField> allConditionKeys() {
    return List.of(
        new UpdateKeyField("id", "id", "=", ""),
        new UpdateKeyField("day", "day", "BETWEEN", "to_day"),
        new UpdateKeyField("", "deleted", "IS NULL", ""),
        new UpdateKeyField("", "created", "IS NOT NULL", ""),
        new UpdateKeyField("note", "note", "= ~NULL", ""));
  }

  private static IRowMeta simpleRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  private static IRowMeta wideRowMeta() {
    IRowMeta rowMeta = simpleRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("day"));
    rowMeta.addValueMeta(new ValueMetaString("to_day"));
    rowMeta.addValueMeta(new ValueMetaString("note"));
    return rowMeta;
  }

  private static IRowMeta returnRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  private static Object[] row(String id, String name) {
    return new Object[] {id, name};
  }
}

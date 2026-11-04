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

package org.apache.hop.pipeline.transforms.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.ISavepointWork;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What runs inside the savepoint section of a row and what runs after it. Only the row's SQL may be
 * inside: a row passed on or sent to the error hop inside the section could block on a full buffer
 * while the other transforms of a transactional pipeline wait for the section, and a commit inside
 * the section would end the savepoint before it is released.
 */
class UpdateSavepointTest {

  private TransformMockHelper<UpdateMeta, UpdateData> helper;
  private Database db;
  private PreparedStatement lookupStatement;
  private PreparedStatement updateStatement;

  /** Whether the mocked database is inside runInSavepoint. */
  private boolean inSection;

  /** Every call of interest, with "(in section)" when it happened inside it. */
  private final List<String> events = new ArrayList<>();

  @BeforeEach
  void setUp() throws Exception {
    helper = new TransformMockHelper<>("update", UpdateMeta.class, UpdateData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.quoteField(anyString())).thenAnswer(i -> "\"" + i.getArgument(0) + "\"");
    when(databaseMeta.stripCR(any(StringBuilder.class)))
        .thenAnswer(i -> i.getArgument(0).toString());
    when(helper.pipelineMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);

    lookupStatement = mock(PreparedStatement.class);
    updateStatement = mock(PreparedStatement.class);
    Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(0, String.class).startsWith("SELECT")
                    ? lookupStatement
                    : updateStatement);

    db = mock(Database.class);
    when(db.getConnection()).thenReturn(connection);
    when(db.getReturnRowMeta()).thenReturn(nameRowMeta());
    when(db.runInSavepoint(any()))
        .thenAnswer(
            invocation -> {
              inSection = true;
              events.add("savepoint");
              try {
                return invocation.getArgument(0, ISavepointWork.class).run();
              } finally {
                inSection = false;
                events.add("released or rolled back");
              }
            });
    doAnswer(
            invocation -> {
              events.add("update" + where());
              return false;
            })
        .when(db)
        .insertRow(any(PreparedStatement.class), anyBoolean(), anyBoolean());
    doAnswer(
            invocation -> {
              events.add("commit" + where());
              return null;
            })
        .when(db)
        .commit();
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  private String where() {
    return inSection ? " (in section)" : "";
  }

  @Test
  void onlyTheUpdateIsInsideTheSection() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(new Object[] {"Bob"});

    run(1, null, row("1", "Alice"));

    assertEquals(
        List.of("savepoint", "update (in section)", "released or rolled back", "commit", "row out"),
        events);
    // The transform commits, not insertRow: a commit inside the section would end the savepoint.
    verify(db).insertRow(updateStatement, false, false);
  }

  @Test
  void theCommitComesEveryCommitSizeRowsAfterTheSection() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(new Object[] {"Bob"});

    run(2, null, row("1", "Alice"), row("2", "Carol"), row("3", "Dave"), row("4", "Eve"));

    verify(db, times(2)).commit();
    assertFalse(events.contains("commit (in section)"));
  }

  /**
   * The bug from the review: a failure after the row's SQL (here passing the row on) went to the
   * catch block that rolled back to the savepoint, which was already released or committed.
   */
  @Test
  void aFailureAfterTheSectionDoesNotRollBack() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(new Object[] {"Bob"});

    List<Object[]> errors = new ArrayList<>();
    run(100, errors, new HopTransformException("output refused the row"), row("1", "Alice"));

    verify(db, never()).rollback(any(Savepoint.class));
    verify(db, never()).releaseSavepoint(any(Savepoint.class));
    verify(db, times(1)).runInSavepoint(any());
    assertEquals(1, errors.size());
  }

  @Test
  void aFailedUpdateGoesToTheErrorHopAfterTheSection() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(new Object[] {"Bob"});
    doAnswer(
            invocation -> {
              events.add("update" + where());
              throw new HopDatabaseException("value too long");
            })
        .when(db)
        .insertRow(any(PreparedStatement.class), anyBoolean(), anyBoolean());

    List<Object[]> errors = new ArrayList<>();
    run(100, errors, row("1", "Alice"));

    assertEquals(
        List.of("savepoint", "update (in section)", "released or rolled back", "error out"),
        events);
    assertEquals(1, errors.size());
  }

  /** A key that is not found goes to the error hop too, but only after the section. */
  @Test
  void aMissingKeyGoesToTheErrorHopAfterTheSection() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(null);

    List<Object[]> errors = new ArrayList<>();
    run(100, errors, row("1", "Alice"), row("2", "Bob"));

    assertEquals(
        List.of(
            "savepoint",
            "released or rolled back",
            "error out",
            "savepoint",
            "released or rolled back",
            "error out"),
        events);
    assertEquals(2, errors.size());
    verify(db, never()).insertRow(any(PreparedStatement.class), anyBoolean(), anyBoolean());
  }

  /** Without savepoints nothing changes: insertRow commits itself, no section. */
  @Test
  void withoutSavepointsThereIsNoSection() throws Exception {
    when(db.getLookup(lookupStatement)).thenReturn(new Object[] {"Bob"});

    Update update = transform(new ArrayList<>(), null, row("1", "Alice"));
    update.getData().useSavepoints = false;
    update.getData().commitSize = 1;
    drain(update);

    verify(db, never()).runInSavepoint(any());
    verify(db).insertRow(updateStatement, false, true);
  }

  private void run(int commitSize, List<Object[]> errors, Object[]... rows) throws HopException {
    run(commitSize, errors, null, rows);
  }

  private void run(
      int commitSize, List<Object[]> errors, HopException putRowFailure, Object[]... rows)
      throws HopException {
    Update update = transform(errors == null ? new ArrayList<>() : errors, putRowFailure, rows);
    update.getData().useSavepoints = true;
    update.getData().commitSize = commitSize;
    drain(update);
  }

  private static void drain(Update update) throws HopException {
    while (update.processRow()) {
      // drain the input
    }
  }

  private Update transform(List<Object[]> errors, HopException putRowFailure, Object[]... rows) {
    UpdateMeta meta = new UpdateMeta();
    meta.setDefault();
    meta.setConnection("db");
    UpdateLookupField lookupField = new UpdateLookupField();
    lookupField.setTableName("table_name");
    lookupField.setLookupKeys(List.of(new UpdateKeyField("id", "id", "=", "")));
    lookupField.setUpdateFields(List.of(new UpdateField("name", "name")));
    meta.setLookupField(lookupField);

    UpdateData data = new UpdateData();
    data.schemaTable = "\"table_name\"";
    data.db = db;

    QueueRowSet input = new QueueRowSet();
    for (Object[] row : rows) {
      input.putRow(inputRowMeta(), row);
    }
    input.setDone();

    Update update =
        new Update(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline) {
          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            events.add("row out" + where());
            if (putRowFailure instanceof HopTransformException failure) {
              throw failure;
            }
          }

          @Override
          public void putError(
              IRowMeta rowMeta,
              Object[] row,
              long nrErrors,
              String errorDescriptions,
              String fieldNames,
              String errorCodes) {
            events.add("error out" + where());
            errors.add(row);
          }
        };
    update.setInputRowMeta(inputRowMeta());
    update.addRowSetToInputRowSets(input);
    update.addRowSetToOutputRowSets(new QueueRowSet());
    return update;
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  private static IRowMeta nameRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  private static Object[] row(String id, String name) {
    return new Object[] {id, name};
  }
}

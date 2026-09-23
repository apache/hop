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
package org.apache.hop.pgvector.transforms.upsert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pgvector.util.PgVectorSchemaBuilder;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class PgVectorUpsertTest {

  private TransformMockHelper<PgVectorUpsertMeta, PgVectorUpsertData> helper;
  private final Map<Integer, Object> bound = new HashMap<>();
  private List<Object[]> passed;
  private List<Object[]> diverted;
  private Database database;
  private Savepoint savepoint;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "PgVectorUpsert", PgVectorUpsertMeta.class, PgVectorUpsertData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    bound.clear();
    passed = new ArrayList<>();
    diverted = new ArrayList<>();
    database = mock(Database.class);
    savepoint = mock(Savepoint.class);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void buildsTheSyntheticKeyFromTheChunkIndexThatIsStored() throws Exception {
    // A null chunk index is bound as 0, so the key has to say 0 too and not "null".
    run(
        newMeta(),
        stubStatement(),
        List.<Object[]>of(new Object[] {"doc-a", null, "text", "[1,2]"}));

    assertEquals("doc-a_0", bound.get(1), "id column");
    assertEquals(0, bound.get(3), "chunk_index column");
  }

  @Test
  void divertsOnlyTheFailingRowWhenErrorHandlingIsOn() throws Exception {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);

    PreparedStatement statement = stubStatement();
    // Reject the second row only.
    doAnswer(rejectCall(2)).when(statement).executeUpdate();

    run(
        newMeta(),
        statement,
        List.<Object[]>of(
            new Object[] {"doc-a", "0", "one", "[1,2]"},
            new Object[] {"doc-a", "1", "two", "[1,2]"},
            new Object[] {"doc-a", "2", "three", "[1,2]"}));

    assertEquals(2, passed.size(), "the rows that stored must still be passed on");
    assertEquals(1, diverted.size(), "only the rejected row goes to the error hop");
  }

  @Test
  void revertsToASavepointSoRowsAfterARejectedOneStillStore() throws Exception {
    // PostgreSQL aborts the whole transaction on a failed statement. Without a savepoint rollback
    // every row after the first rejection fails too, and the error hop swallows the lot.
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    when(database.setSavepoint()).thenReturn(savepoint);

    PreparedStatement statement = stubStatement();
    doAnswer(rejectCall(2)).when(statement).executeUpdate();

    PgVectorUpsertData data = newData(newMeta(), statement);
    data.useSafePoints = true;
    data.releaseSavepoint = true;

    run(
        newMeta(),
        data,
        List.<Object[]>of(
            new Object[] {"doc-a", "0", "one", "[1,2]"},
            new Object[] {"doc-a", "1", "two", "[1,2]"},
            new Object[] {"doc-a", "2", "three", "[1,2]"}));

    verify(database, times(1)).rollback(savepoint);
    assertEquals(2, passed.size(), "the rows after the rejected one must still store");
    assertEquals(1, diverted.size(), "only the rejected row goes to the error hop");
  }

  @Test
  void commitsEveryCommitSizeRowsOutsideBatchMode() throws Exception {
    PgVectorUpsertData data = newData(newMeta(), stubStatement());
    data.commitSize = 2;

    run(
        newMeta(),
        data,
        List.<Object[]>of(
            new Object[] {"doc-a", "0", "one", "[1,2]"},
            new Object[] {"doc-a", "1", "two", "[1,2]"},
            new Object[] {"doc-a", "2", "three", "[1,2]"},
            new Object[] {"doc-a", "3", "four", "[1,2]"},
            new Object[] {"doc-a", "4", "five", "[1,2]"}));

    // Rows 2 and 4 hit the commit size; the fifth is left for the end-of-run commit.
    verify(database, times(2)).commit();
  }

  @Test
  void doesNotCommitPerRowWhenTheCommitSizeIsNotReached() throws Exception {
    PgVectorUpsertData data = newData(newMeta(), stubStatement());
    data.commitSize = Integer.MAX_VALUE;

    run(
        newMeta(),
        data,
        List.<Object[]>of(
            new Object[] {"doc-a", "0", "one", "[1,2]"},
            new Object[] {"doc-a", "1", "two", "[1,2]"}));

    verify(database, never()).commit();
  }

  private static org.mockito.stubbing.Answer<Integer> rejectCall(int failingCall) {
    return new org.mockito.stubbing.Answer<>() {
      private int call = 0;

      @Override
      public Integer answer(org.mockito.invocation.InvocationOnMock invocation) throws Throwable {
        if (++call == failingCall) {
          throw new SQLException("duplicate key");
        }
        return 1;
      }
    };
  }

  @Test
  void rejectsARowThatHasNoDocumentIdToBuildAKeyFrom() throws Exception {
    // A chunk index repeats across documents, so "null_3" would be a key built from nothing.
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);

    run(
        newMeta(),
        stubStatement(),
        java.util.Arrays.<Object[]>asList(new Object[] {null, "3", "text", "[1,2]"}));

    assertEquals(0, passed.size(), "a row with no usable key must not be stored");
    assertEquals(1, diverted.size(), "it belongs on the error hop");
  }

  @Test
  void keepsTheDocumentIdWhenOnlyTheChunkIndexIsMissing() throws Exception {
    run(
        newMeta(),
        stubStatement(),
        java.util.Arrays.<Object[]>asList(new Object[] {"doc-a", null, "text", "[1,2]"}));

    assertEquals("doc-a_0", bound.get(1), "id column");
    assertEquals(1, passed.size(), "a missing chunk index is read as 0, not a failure");
  }

  @Test
  void neverBatchesWithAnErrorHopEvenWhereSavepointsAreUnavailable() {
    // A dialect without savepoints still cannot map a failed executeBatch back onto rows, so
    // batching would lose them: neither written nor diverted.
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    IDatabase iDatabase = mock(IDatabase.class);
    when(iDatabase.isUseSafePoints()).thenReturn(false);

    PgVectorUpsertMeta meta = newMeta();
    meta.setCommitSize("500");
    PgVectorUpsertData data = newData(meta, mock(PreparedStatement.class));
    PgVectorUpsert transform =
        new PgVectorUpsert(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform.configureCommitStrategy(iDatabase);

    assertFalse(data.batchMode, "an error hop must rule out batching on any dialect");
  }

  @Test
  void batchesWhenNoErrorHopIsAttached() {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(false);
    IDatabase iDatabase = mock(IDatabase.class);
    when(iDatabase.isUseSafePoints()).thenReturn(true);

    PgVectorUpsertMeta meta = newMeta();
    meta.setCommitSize("500");
    PgVectorUpsertData data = newData(meta, mock(PreparedStatement.class));
    PgVectorUpsert transform =
        new PgVectorUpsert(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform.configureCommitStrategy(iDatabase);

    assertTrue(data.batchMode, "batching is the default without an error hop");
    assertFalse(data.useSafePoints, "savepoints are only needed to divert rows");
  }

  @Test
  void revertsToASavepointWhenTheDocumentDeleteFails() throws Exception {
    // A failed delete aborts the transaction exactly as a failed insert does.
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    when(database.setSavepoint()).thenReturn(savepoint);

    PgVectorUpsertMeta meta = newMeta();
    meta.setDeleteDocumentBeforeUpsert(true);

    PreparedStatement deleteStatement = mock(PreparedStatement.class);
    when(deleteStatement.executeUpdate()).thenThrow(new SQLException("deadlock detected"));

    PgVectorUpsertData data = newData(meta, stubStatement());
    data.deleteStatement = deleteStatement;
    data.useSafePoints = true;
    data.releaseSavepoint = true;

    run(
        meta,
        data,
        List.<Object[]>of(
            new Object[] {"doc-a", "0", "one", "[1,2]"},
            new Object[] {"doc-b", "0", "two", "[1,2]"}));

    verify(database, times(2)).rollback(savepoint);
    assertEquals(2, diverted.size(), "both deletes fail, so both rows are diverted");
  }

  @Test
  void runsSchemaDdlInAutocommitBeforeSwitchingToTheDataCommitSize() throws Exception {
    // Inside the data transaction the rollback of one failed row would undo the created table.
    PgVectorUpsertMeta meta = newMeta();
    meta.setCreateTableIfMissing(true);
    meta.setEmbeddingDimensions("768");

    PgVectorUpsertData data = newData(meta, mock(PreparedStatement.class));
    data.commitSize = 500;
    PgVectorUpsert transform =
        new PgVectorUpsert(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform.prepareSchemaAndCommitMode("public", "chunks");

    InOrder inOrder = inOrder(database);
    inOrder.verify(database).setCommit(0);
    inOrder.verify(database, atLeastOnce()).execStatement(anyString());
    inOrder.verify(database).setCommit(500);
  }

  @Test
  void refusesToDeleteADocumentOnceTheTrackedSetIsFull() throws Exception {
    // Evicting an entry would let a document be deleted twice, the second time taking the chunks
    // this run had already written for it.
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    PgVectorUpsertMeta meta = newMeta();
    meta.setDeleteDocumentBeforeUpsert(true);

    PreparedStatement deleteStatement = mock(PreparedStatement.class);
    PgVectorUpsertData data = newData(meta, stubStatement());
    data.deleteStatement = deleteStatement;
    for (int i = 0; i < 1_000_000; i++) {
      data.deletedDocuments.add("filler-" + i);
    }

    run(meta, data, List.<Object[]>of(new Object[] {"doc-new", "0", "one", "[1,2]"}));

    verify(deleteStatement, never()).executeUpdate();
    assertEquals(1, diverted.size(), "the row is diverted rather than silently mis-deleted");
    assertEquals(0, passed.size(), "and it must not be written");
  }

  private PgVectorUpsertMeta newMeta() {
    PgVectorUpsertMeta meta = new PgVectorUpsertMeta();
    meta.setDefault();
    meta.setDocumentIdField("document_id");
    meta.setChunkIndexField("chunk_index");
    meta.setContentField("content");
    meta.setEmbeddingField("embedding");
    meta.setCreateTableIfMissing(false);
    return meta;
  }

  private PreparedStatement stubStatement() throws Exception {
    PreparedStatement statement = mock(PreparedStatement.class);
    doAnswer(
            invocation -> {
              bound.put(invocation.getArgument(0), invocation.getArgument(1));
              return null;
            })
        .when(statement)
        .setString(anyInt(), anyString());
    doAnswer(
            invocation -> {
              bound.put(invocation.getArgument(0), invocation.getArgument(1));
              return null;
            })
        .when(statement)
        .setInt(anyInt(), anyInt());
    when(statement.executeUpdate()).thenReturn(1);
    return statement;
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("document_id"));
    inputRowMeta.addValueMeta(new ValueMetaString("chunk_index"));
    inputRowMeta.addValueMeta(new ValueMetaString("content"));
    inputRowMeta.addValueMeta(new ValueMetaString("embedding"));
    return inputRowMeta;
  }

  /** Mirrors what openDatabase() and resolveFieldIndices() set up at run time. */
  private PgVectorUpsertData newData(PgVectorUpsertMeta meta, PreparedStatement statement) {
    PgVectorUpsertData data = new PgVectorUpsertData();
    data.inputRowMeta = inputRowMeta();
    data.documentIdFieldIndex = 0;
    data.chunkIndexFieldIndex = 1;
    data.contentFieldIndex = 2;
    data.embeddingFieldIndex = 3;
    data.tableColumns = PgVectorSchemaBuilder.tableColumns(meta);
    // One binding per column, parameter indexes starting at 1.
    int parameterIndex = 1;
    for (org.apache.hop.pgvector.util.PgVectorTableColumn column : data.tableColumns) {
      int streamIndex =
          switch (column.name()) {
            case "document_id" -> 0;
            case "chunk_index" -> 1;
            case "content" -> 2;
            case "embedding" -> 3;
            default -> -1;
          };
      data.mappingBindings.add(
          new PgVectorUpsertData.MappingBinding(streamIndex, parameterIndex++, column));
    }
    data.insertStatement = statement;
    data.database = database;
    // Single-row mode is what an attached error hop forces, and what these tests exercise.
    data.batchMode = false;
    data.commitSize = Integer.MAX_VALUE;
    return data;
  }

  private void run(PgVectorUpsertMeta meta, PreparedStatement statement, List<Object[]> rows)
      throws Exception {
    run(meta, newData(meta, statement), rows);
  }

  private void run(PgVectorUpsertMeta meta, PgVectorUpsertData data, List<Object[]> rows)
      throws Exception {
    IRowMeta inputRowMeta = data.inputRowMeta;

    PgVectorUpsert transform =
        spy(
            new PgVectorUpsert(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    transform.init();
    transform.setInputRowMeta(inputRowMeta);
    // The statement is already stubbed, so skip the connect branch in processRow().
    transform.first = false;

    Iterator<Object[]> iterator = rows.iterator();
    doAnswer(invocation -> iterator.hasNext() ? iterator.next() : null).when(transform).getRow();
    doAnswer(
            invocation -> {
              passed.add(invocation.getArgument(1));
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any(Object[].class));
    doAnswer(
            invocation -> {
              diverted.add(invocation.getArgument(1));
              return null;
            })
        .when(transform)
        .putError(any(), any(), anyLong(), any(), any(), any());

    while (transform.processRow()) {
      // drain
    }
  }
}

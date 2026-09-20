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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Releasing the database connection when the end of the input never comes.
 *
 * <p>The transform used to flush and disconnect only from the end-of-input branch of {@code
 * processRow()}. A stop or a failure in the middle of a row skips that branch, and a
 * single-threaded (streaming) pipeline never reaches it at all: the connection then stayed open for
 * as long as the JVM lived, and with it the uncommitted transaction and the row locks it held on
 * the database. See <a href="https://github.com/apache/hop/issues/8288">issue 8288</a>.
 *
 * <p>The invariants pinned here: {@code dispose()} always ends with {@code disconnect()} when a
 * connection is open, commits pending work unless the transform failed, and {@code batchComplete()}
 * commits between batches without closing the statements the next batch reuses.
 */
class SynchronizeAfterMergeDisposeTest {

  private static final String INSERT_KEY = "\"T\"" + SynchronizeAfterMerge.CONST_INSERT;
  private static final String UPDATE_KEY = "\"T\"" + SynchronizeAfterMerge.CONST_UPDATE;

  private SynchronizeAfterMerge transform;
  private SynchronizeAfterMergeData data;
  private TransformMeta transformMeta;
  private Database db;
  private PreparedStatement insertStatement;
  private PreparedStatement updateStatement;
  private List<Object[]> emitted;
  private List<String> rejected;

  @BeforeEach
  void setUp() throws Exception {
    SynchronizeAfterMergeMeta meta = mock(SynchronizeAfterMergeMeta.class);
    transformMeta = mock(TransformMeta.class);
    doReturn("transform").when(transformMeta).getName();
    doReturn(mock(TransformPartitioningMeta.class))
        .when(transformMeta)
        .getTargetTransformPartitioningMeta();
    doReturn(meta).when(transformMeta).getTransform();

    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    doReturn(transformMeta).when(pipelineMeta).findTransform(anyString());

    db = mock(Database.class);
    Connection connection = mock(Connection.class);
    doReturn(connection).when(db).getConnection();

    insertStatement = mock(PreparedStatement.class);
    updateStatement = mock(PreparedStatement.class);

    data = new SynchronizeAfterMergeData();
    data.db = db;
    data.batchMode = true;
    data.insertValue = "insert";
    data.indexOfOperationOrderField = 1;
    data.inputRowMeta = new RowMeta();
    data.inputRowMeta.addValueMeta(new ValueMetaString("name"));
    data.inputRowMeta.addValueMeta(new ValueMetaString("operation"));
    data.outputRowMeta = data.inputRowMeta;
    data.preparedStatements.put(INSERT_KEY, insertStatement);
    data.preparedStatements.put(UPDATE_KEY, updateStatement);
    data.commitCounterMap.put(INSERT_KEY, 3);
    data.commitCounterMap.put(UPDATE_KEY, 2);
    data.batchBuffer = new ArrayList<>();

    transform =
        spy(
            new SynchronizeAfterMerge(
                transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine())));
    doReturn(transformMeta).when(transform).getTransformMeta();
    doReturn(false).when(transform).isRowLevel();
    doNothing().when(transform).logDetailed(anyString());
    doNothing().when(transform).logError(anyString());
    doNothing().when(transform).logError(anyString(), any(Throwable.class));

    emitted = new ArrayList<>();
    rejected = new ArrayList<>();
    doAnswer(
            inv -> {
              emitted.add(inv.getArgument(1));
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any());
    doAnswer(
            inv -> {
              rejected.add(inv.getArgument(3));
              return null;
            })
        .when(transform)
        .putError(
            any(IRowMeta.class),
            any(),
            anyLong(),
            anyString(),
            nullable(String.class),
            anyString());
  }

  private void bufferRows(int count) {
    for (int i = 0; i < count; i++) {
      data.batchBuffer.add(new Object[] {"row" + i, "insert"});
    }
  }

  /** The stop-in-the-middle-of-a-row and the streaming case: nothing flushed us before. */
  @Test
  void disposeFlushesCommitsAndDisconnectsWhenEndOfInputNeverCame() throws Exception {
    bufferRows(5);

    transform.dispose();

    InOrder order = inOrder(db);
    order.verify(db).emptyAndCommit(insertStatement, true, 3, true);
    order.verify(db).emptyAndCommit(updateStatement, true, 2, true);
    order.verify(db).disconnect();
    verify(db, never()).rollback();
    assertEquals(5, emitted.size(), "the buffered rows leave on the output before we disconnect");
    assertTrue(data.batchBuffer.isEmpty());
    assertNull(data.db, "the database handle is released for the garbage collector");
  }

  /** After a failure nothing that is still pending may reach the table. */
  @Test
  void disposeRollsBackAndDisconnectsAfterAFailure() throws Exception {
    bufferRows(5);
    transform.setErrors(1);

    transform.dispose();

    InOrder order = inOrder(db);
    order.verify(db).rollback();
    order.verify(db).disconnect();
    verify(db, never()).emptyAndCommit(any(), anyBoolean(), anyInt(), anyBoolean());
    assertTrue(emitted.isEmpty(), "no row is reported as written after a rollback");
  }

  /** The normal end-of-input path already disconnected: dispose() must not do it twice. */
  @Test
  void disposeIsANoOpOnceTheConnectionIsGone() throws Exception {
    doReturn(null).when(db).getConnection();

    transform.dispose();

    verify(db, never()).disconnect();
    verify(db, never()).rollback();
    assertNull(data.db);
  }

  /** A transform whose init() never got as far as a connection. */
  @Test
  void disposeSurvivesAMissingDatabase() throws Exception {
    data.db = null;

    transform.dispose();

    verify(db, never()).disconnect();
  }

  /** Between batches the statements are reused: commit, but leave them open. */
  @Test
  void batchCompleteCommitsWithoutClosingTheStatements() throws Exception {
    bufferRows(5);

    transform.batchComplete();

    verify(db).emptyAndCommit(insertStatement, true, 3, false);
    verify(db).emptyAndCommit(updateStatement, true, 2, false);
    verify(db, never()).disconnect();
    assertEquals(0, data.commitCounterMap.get(INSERT_KEY), "the batch counter starts over");
    assertEquals(0, data.commitCounterMap.get(UPDATE_KEY), "the batch counter starts over");
    assertEquals(5, emitted.size());
    assertTrue(data.batchBuffer.isEmpty());
  }

  @Test
  void batchCompleteIsANoOpWithoutAConnection() throws Exception {
    doReturn(null).when(db).getConnection();

    transform.batchComplete();

    verify(db, never()).emptyAndCommit(any(), anyBoolean(), anyInt(), anyBoolean());
  }

  /**
   * A batch that fails between batches gets the same recovery as one that fails mid-stream: the
   * failed batches are dropped so the next batch does not re-run them, what went through is
   * committed and every buffered row leaves on one stream or the other.
   */
  @Test
  void batchCompleteWithErrorHandlingRoutesTheFailedBatchAndKeepsGoing() throws Exception {
    doReturn(true).when(transformMeta).isDoingErrorHandling();
    bufferRows(5);
    BatchUpdateException cause =
        new BatchUpdateException("boom", new int[] {1, 1, java.sql.Statement.EXECUTE_FAILED});
    HopDatabaseBatchException failure =
        Database.createHopDatabaseBatchException("Error updating batch", cause);
    doThrow(failure)
        .when(db)
        .emptyAndCommit(eq(insertStatement), anyBoolean(), anyInt(), eq(false));

    transform.batchComplete();

    verify(db).clearBatch(insertStatement);
    verify(db).clearBatch(updateStatement);
    verify(db).commit(true);
    verify(db, never()).rollback();
    verify(db, never()).disconnect();
    assertEquals(5, emitted.size() + rejected.size(), "every buffered row leaves exactly once");
    assertTrue(data.batchBuffer.isEmpty());
  }

  /** Without error handling a failed batch is a failed transform. */
  @Test
  void batchCompleteWithoutErrorHandlingRollsBackAndFails() throws Exception {
    doReturn(false).when(transformMeta).isDoingErrorHandling();
    bufferRows(5);
    HopDatabaseBatchException failure =
        Database.createHopDatabaseBatchException(
            "Error updating batch", new BatchUpdateException("boom", new int[0]));
    doThrow(failure)
        .when(db)
        .emptyAndCommit(eq(insertStatement), anyBoolean(), anyInt(), eq(false));

    assertThrows(HopException.class, () -> transform.batchComplete());

    verify(db).clearBatch(insertStatement);
    verify(db).clearBatch(updateStatement);
    verify(db).rollback();
    verify(db, never()).commit(anyBoolean());
    verify(db, never()).disconnect();
  }
}

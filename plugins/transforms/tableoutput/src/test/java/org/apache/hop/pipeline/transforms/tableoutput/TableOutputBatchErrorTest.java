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

import static java.sql.Statement.EXECUTE_FAILED;
import static java.sql.Statement.SUCCESS_NO_INFO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.database.Database;
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

/**
 * Accounting for the rows of a failed batch.
 *
 * <p>JDBC drivers disagree about what {@link BatchUpdateException#getUpdateCounts()} contains, and
 * the transform used to assume the one shape SQL Server produces. Against a driver that stops at
 * the first failure - Oracle and Derby do - the array is shorter than the batch, and the rows it
 * does not mention used to be dropped on the floor: no output row, no error row, no log line. That
 * is <a href="https://github.com/apache/hop/issues/5758">issue #5758</a>.
 *
 * <p>The invariant every test here checks is the same one: <em>every buffered row leaves on exactly
 * one stream</em>, whatever the driver reported.
 */
class TableOutputBatchErrorTest {

  private static final int BATCH = 10;

  private TableOutput transform;
  private TableOutputData data;
  private TableOutputMeta meta;

  private List<Object[]> written;
  private List<String> rejected;

  @BeforeEach
  void setUp() throws Exception {
    meta = mock(TableOutputMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);
    doReturn("transform").when(transformMeta).getName();
    doReturn(mock(TransformPartitioningMeta.class))
        .when(transformMeta)
        .getTargetTransformPartitioningMeta();
    doReturn(meta).when(transformMeta).getTransform();

    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    doReturn(transformMeta).when(pipelineMeta).findTransform(anyString());

    data = new TableOutputData();
    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta(new ValueMetaString("name"));
    data.insertRowMeta = data.outputRowMeta;
    data.batchBuffer = new ArrayList<>();
    data.batchBindBuffer = new ArrayList<>();
    data.db = mock(Database.class);

    transform =
        spy(
            new TableOutput(
                transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine())));
    doReturn(transformMeta).when(transform).getTransformMeta();
    doReturn(false).when(transform).isRowLevel();
    doNothing().when(transform).logDetailed(anyString());

    written = new ArrayList<>();
    rejected = new ArrayList<>();

    doAnswer(
            inv -> {
              written.add(inv.getArgument(1));
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
      data.batchBuffer.add(new Object[] {"row" + i});
    }
  }

  /** Every buffered row has to come out of exactly one of the two streams. */
  private void assertAllRowsAccountedFor() {
    assertEquals(
        BATCH,
        written.size() + rejected.size(),
        "every buffered row must be emitted exactly once, on one stream or the other");
    assertTrue(data.batchBuffer.isEmpty(), "the buffer must be drained");
  }

  @Test
  void marksFailedRows_splitsOnTheCountsArray() throws HopException {
    // SQL Server, MySQL, H2: a full-length array with EXECUTE_FAILED at the failing rows.
    bufferRows(BATCH);
    int[] counts = {1, 1, 1, EXECUTE_FAILED, 1, 1, EXECUTE_FAILED, 1, 1, 1};

    transform.processBatchException(null, "T", "batch failed", counts, List.of());

    assertEquals(8, written.size());
    assertEquals(2, rejected.size());
    assertAllRowsAccountedFor();
  }

  /** The regression test for issue #5758. */
  @Test
  void keepsPrefix_shortCountsArrayDoesNotDropTheTail() throws HopException {
    // Oracle and Derby stop at the first failure, so a batch of 10 that fails at index 3 comes back
    // with only three counts. Rows 3..9 are not in the database and must be rejected, not dropped.
    bufferRows(BATCH);
    int[] counts = {1, 1, 1};

    transform.processBatchException(
        null, "T", "ORA-00001: unique constraint violated", counts, List.of());

    assertEquals(3, written.size(), "only the rows the driver confirmed may go downstream");
    assertEquals(
        7, rejected.size(), "the failing row and the six never attempted must be rejected");
    assertAllRowsAccountedFor();
  }

  @Test
  void keepsPrefix_failureOnTheFirstRowGivesAnEmptyArray() throws HopException {
    // Oracle returns a zero-length array - not null - when the very first row of the batch fails.
    // The old code's loop simply never ran, and the whole commit window disappeared.
    bufferRows(BATCH);

    transform.processBatchException(null, "T", "ORA-12899: value too large", new int[0], List.of());

    assertEquals(0, written.size());
    assertEquals(BATCH, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void keepsPrefix_theFailingRowIsDistinguishedFromTheUntriedOnes() throws HopException {
    bufferRows(BATCH);

    transform.processBatchException(
        null, "T", "ORA-00001: unique constraint violated", new int[] {1, 1, 1}, List.of());

    assertEquals(
        "ORA-00001: unique constraint violated",
        rejected.get(0),
        "the row the database actually rejected keeps the database's own message");
    for (String message : rejected.subList(1, rejected.size())) {
      assertTrue(
          message.contains("not sent to the database"),
          "rows the driver never attempted must say so, not repeat the first row's error");
    }
  }

  @Test
  void losesWholeBatch_everyRowIsRejected() throws HopException {
    // PostgreSQL and DuckDB abort the transaction: a full-length array of EXECUTE_FAILED, nothing
    // durable.
    bufferRows(BATCH);
    int[] counts = new int[BATCH];
    Arrays.fill(counts, EXECUTE_FAILED);

    transform.processBatchException(null, "T", "current transaction is aborted", counts, List.of());

    assertEquals(0, written.size());
    assertEquals(BATCH, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void noCountsAtAll_everyRowIsRejected() throws HopException {
    // A plain SQLException rather than a BatchUpdateException - SQLite, DuckDB, and SQL Server's
    // batch-aborting error classes.
    bufferRows(BATCH);

    transform.processBatchException(null, "T", "driver reported no update counts", null, List.of());

    assertEquals(0, written.size());
    assertEquals(BATCH, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void successNoInfoCountsAsSuccess() throws HopException {
    // JDBC says success is anything that is not EXECUTE_FAILED. SUCCESS_NO_INFO (-2) means the row
    // was applied without the driver saying how many rows it touched; the old "> 0" test read that
    // as a failure and pushed rows that ARE in the table onto the error stream.
    bufferRows(BATCH);
    int[] counts = {
      SUCCESS_NO_INFO, SUCCESS_NO_INFO, SUCCESS_NO_INFO, EXECUTE_FAILED, SUCCESS_NO_INFO,
      SUCCESS_NO_INFO, SUCCESS_NO_INFO, SUCCESS_NO_INFO, SUCCESS_NO_INFO, SUCCESS_NO_INFO
    };

    transform.processBatchException(null, "T", "batch failed", counts, List.of());

    assertEquals(9, written.size(), "SUCCESS_NO_INFO must not be read as a failure");
    assertEquals(1, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void zeroRowCountIsNotAFailure() throws HopException {
    // A statement that matched no rows is still a statement that ran.
    bufferRows(BATCH);
    int[] counts = {1, 0, 1, 0, 1, 1, 1, 1, 1, 1};

    transform.processBatchException(null, "T", "batch failed", counts, List.of());

    assertEquals(BATCH, written.size());
    assertEquals(0, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void perRowMessagesAreUsedWhenTheDriverChainsThem() throws HopException {
    bufferRows(BATCH);
    int[] counts = {1, EXECUTE_FAILED, 1, EXECUTE_FAILED, 1, 1, 1, 1, 1, 1};
    List<Exception> chained =
        List.of(
            new BatchUpdateException("first row problem", counts),
            new BatchUpdateException("second row problem", counts));

    transform.processBatchException(null, "T", "generic batch message", counts, chained);

    assertEquals(2, rejected.size());
    assertTrue(rejected.get(0).contains("first row problem"));
    assertTrue(rejected.get(1).contains("second row problem"));
  }

  // ---------------------------------------------------------------------------------------------
  // Re-driving the untried tail. A driver that stops at the first failure never sent the rows
  // behind
  // it, so they are still owed a write: rejecting them would leave Oracle writing fewer rows than
  // PostgreSQL or SQL Server for the same input.
  // ---------------------------------------------------------------------------------------------

  /** A prepared statement that fails the batch at a given offset, the way Oracle does. */
  private PreparedStatement statementFailingAt(int... failingOffsets) throws Exception {
    PreparedStatement ps = mock(PreparedStatement.class);
    List<Integer> remaining = new ArrayList<>();
    for (int f : failingOffsets) {
      remaining.add(f);
    }
    int[] submitted = {0};
    doAnswer(inv -> submitted[0]++).when(ps).addBatch();
    doAnswer(
            inv -> {
              int n = submitted[0];
              submitted[0] = 0;
              if (!remaining.isEmpty()) {
                int stopAt = remaining.remove(0);
                int[] prefix = new int[stopAt];
                Arrays.fill(prefix, 1);
                throw new BatchUpdateException("ORA-00001: unique constraint violated", prefix);
              }
              int[] all = new int[n];
              Arrays.fill(all, 1);
              return all;
            })
        .when(ps)
        .executeBatch();
    return ps;
  }

  @Test
  void keepsPrefix_theUntriedTailIsRewrittenNotRejected() throws Exception {
    // Ten rows, bad ones at 3 and 6. Oracle stops at 3; the retry of rows 4..9 stops at 6; the
    // retry
    // of rows 7..9 succeeds. Eight rows end up written, exactly as on a driver that marks failures
    // in place - which is the whole point.
    bufferRows(BATCH);
    for (int i = 0; i < BATCH; i++) {
      data.batchBindBuffer.add(new Object[] {"row" + i});
    }
    PreparedStatement ps = statementFailingAt(3, 2);

    transform.processBatchException(
        ps, "T", "ORA-00001: unique constraint violated", new int[] {1, 1, 1}, List.of());

    assertEquals(8, written.size(), "the six good rows behind the failures must be written");
    assertEquals(2, rejected.size(), "only the two rows the database actually refused are rejects");
    assertAllRowsAccountedFor();
  }

  @Test
  void keepsPrefix_retryStopsWhenTheDriverStopsReportingCounts() throws Exception {
    // If a retry comes back with no counts at all we can no longer tell what was applied, so the
    // remainder is reported rather than driven again - retrying blind could write a row twice.
    bufferRows(BATCH);
    for (int i = 0; i < BATCH; i++) {
      data.batchBindBuffer.add(new Object[] {"row" + i});
    }
    PreparedStatement ps = mock(PreparedStatement.class);
    doAnswer(inv -> null).when(ps).addBatch();
    doAnswer(
            inv -> {
              throw new SQLException("connection went away");
            })
        .when(ps)
        .executeBatch();

    transform.processBatchException(ps, "T", "ORA-00001", new int[] {1, 1, 1}, List.of());

    assertEquals(3, written.size());
    assertEquals(7, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void tailIsNotRetriedWhenTheBufferSpansSeveralTables() throws Exception {
    // With the table name in a field the buffer interleaves rows for different statements, so the
    // tail cannot be bound unambiguously. It is reported instead of retried.
    bufferRows(BATCH);
    for (int i = 0; i < BATCH; i++) {
      data.batchBindBuffer.add(new Object[] {"row" + i});
    }
    doReturn(true).when(meta).isTableNameInField();
    PreparedStatement ps = statementFailingAt();

    transform.processBatchException(ps, "T", "ORA-00001", new int[] {1, 1, 1}, List.of());

    assertEquals(3, written.size());
    assertEquals(7, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void moreCountsThanBufferedRowsFailsLoudly() {
    // The counts belong to a different batch than the one we buffered, so no row can be matched to
    // a
    // count. Reporting the wrong rows on the wrong stream would be worse than stopping here.
    bufferRows(3);

    HopException e =
        assertThrows(
            HopException.class,
            () ->
                transform.processBatchException(
                    null, "T", "mismatch", new int[] {1, 1, 1, 1, 1}, List.of()));
    assertTrue(e.getMessage().contains("Unable to attribute batch errors to rows"));
  }

  @Test
  void nullExceptionListIsTolerated() throws HopException {
    bufferRows(BATCH);
    int[] counts = {1, 1, 1, EXECUTE_FAILED, 1, 1, 1, 1, 1, 1};

    transform.processBatchException(null, "T", "batch failed", counts, null);

    assertEquals(9, written.size());
    assertEquals(1, rejected.size());
    assertAllRowsAccountedFor();
  }
}

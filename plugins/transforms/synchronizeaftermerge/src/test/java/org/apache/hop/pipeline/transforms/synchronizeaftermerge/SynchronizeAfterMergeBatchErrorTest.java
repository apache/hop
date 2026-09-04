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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
 * <p>This transform carries the same batch-error handling Table Output does, and carried the same
 * defect: the row-splitting loop was bounded by {@code updateCounts.length} and the buffer was
 * cleared unconditionally afterwards, so against a driver that stops at the first failure - Oracle
 * and Derby - every row from the failure onward reached neither stream. See <a
 * href="https://github.com/apache/hop/issues/5758">issue 5758</a>, reported against Table Output.
 *
 * <p>The invariant here is the one that was broken: <em>every buffered row leaves on exactly one
 * stream</em>, whatever shape the driver's update counts arrive in.
 *
 * <p>Unlike Table Output this transform does not re-drive the rows the database never attempted -
 * its three statements share one interleaved buffer, so the values to re-bind cannot be worked out.
 * The tests below pin that as the current behaviour rather than as the desirable one.
 */
class SynchronizeAfterMergeBatchErrorTest {

  private static final int BATCH = 10;

  private SynchronizeAfterMerge transform;
  private SynchronizeAfterMergeData data;

  private List<Object[]> emitted;
  private List<String> rejected;

  @BeforeEach
  void setUp() throws Exception {
    SynchronizeAfterMergeMeta meta = mock(SynchronizeAfterMergeMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);
    doReturn("transform").when(transformMeta).getName();
    doReturn(mock(TransformPartitioningMeta.class))
        .when(transformMeta)
        .getTargetTransformPartitioningMeta();
    doReturn(meta).when(transformMeta).getTransform();

    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    doReturn(transformMeta).when(pipelineMeta).findTransform(anyString());

    data = new SynchronizeAfterMergeData();
    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta(new ValueMetaString("name"));
    data.batchBuffer = new ArrayList<>();

    transform =
        spy(
            new SynchronizeAfterMerge(
                transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine())));
    doReturn(transformMeta).when(transform).getTransformMeta();
    doReturn(false).when(transform).isRowLevel();
    doNothing().when(transform).logDetailed(anyString());

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
      data.batchBuffer.add(new Object[] {"row" + i});
    }
  }

  /** Every buffered row has to come out of exactly one of the two streams. */
  private void assertAllRowsAccountedFor() {
    assertEquals(
        BATCH,
        emitted.size() + rejected.size(),
        "every buffered row must be emitted exactly once, on one stream or the other");
    assertTrue(data.batchBuffer.isEmpty(), "the buffer must be drained");
  }

  @Test
  void marksFailedRows_splitsOnTheCountsArray() throws HopException {
    // SQL Server, MySQL, H2: a full-length array with EXECUTE_FAILED at the failing rows.
    bufferRows(BATCH);
    int[] counts = {1, 1, 1, EXECUTE_FAILED, 1, 1, EXECUTE_FAILED, 1, 1, 1};

    transform.processBatchException("batch failed", counts, List.of());

    assertEquals(8, emitted.size());
    assertEquals(2, rejected.size());
    assertAllRowsAccountedFor();
  }

  /** The regression guard: a short counts array must not take the tail down with it. */
  @Test
  void keepsPrefix_shortCountsArrayDoesNotDropTheTail() throws HopException {
    // Oracle and Derby stop at the first failure, so a batch of ten that fails at index three comes
    // back with three counts. The old loop ended there and the buffer was cleared: seven rows gone.
    bufferRows(BATCH);

    transform.processBatchException(
        "ORA-00001: unique constraint violated", new int[] {1, 1, 1}, List.of());

    assertEquals(3, emitted.size(), "only the rows the driver confirmed may go downstream");
    assertEquals(
        7, rejected.size(), "the failing row and the six never attempted must be rejected");
    assertAllRowsAccountedFor();
  }

  @Test
  void keepsPrefix_failureOnTheFirstRowGivesAnEmptyArray() throws HopException {
    // A first-row failure yields a zero-length array, not a null one, so the old code took the
    // counts branch, ran its loop zero times, and discarded the entire commit window.
    bufferRows(BATCH);

    transform.processBatchException("ORA-12899: value too large", new int[0], List.of());

    assertEquals(0, emitted.size());
    assertEquals(BATCH, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void keepsPrefix_theFailingRowIsDistinguishedFromTheUntriedOnes() throws HopException {
    bufferRows(BATCH);

    transform.processBatchException(
        "ORA-00001: unique constraint violated", new int[] {1, 1, 1}, List.of());

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
    // PostgreSQL and DuckDB abort the transaction: a full-length array of EXECUTE_FAILED, and
    // nothing durable.
    bufferRows(BATCH);
    int[] counts = new int[BATCH];
    Arrays.fill(counts, EXECUTE_FAILED);

    transform.processBatchException("current transaction is aborted", counts, List.of());

    assertEquals(0, emitted.size());
    assertEquals(BATCH, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void noCountsAtAll_everyRowIsRejected() throws HopException {
    // A plain SQLException rather than a BatchUpdateException - SQLite, DuckDB, and SQL Server's
    // batch-aborting error classes.
    bufferRows(BATCH);

    transform.processBatchException("driver reported no update counts", null, List.of());

    assertEquals(0, emitted.size());
    assertEquals(BATCH, rejected.size());
    assertAllRowsAccountedFor();
  }

  @Test
  void successNoInfoCountsAsSuccess() throws HopException {
    // JDBC says success is anything that is not EXECUTE_FAILED. The old "> 0" test read
    // SUCCESS_NO_INFO as a failure and pushed rows the database had applied onto the error stream.
    bufferRows(BATCH);
    int[] counts = {
      SUCCESS_NO_INFO, SUCCESS_NO_INFO, SUCCESS_NO_INFO, EXECUTE_FAILED, SUCCESS_NO_INFO,
      SUCCESS_NO_INFO, SUCCESS_NO_INFO, SUCCESS_NO_INFO, SUCCESS_NO_INFO, SUCCESS_NO_INFO
    };

    transform.processBatchException("batch failed", counts, List.of());

    assertEquals(9, emitted.size(), "SUCCESS_NO_INFO must not be read as a failure");
    assertEquals(1, rejected.size());
    assertAllRowsAccountedFor();
  }

  /**
   * This transform issues updates and deletes, not just inserts, so a statement that matched no
   * rows is entirely ordinary - and the old {@code > 0} test reported every one of them as an
   * error.
   */
  @Test
  void zeroRowCountIsNotAFailure() throws HopException {
    bufferRows(BATCH);
    int[] counts = {1, 0, 1, 0, 0, 1, 1, 0, 1, 1};

    transform.processBatchException("batch failed", counts, List.of());

    assertEquals(
        BATCH, emitted.size(), "an update or delete that matched no rows still ran successfully");
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

    transform.processBatchException("generic batch message", counts, chained);

    assertEquals(2, rejected.size());
    assertTrue(rejected.get(0).contains("first row problem"));
    assertTrue(rejected.get(1).contains("second row problem"));
  }

  @Test
  void moreCountsThanBufferedRowsFailsLoudly() {
    // The counts belong to a different batch than the one buffered - which this transform can
    // produce on its own, because it runs a commit schedule alongside the one inside Database. No
    // row can be matched to a count, and reporting the wrong rows would be worse than stopping.
    bufferRows(3);

    HopException e =
        assertThrows(
            HopException.class,
            () ->
                transform.processBatchException("mismatch", new int[] {1, 1, 1, 1, 1}, List.of()));
    assertTrue(e.getMessage().contains("Unable to attribute batch errors to rows"));
  }

  @Test
  void nullExceptionListIsTolerated() throws HopException {
    bufferRows(BATCH);
    int[] counts = {1, 1, 1, EXECUTE_FAILED, 1, 1, 1, 1, 1, 1};

    transform.processBatchException("batch failed", counts, null);

    assertEquals(9, emitted.size());
    assertEquals(1, rejected.size());
    assertAllRowsAccountedFor();
  }
}

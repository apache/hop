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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Which stream each row of a failed batch goes to, for each shape of update counts a driver gives
 * back. Every row in the batch has to leave on exactly one stream.
 */
class UpdateBatchErrorTest {

  private static final int BATCH = 10;

  private Update update;
  private TransformMeta transformMeta;
  private UpdateData data;
  private PreparedStatement statement;
  private List<Long> output;
  private List<Long> errors;

  @BeforeEach
  void setUp() throws Exception {
    UpdateMeta meta = mock(UpdateMeta.class);
    transformMeta = mock(TransformMeta.class);
    doReturn("update").when(transformMeta).getName();
    doReturn(mock(TransformPartitioningMeta.class))
        .when(transformMeta)
        .getTargetTransformPartitioningMeta();
    doReturn(meta).when(transformMeta).getTransform();
    doReturn(true).when(transformMeta).isDoingErrorHandling();
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    doReturn(transformMeta).when(pipelineMeta).findTransform(anyString());

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));

    data = new UpdateData();
    data.db = mock(Database.class);
    data.batchMode = true;
    data.outputRowMeta = rowMeta;
    data.updateParameterRowMeta = rowMeta;
    statement = mock(PreparedStatement.class);
    data.prepStatementUpdate = statement;
    for (long id = 0; id < BATCH; id++) {
      Object[] row = {id};
      data.batchBuffer.add(new UpdateData.BatchedRow(row, row, row));
    }

    update = spy(new Update(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine()));
    doReturn(transformMeta).when(update).getTransformMeta();
    doReturn(rowMeta).when(update).getInputRowMeta();

    output = new ArrayList<>();
    errors = new ArrayList<>();
    doAnswer(
            inv -> {
              output.add((Long) ((Object[]) inv.getArgument(1))[0]);
              return null;
            })
        .when(update)
        .putRow(any(IRowMeta.class), any());
    doAnswer(
            inv -> {
              errors.add((Long) ((Object[]) inv.getArgument(1))[0]);
              return null;
            })
        .when(update)
        .putError(
            any(IRowMeta.class),
            any(),
            anyLong(),
            nullable(String.class),
            nullable(String.class),
            anyString());
  }

  private static int[] counts(int length, int... failed) {
    int[] counts = new int[length];
    Arrays.fill(counts, 1);
    for (int i : failed) {
      counts[i] = EXECUTE_FAILED;
    }
    return counts;
  }

  private void assertEveryRowLeftOnce() {
    List<Long> all = new ArrayList<>(output);
    all.addAll(errors);
    all.sort(Long::compare);
    assertEquals(
        LongStream.range(0, BATCH).boxed().toList(),
        all,
        "every row of the batch must leave on exactly one stream");
    assertTrue(data.batchBuffer.isEmpty(), "the batch must be emptied");
  }

  /** SQL Server, MySQL, H2: one count per row. */
  @Test
  void aCountPerRowMarksTheFailedRows() throws HopException {
    update.routeFailedBatch("failed", counts(BATCH, 3, 7), null);

    assertEquals(List.of(3L, 7L), errors);
    assertEveryRowLeftOnce();
    assertEquals(BATCH - 2, update.getLinesUpdated());
  }

  /** Oracle, Derby: stopped at row 3; rows 4..9 were never sent and are sent again. */
  @Test
  void aShortCountsArraySendsTheRowsAfterTheFailureAgain() throws Exception {
    when(statement.executeBatch()).thenReturn(counts(BATCH - 4));

    update.routeFailedBatch("failed", counts(3), null);

    assertEquals(List.of(3L), errors);
    assertEveryRowLeftOnce();
    verify(statement, times(BATCH - 4)).addBatch();
  }

  /** The retry fails again, at its third row (row 6 of the batch). */
  @Test
  void aRetryThatFailsIsSortedOutTheSameWay() throws Exception {
    when(statement.executeBatch())
        .thenThrow(new BatchUpdateException("again", counts(2)))
        .thenReturn(counts(3));

    update.routeFailedBatch("failed", counts(3), null);

    assertEquals(List.of(3L, 6L), errors);
    assertEveryRowLeftOnce();
  }

  /** A failure on the first row gives a zero-length array, not null. */
  @Test
  void aFailureOnTheFirstRow() throws Exception {
    when(statement.executeBatch()).thenReturn(counts(BATCH - 1));

    update.routeFailedBatch("failed", new int[0], null);

    assertEquals(List.of(0L), errors);
    assertEveryRowLeftOnce();
  }

  /** Nothing says which rows were applied: sending them again could update a row twice. */
  @Test
  void noCountsSendsEveryRowToTheErrorHop() throws Exception {
    update.routeFailedBatch("failed", null, null);

    assertEquals(BATCH, errors.size());
    assertEveryRowLeftOnce();
    verify(statement, never()).executeBatch();
  }

  @Test
  void successNoInfoAndZeroRowsAreNotFailures() throws HopException {
    int[] counts = counts(BATCH, 5);
    counts[1] = SUCCESS_NO_INFO;
    counts[2] = 0;

    update.routeFailedBatch("failed", counts, null);

    assertEquals(List.of(5L), errors);
    assertEveryRowLeftOnce();
  }

  @Test
  void moreCountsThanRowsFails() {
    assertThrows(
        HopException.class, () -> update.routeFailedBatch("failed", counts(BATCH + 1), null));
  }

  /** Without an error hop a failed batch rolls back and stops the transform. */
  @Test
  void withoutErrorHopTheBatchIsRolledBack() throws Exception {
    doReturn(false).when(transformMeta).isDoingErrorHandling();
    doThrow(new BatchUpdateException("failed", counts(BATCH, 3))).when(statement).executeBatch();

    assertThrows(HopException.class, () -> update.flushBatch());

    verify(data.db).rollback();
    assertTrue(output.isEmpty());
    assertTrue(errors.isEmpty());
    assertTrue(data.batchBuffer.isEmpty());
  }

  @Test
  void aBatchThatRunsPassesEveryRowOn() throws Exception {
    when(statement.executeBatch()).thenReturn(counts(BATCH));

    update.flushBatch();

    assertEquals(BATCH, output.size());
    assertTrue(errors.isEmpty());
    assertEveryRowLeftOnce();
    verify(data.db).commit();
  }
}

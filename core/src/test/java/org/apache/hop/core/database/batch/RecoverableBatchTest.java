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

package org.apache.hop.core.database.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.junit.batch.FakeBatchDatabase;
import org.apache.hop.junit.batch.FakeBatchDatabase.Behaviour;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Drives {@link RecoverableBatch} against a fake driver for every measured failure behaviour, and
 * checks the one thing that matters: what the batch reports as written is exactly what the database
 * holds, and every row is reported once.
 */
class RecoverableBatchTest {

  private static final IBatchRowBinder BINDER =
      (PreparedStatement statement, Object[] row) -> {
        try {
          statement.setObject(1, row[0]);
        } catch (SQLException e) {
          throw new HopDatabaseException(e);
        }
      };

  /** Records the verdicts in order. */
  static class Recorder implements IBatchRowListener {
    final List<Object> written = new ArrayList<>();
    final List<Object> rejected = new ArrayList<>();
    final List<Object> order = new ArrayList<>();
    final List<String> messages = new ArrayList<>();

    /** The update count of every written row, in the order of {@link #written}. */
    final List<Integer> counts = new ArrayList<>();

    int notAttempted;

    @Override
    public void rowWritten(Object[] outputRow, int updateCount) {
      counts.add(updateCount);
      rowWritten(outputRow);
    }

    @Override
    public void rowWritten(Object[] outputRow) {
      written.add(outputRow[0]);
      order.add(outputRow[0]);
    }

    @Override
    public void rowRejected(Object[] outputRow, String message, boolean attempted) {
      rejected.add(outputRow[0]);
      order.add(outputRow[0]);
      messages.add(message);
      if (!attempted) {
        notAttempted++;
      }
    }
  }

  /** Every driver behaviour paired with every contract that may be declared for it. */
  static Stream<Arguments> behaviourAndContract() {
    return Stream.of(
        Arguments.of(Behaviour.MARKS_FAILED_ROWS, BatchFailureContract.MARKS_FAILED_ROWS),
        Arguments.of(Behaviour.MARKS_FAILED_ROWS, BatchFailureContract.UNKNOWN),
        Arguments.of(Behaviour.KEEPS_PREFIX, BatchFailureContract.KEEPS_PREFIX),
        Arguments.of(Behaviour.KEEPS_PREFIX, BatchFailureContract.UNKNOWN),
        Arguments.of(Behaviour.LOSES_WHOLE_BATCH, BatchFailureContract.LOSES_WHOLE_BATCH),
        Arguments.of(Behaviour.LOSES_WHOLE_BATCH, BatchFailureContract.UNKNOWN),
        Arguments.of(Behaviour.PARTIAL_WITHOUT_COUNTS, BatchFailureContract.UNKNOWN),
        // A wrong declaration must not lose rows either: the batch falls back to rolling back.
        Arguments.of(Behaviour.PARTIAL_WITHOUT_COUNTS, BatchFailureContract.MARKS_FAILED_ROWS),
        Arguments.of(Behaviour.LOSES_WHOLE_BATCH, BatchFailureContract.MARKS_FAILED_ROWS));
  }

  /** Bad rows at the start, in the middle, at the end, next to each other, and all of them. */
  static Stream<Arguments> scenarios() {
    List<int[]> badPositions =
        List.of(
            new int[] {},
            new int[] {0},
            new int[] {9},
            new int[] {3, 6},
            new int[] {4, 5},
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    return behaviourAndContract()
        .flatMap(
            pair ->
                badPositions.stream().map(bad -> Arguments.of(pair.get()[0], pair.get()[1], bad)));
  }

  @ParameterizedTest(name = "{0} declared as {1}, bad rows {2}")
  @MethodSource("scenarios")
  void everyRowGetsTheVerdictTheDatabaseAgreesWith(
      Behaviour behaviour, BatchFailureContract contract, int[] badPositions) throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(behaviour);
    Set<Object> bad = new LinkedHashSet<>();
    for (int position : badPositions) {
      bad.add(100 + position);
    }
    db.seed(bad.toArray());
    // Counts other than 1, as an UPDATE that matches no row (0) or several rows (3) returns.
    db.updateCount(102, 0).updateCount(107, 3);

    RecoverableBatch batch =
        new RecoverableBatch(db.connection(), prepare(db), BINDER, contract, true);
    List<Object> keys = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      int key = 100 + i;
      keys.add(key);
      batch.add(new Object[] {key}, new Object[] {key});
    }

    Recorder recorder = new Recorder();
    batch.flush(recorder);

    Set<Object> inserted = new LinkedHashSet<>(db.committed());
    inserted.removeAll(bad);
    assertEquals(
        new LinkedHashSet<>(recorder.written),
        inserted,
        "rows reported as written must be exactly the rows committed");
    assertEquals(new LinkedHashSet<>(bad), new LinkedHashSet<>(recorder.rejected));
    assertEquals(keys, recorder.order, "every row reported once, in input order");
    assertEquals(0, recorder.notAttempted);
    assertEquals(0, batch.size());
    assertEquals(
        expectedCounts(db, recorder.written), recorder.counts, "each row with its own count");
  }

  static List<Integer> expectedCounts(FakeBatchDatabase db, List<Object> written) {
    List<Integer> expected = new ArrayList<>();
    for (Object key : written) {
      expected.add(db.countOf(key));
    }
    return expected;
  }

  /**
   * The transaction can hold other work when a batch is flushed, such as the truncate of the table
   * before the first batch. Recovering a failure must never undo it.
   */
  @ParameterizedTest(name = "{0} declared as {1}")
  @MethodSource("behaviourAndContract")
  void recoveringKeepsTheWorkTheTransactionHeldBefore(
      Behaviour behaviour, BatchFailureContract contract) throws Exception {
    assertRecoveryKeepsEarlierWork(new FakeBatchDatabase(behaviour), contract);
  }

  /** DuckDB has no savepoints: the earlier work is committed before the batch runs. */
  @ParameterizedTest(name = "{0} declared as {1}")
  @MethodSource("behaviourAndContract")
  void recoveringKeepsTheWorkTheTransactionHeldBeforeWithoutSavepoints(
      Behaviour behaviour, BatchFailureContract contract) throws Exception {
    assertRecoveryKeepsEarlierWork(new FakeBatchDatabase(behaviour).withoutSavepoints(), contract);
  }

  private static void assertRecoveryKeepsEarlierWork(
      FakeBatchDatabase db, BatchFailureContract contract) throws Exception {
    db.writeUncommitted("truncate").seed(105);
    RecoverableBatch batch =
        new RecoverableBatch(db.connection(), prepare(db), BINDER, contract, true);
    for (int i = 0; i < 10; i++) {
      batch.add(new Object[] {100 + i}, new Object[] {100 + i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(105), recorder.rejected);
    assertEquals(List.of(100, 101, 102, 103, 104, 106, 107, 108, 109), recorder.written);
    assertTrue(db.committed().contains("truncate"), "earlier work undone: " + db.committed());
  }

  /** Without an error hop a failed load is undone as a whole, earlier work included. */
  @Test
  void failingWithoutRecoveryUndoesTheWorkTheTransactionHeldBefore() throws Exception {
    FakeBatchDatabase db =
        new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).writeUncommitted("truncate").seed(105);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true);
    for (int i = 0; i < 10; i++) {
      batch.add(new Object[] {100 + i}, new Object[] {100 + i});
    }
    assertThrows(HopDatabaseBatchException.class, () -> batch.flushOrFail(new Recorder()));
    db.connection().commit();

    assertEquals(Set.of(105), db.committed());
  }

  @Test
  void aCleanBatchIsOneExecutionAndOneCommit() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true);
    for (int i = 0; i < 1000; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(1000, recorder.written.size());
    assertEquals(1, db.getExecutions());
    assertEquals(1, db.getCommits());
    assertEquals(0, db.getRollbacks());
  }

  @Test
  void bisectionNeedsFarFewerRoundTripsThanRowByRow() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn(517);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true);
    for (int i = 0; i < 1000; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(517), recorder.rejected);
    assertEquals(999, recorder.written.size());
    // One failed batch, then two halves per level down to a single row: about 2 * log2(1000).
    assertTrue(db.getExecutions() <= 22, "executions: " + db.getExecutions());
  }

  @Test
  void aDriverThatNamesTheFailingEntryNeedsAFewRoundTripsPerBadRow() throws Exception {
    FakeBatchDatabase db =
        new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn(10, 400, 401, 999);
    RecoverableBatch batch =
        new RecoverableBatch(
                db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true)
            .withFailedEntryLocator(RecoverableBatchTest::pgjdbcEntry);
    for (int i = 0; i < 1000; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(10, 400, 401, 999), recorder.rejected);
    assertEquals(996, recorder.written.size());
    assertEquals(new LinkedHashSet<>(recorder.written), db.committed());
    // The failed batch, then per bad row: the next attempt at all the other rows (before and after
    // it) as one batch, and the row on its own once those are committed. 401 is blamed at the same
    // position as 400 before it, so it is checked on its own once more before trusting the hint.
    assertEquals(1 + 4 * 2 + 1, db.getExecutions());
    assertEquals(4, batch.getRejectedRows());
  }

  @Test
  void aLocatedBadRowIsReportedInOrderBetweenTheRowsAroundIt() throws Exception {
    FakeBatchDatabase db =
        new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn(5).updateCount(3, 0);
    RecoverableBatch batch =
        new RecoverableBatch(
                db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true)
            .withFailedEntryLocator(RecoverableBatchTest::pgjdbcEntry);
    for (int i = 0; i < 10; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), recorder.order);
    assertEquals(List.of(5), recorder.rejected);
    assertEquals(List.of(1, 1, 1, 0, 1, 1, 1, 1, 1), recorder.counts);
    assertEquals(Set.of(0, 1, 2, 3, 4, 6, 7, 8, 9), db.committed());
    // The failed batch, the nine others as one batch, then row 5 on its own.
    assertEquals(3, db.getExecutions());
    assertEquals(1, db.getExecuteUpdateCalls());
  }

  /**
   * Two rows with the same key in one batch: the second only fails after the first, so on its own
   * it succeeds. It must still be the second one that is rejected, as in the original batch.
   */
  @Test
  void aBlamedRowThatOnlyFailsAfterTheRowsBeforeItIsJudgedInItsPlace() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH);
    RecoverableBatch batch =
        new RecoverableBatch(
                db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true)
            .withFailedEntryLocator(RecoverableBatchTest::pgjdbcEntry);
    for (int i = 0; i < 10; i++) {
      // Row 6 repeats the key of row 3.
      batch.add(new Object[] {i == 6 ? 3 : i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), recorder.order);
    assertEquals(List.of(6), recorder.rejected);
    assertEquals(Set.of(0, 1, 2, 3, 4, 5, 7, 8, 9), db.committed());
    // The failed batch, the nine others as one batch, then row 6 on its own after them (refused).
    assertEquals(3, db.getExecutions());
  }

  /**
   * A wrong hint names a child row whose parent comes earlier in the same batch, while the real bad
   * row is elsewhere. On its own, before its parent, the child would fail; it must still be
   * written, and only the real bad row rejected.
   */
  @Test
  void aWrongHintNeverRejectsARowThatNeedsAnEarlierRowOfTheBatch() throws Exception {
    FakeBatchDatabase db =
        new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).requires(5, 2).failOn(8);
    AtomicInteger calls = new AtomicInteger();
    RecoverableBatch batch =
        new RecoverableBatch(
                db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true)
            // The first hint names the child (entry 5) instead of the bad row (entry 8).
            .withFailedEntryLocator(
                failure -> calls.getAndIncrement() == 0 ? 5 : pgjdbcEntry(failure));
    for (int i = 0; i < 10; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), recorder.order);
    assertEquals(List.of(8), recorder.rejected);
    assertEquals(Set.of(0, 1, 2, 3, 4, 6, 7, 9, 5), db.committed());
    // The failed batch; the others without 5, failing on 8; the others without 5 and 8, committed;
    // then 5 (now with its parent) and 8 on their own.
    assertEquals(5, db.getExecutions());
  }

  @Test
  void aCombinedRetryThatFailsAgainIsLocatedAgain() throws Exception {
    // Another session inserts key 8 while the batch recovers from key 2: the retry of the other
    // rows fails on 8, which is then located, verified and rejected the same way.
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn(2);
    RecoverableBatch batch =
        new RecoverableBatch(
                db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true)
            .withFailedEntryLocator(
                failure -> {
                  db.seed(8);
                  return pgjdbcEntry(failure);
                });
    for (int i = 0; i < 10; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), recorder.order);
    assertEquals(List.of(2, 8), recorder.rejected);
    assertEquals(Set.of(8, 0, 1, 3, 4, 5, 6, 7, 9), db.committed());
    assertEquals(1 + 2 * 2, db.getExecutions());
  }

  @Test
  void countsOfABatchWithoutInfoAndOfSingleRowsArriveInOrder() throws Exception {
    // A driver that answers SUCCESS_NO_INFO for batched rows still counts a row run on its own.
    FakeBatchDatabase db =
        new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH)
            .failOn(5)
            .updateCount(6, 0)
            .batchCountsWithoutInfo();
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true);
    for (int i = 0; i < 10; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    // Bisection: [0,5) as a batch, [5,10) fails, [5,7) fails, 5 and 6 alone, [7,10) as a batch.
    int noInfo = Statement.SUCCESS_NO_INFO;
    assertEquals(List.of(0, 1, 2, 3, 4, 6, 7, 8, 9), recorder.written);
    assertEquals(
        List.of(noInfo, noInfo, noInfo, noInfo, noInfo, 0, noInfo, noInfo, noInfo),
        recorder.counts);
  }

  @ParameterizedTest
  @EnumSource(
      value = Behaviour.class,
      names = {"MARKS_FAILED_ROWS", "KEEPS_PREFIX"})
  void countsComeFromTheFailedBatchAndTheResentTail(Behaviour behaviour) throws Exception {
    BatchFailureContract contract = BatchFailureContract.valueOf(behaviour.name());
    FakeBatchDatabase db =
        new FakeBatchDatabase(behaviour).failOn(4).updateCount(1, 0).updateCount(7, 2);
    RecoverableBatch batch =
        new RecoverableBatch(db.connection(), prepare(db), BINDER, contract, true);
    for (int i = 0; i < 10; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), recorder.order);
    assertEquals(List.of(1, 0, 1, 1, 1, 1, 2, 1, 1), recorder.counts);
  }

  @Test
  void aCleanBatchReportsTheCountOfEachRow() throws Exception {
    FakeBatchDatabase db =
        new FakeBatchDatabase(Behaviour.MARKS_FAILED_ROWS).updateCount(1, 0).updateCount(2, 5);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.MARKS_FAILED_ROWS, true);
    for (int i = 0; i < 4; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(1, 0, 5, 1), recorder.counts);
  }

  @Test
  void aListenerWithoutCountsStillGetsEveryRow() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn(3);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true);
    for (int i = 0; i < 6; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    List<Object> written = new ArrayList<>();
    batch.flush(
        new IBatchRowListener() {
          @Override
          public void rowWritten(Object[] outputRow) {
            written.add(outputRow[0]);
          }

          @Override
          public void rowRejected(Object[] outputRow, String message, boolean attempted) {
            // not needed here
          }
        });
    assertEquals(List.of(0, 1, 2, 4, 5), written);
  }

  @Test
  void aWrongHintFallsBackWithoutRejectingAGoodRow() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn(70);
    RecoverableBatch batch =
        new RecoverableBatch(
                db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true)
            .withFailedEntryLocator(failure -> 20);
    for (int i = 0; i < 100; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(70), recorder.rejected);
    assertEquals(99, recorder.written.size());
    assertEquals(new LinkedHashSet<>(recorder.written), db.committed());
  }

  @Test
  void aHintThatCountsStatementsInsteadOfRowsIsDroppedAfterOneMiss() throws Exception {
    // pgjdbc with reWriteBatchedInserts=true blames "entry 0" for any bad row in its first
    // rewritten statement. Following that hint row by row would cost hundreds of executions.
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn(300);
    RecoverableBatch batch =
        new RecoverableBatch(
                db.connection(), prepare(db), BINDER, BatchFailureContract.LOSES_WHOLE_BATCH, true)
            .withFailedEntryLocator(failure -> 0);
    for (int i = 0; i < 1000; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(300), recorder.rejected);
    assertEquals(999, recorder.written.size());
    assertEquals(new LinkedHashSet<>(recorder.written), db.committed());
    assertTrue(db.getExecutions() <= 25, "executions: " + db.getExecutions());
  }

  private static int pgjdbcEntry(SQLException failure) {
    java.util.regex.Matcher matcher =
        java.util.regex.Pattern.compile("Batch entry (\\d+) ").matcher(failure.getMessage());
    return matcher.find() ? Integer.parseInt(matcher.group(1)) : -1;
  }

  @Test
  void keepsPrefixResendsTheTailOncePerBadRow() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.KEEPS_PREFIX).failOn(10, 500);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.KEEPS_PREFIX, true);
    for (int i = 0; i < 1000; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of(10, 500), recorder.rejected);
    assertEquals(998, recorder.written.size());
    assertEquals(3, db.getExecutions());
  }

  @Test
  void theRejectedRowCarriesItsOwnMessage() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn(3);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.UNKNOWN, true);
    for (int i = 0; i < 8; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(1, recorder.messages.size());
    assertTrue(recorder.messages.get(0).contains("key 3"), recorder.messages.get(0));
  }

  @Test
  void withoutTransactionsUnaccountedRowsAreReportedAsNotAttempted() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.PARTIAL_WITHOUT_COUNTS, false).seed(3);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.UNKNOWN, false);
    for (int i = 0; i < 6; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    // Nothing can be undone, so nothing is retried blindly: no row is written twice, and no row is
    // claimed as written that the driver didn't confirm.
    assertEquals(List.of(), recorder.written);
    assertEquals(6, recorder.rejected.size());
    assertEquals(6, recorder.notAttempted);
  }

  @Test
  void withoutAListenerAFailureRollsBackAndThrows() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.MARKS_FAILED_ROWS).seed(2);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.MARKS_FAILED_ROWS, true);
    for (int i = 0; i < 5; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }

    HopDatabaseBatchException e =
        assertThrows(HopDatabaseBatchException.class, () -> batch.flush(null));
    assertEquals(5, e.getUpdateCounts().length);
    assertEquals(Set.of(2), db.committed());
    assertEquals(0, batch.size());
  }

  @Test
  void flushOrFailReportsWrittenRowsAndThrowsOnFailure() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.MARKS_FAILED_ROWS).seed(7);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.MARKS_FAILED_ROWS, true);
    Recorder recorder = new Recorder();
    for (int i = 0; i < 5; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    batch.flushOrFail(recorder);
    assertEquals(List.of(0, 1, 2, 3, 4), recorder.written);

    for (int i = 5; i < 10; i++) {
      batch.add(new Object[] {i}, new Object[] {i});
    }
    assertThrows(HopDatabaseBatchException.class, () -> batch.flushOrFail(recorder));
    assertEquals(
        List.of(0, 1, 2, 3, 4), recorder.written, "nothing of the failed batch is written");
    assertEquals(List.of(), recorder.rejected);
    assertEquals(Set.of(7, 0, 1, 2, 3, 4), db.committed());
  }

  @Test
  void discardDropsUnflushedRowsWithoutRunningThem() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.MARKS_FAILED_ROWS);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.MARKS_FAILED_ROWS, true);
    batch.add(new Object[] {1}, new Object[] {"one"});
    batch.add(new Object[] {2}, new Object[] {"two"});

    List<Object[]> dropped = batch.discard();

    assertEquals(2, dropped.size());
    assertEquals("one", dropped.get(0)[0]);
    assertEquals(0, db.getExecutions());
    batch.flush(new Recorder());
    assertEquals(0, db.getExecutions());
  }

  @Test
  void outputRowsAreReportedNotBindRows() throws Exception {
    FakeBatchDatabase db = new FakeBatchDatabase(Behaviour.KEEPS_PREFIX).seed(1);
    RecoverableBatch batch =
        new RecoverableBatch(
            db.connection(), prepare(db), BINDER, BatchFailureContract.KEEPS_PREFIX, true);
    batch.add(new Object[] {0}, new Object[] {"zero"});
    batch.add(new Object[] {1}, new Object[] {"one"});
    batch.add(new Object[] {2}, new Object[] {"two"});
    Recorder recorder = new Recorder();
    batch.flush(recorder);

    assertEquals(List.of("zero", "two"), recorder.written);
    assertEquals(List.of("one"), recorder.rejected);
    assertEquals(Set.of(1, 0, 2), db.committed());
  }

  private static PreparedStatement prepare(FakeBatchDatabase db) throws SQLException {
    return db.connection().prepareStatement("INSERT INTO T VALUES (?)");
  }
}

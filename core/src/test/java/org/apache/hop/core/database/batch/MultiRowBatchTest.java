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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.exception.HopDatabaseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Chunking, remainders, counts and failure recovery of {@link MultiRowBatch}, against a fake driver
 * whose statements apply several rows atomically.
 */
class MultiRowBatchTest {

  /** Every verdict, in the order it was given: "w:key=count" or "r:key". */
  private static final class Verdicts implements IBatchRowListener {
    final List<String> log = new ArrayList<>();
    final Map<Object, Integer> counts = new LinkedHashMap<>();

    @Override
    public void rowWritten(Object[] outputRow) {
      throw new AssertionError("the two-argument method is called");
    }

    @Override
    public void rowWritten(Object[] outputRow, int updateCount) {
      log.add("w:" + outputRow[0]);
      counts.put(outputRow[0], updateCount);
    }

    @Override
    public void rowRejected(Object[] outputRow, String message, boolean attempted) {
      log.add("r:" + outputRow[0]);
    }

    @Override
    public void rowNotBound(Object[] outputRow, HopDatabaseException error) {
      log.add("u:" + outputRow[0]);
    }
  }

  private static MultiRowBatch batch(
      FakeMultiRowDatabase fake, BatchFailureContract contract, int rows) {
    return new MultiRowBatch(fake.connection(), fake.statements(rows), contract, true)
        .withFailedEntryLocator(MultiRowBatchTest::batchEntry);
  }

  private static final Pattern BATCH_ENTRY = Pattern.compile("Batch entry (\\d+)");

  private static int batchEntry(java.sql.SQLException failure) {
    Matcher matcher = BATCH_ENTRY.matcher(String.valueOf(failure.getMessage()));
    return matcher.find() ? Integer.parseInt(matcher.group(1)) : -1;
  }

  private static void add(MultiRowBatch batch, Object key, Object value) {
    batch.add(new Object[] {key, value}, new Object[] {key + "=" + value});
  }

  private static void addKeys(MultiRowBatch batch, int from, int to) {
    for (int i = from; i <= to; i++) {
      add(batch, i, "v" + i);
    }
  }

  private static List<String> written(int from, int to, int... rejected) {
    List<String> log = new ArrayList<>();
    for (int i = from; i <= to; i++) {
      boolean bad = false;
      for (int r : rejected) {
        bad |= r == i;
      }
      log.add((bad ? "r:" : "w:") + i + "=v" + i);
    }
    return log;
  }

  @Test
  void cleanChunksAreOneBatchPlusTheRemainder() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true);
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 5);
    addKeys(batch, 1, 12);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);

    assertEquals(written(1, 12), verdicts.log);
    // Two statements of five in one batch, the remainder of two on its own.
    assertEquals(List.of(5, 5, 2), fake.statementSizes);
    assertEquals(1, fake.executeBatchCalls);
    assertEquals(1, fake.executeUpdateCalls);
    assertEquals(1, fake.commits);
    assertEquals(Set.of(5, 2), fake.preparedSizeSet());
    assertEquals(12, fake.committed().size());
    // A row of a multi-row INSERT counts one.
    assertTrue(verdicts.counts.values().stream().allMatch(c -> c == 1), verdicts.counts.toString());
    assertEquals(3, batch.getStatements());
    assertEquals(2, batch.getExecutions());
    assertEquals(0, batch.getFallbackRows());
    assertTrue(batch.isEmpty());
  }

  @Test
  void remaindersArePowersOfTwoLargestFirst() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true);
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 8);
    addKeys(batch, 1, 29);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);

    // 29 = 8 + 8 + 8 + 4 + 1
    assertEquals(List.of(8, 8, 8, 4, 1), fake.statementSizes);
    assertEquals(Set.of(8, 4, 1), fake.preparedSizeSet());
    assertEquals(written(1, 29), verdicts.log);

    // The next flush reuses the prepared statements.
    addKeys(batch, 30, 35);
    batch.flush(verdicts);
    assertEquals(Set.of(8, 4, 2, 1), batch.getPreparedSizes());
    assertEquals(List.of(8, 4, 1, 2), fake.preparedSizes);
    assertEquals(written(1, 35), verdicts.log);
  }

  @Test
  void aDriverThatFoldsTheStatementsOfABatchStillCountsEveryRowOnce() throws Exception {
    // Three statements of two rows go out as one JDBC batch. A driver that folds them (pgjdbc's
    // reWriteBatchedInserts) reports one count for the three, which can't be pinned on a statement;
    // a row of an INSERT of several rows inserted exactly one row all the same.
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true).foldBatchCounts();
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 2);
    addKeys(batch, 1, 6);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);

    assertEquals(written(1, 6), verdicts.log);
    assertEquals(List.of(2, 2, 2), fake.statementSizes);
    assertEquals(1, fake.executeBatchCalls);
    assertTrue(verdicts.counts.values().stream().allMatch(c -> c == 1), verdicts.counts.toString());
    assertEquals(6, fake.committed().size());
  }

  @Test
  void aDuplicateKeyInAStatementIsTheDatabasesToRefuse() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true);
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 4);
    add(batch, 1, "a");
    add(batch, 2, "b");
    add(batch, 1, "c");
    add(batch, 3, "d");
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);

    // One chunk, refused for the duplicate, then row by row: the second 1 is the one refused.
    assertEquals(List.of("w:1=a", "w:2=b", "r:1=c", "w:3=d"), verdicts.log);
    assertEquals(Map.of(1, "a", 2, "b", 3, "d"), fake.committed());
    assertEquals(4, batch.getFallbackRows());
    assertEquals(1, batch.getRejectedRows());
  }

  @ParameterizedTest
  @EnumSource(
      value = BatchFailureContract.class,
      names = {"MARKS_FAILED_ROWS", "KEEPS_PREFIX", "LOSES_WHOLE_BATCH", "UNKNOWN"})
  void aFailingChunkIsRecoveredRowByRow(BatchFailureContract contract) throws Exception {
    FakeMultiRowDatabase fake = new FakeMultiRowDatabase(contract, true).failOn(7, 18, 23);
    MultiRowBatch batch = batch(fake, contract, 5);
    addKeys(batch, 1, 23);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);

    assertEquals(written(1, 23, 7, 18, 23), verdicts.log);
    assertEquals(20, fake.committed().size());
    for (int bad : new int[] {7, 18, 23}) {
      assertTrue(!fake.committed().containsKey(bad));
    }
    assertEquals(3, batch.getRejectedRows());
    assertEquals(1, batch.getRecoveredFlushes());
    if (contract != BatchFailureContract.UNKNOWN) {
      // Only the failing statements run row by row: 5 + 5 rows of full chunks, and the last row,
      // which is a statement of one already.
      assertEquals(11, batch.getFallbackRows(), contract.name());
    }
    // A row of a multi-row INSERT counts one.
    assertEquals(1, verdicts.counts.get("1=v1"));
  }

  @ParameterizedTest
  @EnumSource(
      value = BatchFailureContract.class,
      names = {"MARKS_FAILED_ROWS", "KEEPS_PREFIX", "LOSES_WHOLE_BATCH", "UNKNOWN"})
  void earlierWorkInTheTransactionSurvivesARecovery(BatchFailureContract contract)
      throws Exception {
    FakeMultiRowDatabase fake = new FakeMultiRowDatabase(contract, true).failOn(6);
    // Written in the same transaction before the batch, as the truncate of the table would be.
    PreparedStatement earlier = fake.prepare(1);
    earlier.setObject(1, 100);
    earlier.setObject(2, "earlier");
    earlier.executeUpdate();
    MultiRowBatch batch = batch(fake, contract, 4);
    addKeys(batch, 1, 8);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);

    assertEquals(written(1, 8, 6), verdicts.log);
    assertEquals("earlier", fake.committed().get(100), contract.name());
    assertEquals(8, fake.committed().size(), contract.name());
  }

  @Test
  void chunkSizesAcrossCommitWindows() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.KEEPS_PREFIX, true).failOn(14);
    MultiRowBatch batch = batch(fake, BatchFailureContract.KEEPS_PREFIX, 4);
    Verdicts verdicts = new Verdicts();
    // Window 1: [1..4], [5..8], and [9 10 11] as 2 + 1.
    addKeys(batch, 1, 11);
    batch.flush(verdicts);
    assertEquals(written(1, 11), verdicts.log);
    assertEquals(List.of(4, 4, 2, 1), fake.statementSizes);
    // Window 2, with a failure in its first statement: [12..15] runs row by row.
    addKeys(batch, 12, 20);
    batch.flush(verdicts);
    assertEquals(written(1, 20, 14), verdicts.log);
    assertEquals(19, fake.committed().size());
  }

  @Test
  void flushOrFailRollsBackAndReportsNothing() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true).failOn(8);
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 5);
    addKeys(batch, 1, 10);
    Verdicts verdicts = new Verdicts();
    assertThrows(HopDatabaseException.class, () -> batch.flushOrFail(verdicts));
    assertTrue(verdicts.log.isEmpty());
    assertTrue(fake.committed().isEmpty());
    assertTrue(batch.isEmpty());

    // The batch carries on afterwards.
    addKeys(batch, 20, 21);
    batch.flushOrFail(verdicts);
    assertEquals(List.of("w:20=v20", "w:21=v21"), verdicts.log);
  }

  @Test
  void withoutAListenerAFailureIsThrown() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.KEEPS_PREFIX, true).failOn(2);
    MultiRowBatch batch = batch(fake, BatchFailureContract.KEEPS_PREFIX, 5);
    addKeys(batch, 1, 3);
    assertThrows(HopDatabaseException.class, () -> batch.flush(null));
    assertTrue(fake.committed().isEmpty());
  }

  @Test
  void aValueThatCantBeBoundIsRejectedOnItsOwn() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true);
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 4);
    addKeys(batch, 1, 5);
    add(batch, 6, "unbindable");
    addKeys(batch, 7, 9);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);
    assertEquals(
        List.of(
            "w:1=v1",
            "w:2=v2",
            "w:3=v3",
            "w:4=v4",
            "w:5=v5",
            "u:6=unbindable",
            "w:7=v7",
            "w:8=v8",
            "w:9=v9"),
        verdicts.log);
    assertEquals(8, fake.committed().size());
    assertEquals(1, batch.getRejectedRows());
  }

  @Test
  void withoutRecoveryAValueThatCantBeBoundNamesItsRow() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true);
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 4);
    addKeys(batch, 1, 5);
    add(batch, 6, "unbindable");
    Verdicts verdicts = new Verdicts();

    MultiRowBatch.UnboundRowException e =
        assertThrows(MultiRowBatch.UnboundRowException.class, () -> batch.flushOrFail(verdicts));
    assertEquals("6=unbindable", e.getOutputRow()[0]);
    assertEquals("can't convert value", e.getCause().getMessage().trim());
    assertTrue(verdicts.log.isEmpty(), verdicts.log.toString());
    assertTrue(fake.committed().isEmpty());
    assertTrue(batch.isEmpty());
  }

  @Test
  void aStatementIsCutShortBeforeItsValuesPassTheByteLimit() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true);
    MultiRowBatch batch =
        new MultiRowBatch(
            fake.connection(),
            fake.statements(8, 100),
            BatchFailureContract.MARKS_FAILED_ROWS,
            true);
    assertEquals(100, batch.getMaxStatementBytes());
    for (int i = 1; i <= 10; i++) {
      add(batch, i, "x".repeat(30));
    }
    // Too large to share a statement, and the row after it.
    add(batch, 11, "y".repeat(150));
    add(batch, 12, "z");
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);

    // Three rows of 30 bytes fit in 100, a fourth doesn't: a statement takes the largest power of
    // two of the rows that fit, so that it is one statement, and the rest start the next one.
    assertEquals(List.of(2, 2, 2, 2, 2, 1, 1), fake.statementSizes);
    assertEquals(Set.of(2, 1), batch.getPreparedSizes());
    assertEquals(12, fake.committed().size());
    assertEquals(12, verdicts.log.size());
    assertTrue(verdicts.log.stream().allMatch(v -> v.startsWith("w:")), verdicts.log.toString());

    // Small rows fill their statements to the rows per statement.
    addKeys(batch, 13, 21);
    batch.flush(verdicts);
    assertEquals(List.of(2, 2, 2, 2, 2, 1, 1, 8, 1), fake.statementSizes);
  }

  @Test
  void withOneRowPerStatementTheByteLimitIsNotAsked() {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true);
    MultiRowBatch batch =
        new MultiRowBatch(
            fake.connection(),
            fake.statements(1, 100),
            BatchFailureContract.MARKS_FAILED_ROWS,
            true);
    assertEquals(0, batch.getMaxStatementBytes());
  }

  @Test
  void aFailedCommitSendsTheWindowRowByRow() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true).failCommits(1);
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 4);
    addKeys(batch, 1, 9);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);
    assertEquals(written(1, 9), verdicts.log);
    assertEquals(9, fake.committed().size());
    assertEquals(9, batch.getFallbackRows());
  }

  @Test
  void aStatementTheDriverClosedIsPreparedAgain() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.LOSES_WHOLE_BATCH, true)
            .failOn(3)
            .closeStatementsOnFailure();
    MultiRowBatch batch = batch(fake, BatchFailureContract.LOSES_WHOLE_BATCH, 4);
    addKeys(batch, 1, 8);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);
    assertEquals(written(1, 8, 3), verdicts.log);
    addKeys(batch, 10, 13);
    batch.flush(verdicts);
    assertEquals(11, fake.committed().size());
    assertTrue(fake.preparedSizes.stream().filter(size -> size == 4).count() >= 2);
  }

  @Test
  void withoutTransactionsEveryRowIsAStatementOfItsOwn() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, false).failOn(2);
    MultiRowBatch batch =
        new MultiRowBatch(
            fake.connection(), fake.statements(10), BatchFailureContract.MARKS_FAILED_ROWS, false);
    assertEquals(1, batch.getRowsPerStatement());
    addKeys(batch, 1, 3);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);
    assertEquals(written(1, 3, 2), verdicts.log);
    assertEquals(Set.of(1), fake.preparedSizeSet());
  }

  @Test
  void oneRowPerStatementIsARecoverableBatch() throws Exception {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true).failOn(4);
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 1);
    addKeys(batch, 1, 6);
    Verdicts verdicts = new Verdicts();
    batch.flush(verdicts);
    assertEquals(written(1, 6, 4), verdicts.log);
    assertEquals(1, verdicts.counts.get("1=v1"));
    assertEquals(0, batch.getFallbackRows());
  }

  @Test
  void discardDropsThePendingRows() {
    FakeMultiRowDatabase fake =
        new FakeMultiRowDatabase(BatchFailureContract.MARKS_FAILED_ROWS, true);
    MultiRowBatch batch = batch(fake, BatchFailureContract.MARKS_FAILED_ROWS, 4);
    addKeys(batch, 1, 6);
    assertEquals(6, batch.size());
    assertEquals(6, batch.discard().size());
    assertTrue(batch.isEmpty());
    batch.close();
  }
}

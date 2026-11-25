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

import static org.apache.hop.pipeline.transforms.update.UpdateRun.ROW;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.ids;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.rows;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableUpdatedExcept;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.pipeline.transforms.update.UpdateRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins what Update does on DuckDB when a row fails. DuckDB's driver loses the whole batch on a
 * failure, together with everything else in the transaction since the last commit, and closes the
 * statement; it has no savepoints. A failed statement aborts the transaction until it is rolled
 * back. Since #9722 that contract is used to recover failed batches; a test whose name says legacy
 * runs with HOP_DATABASE_BATCH_RECOVERY=LEGACY, which keeps the handling as it was.
 */
class UpdateBatchBehaviourDuckDBTest {

  @TempDir static Path folder;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  /** A DuckDB database file with the target table, holding ids 1..n named x. */
  static DatabaseMeta target(int n) throws Exception {
    String file = folder.resolve(UUID.randomUUID() + ".duckdb").toString();
    DatabaseMeta databaseMeta =
        new DatabaseMeta("duckdb", "DuckDB", "Native", "", file, "", "", "");
    UpdateRun.createTarget(databaseMeta, n);
    return databaseMeta;
  }

  private static List<Long> counters(
      long input, long updated, long skipped, long written, long rejected) {
    return UpdateBatchBehaviourH2Test.counters(input, updated, skipped, written, rejected);
  }

  @Test
  void batchOn_errorHop_withoutFailuresEveryRowIsUpdated() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db).batch(true).errorHop(true).commitSize(4).watchCommits().run(rows(10));

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(tableUpdatedExcept(10), UpdateRun.table(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(counters(10, 10, 0, 10, 0), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: the first failed batch stops the
   * transform although there is an error hop. DuckDB closed the statement, so clearing its batch
   * fails before the rows are sorted out. The rows of the failed batch, 6-10, reach no stream; only
   * the batch committed before it, 1-5, is in the table and was passed on.
   */
  @Test
  void batchOn_errorHop_legacy_Buggy_aFailedBatchStopsTheTransform() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(rows(10, 8));

    assertTrue(r.batchMode);
    assertEquals(ids(1, 2, 3, 4, 5), r.outputKeys());
    assertEquals(ids(), r.errorKeys());
    assertEquals(tableWith(10, id -> id <= 5), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertTrue(r.errorLog.stream().anyMatch(line -> line.contains("Statement was closed")));
    assertEquals(counters(10, 5, 0, 5, 0), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: a failure in the first batch,
   * and no row reaches a stream at all.
   */
  @Test
  void batchOn_errorHop_legacy_Buggy_aFailureInTheFirstBatchLeavesEveryRowOnNoStream()
      throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(rows(10, 3, 7));

    assertEquals(ids(), r.outputKeys());
    assertEquals(ids(), r.errorKeys());
    assertEquals(tableWith(10, id -> false), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(5, 0, 0, 0, 0), r.counters());
  }

  /**
   * Fixed by #9722: DuckDB's contract is validated (it loses the whole batch), so a failed batch is
   * recovered. Only the failed rows go to the error hop, the others are committed and passed on, in
   * input order, also after the driver closed the statement.
   */
  @Test
  void batchOn_errorHop_aFailedBatchIsRecovered() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .watchCommits()
            .run(rows(10, 3, 7, 8));

    assertEquals(idsWithout(10, 3, 7, 8), r.outputKeys());
    assertEquals(ids(3, 7, 8), r.errorKeys());
    assertEquals(Collections.nCopies(3, "UPD002"), r.errorCodes());
    assertEquals(tableUpdatedExcept(10, 3, 7, 8), UpdateRun.table(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(Collections.nCopies(3, UpdateRun.SEED), r.committedAtError);
    assertEquals(0, r.errorCount);
    assertFalse(r.stopped);
    assertEquals(counters(10, 7, 0, 7, 3), r.counters());
    assertTrue(r.recoveredBatches);
  }

  /** Without an error hop a failed batch stops the transform, the batches before it stay. */
  @Test
  void batchOn_noErrorHop_aFailedBatchIsRolledBackAndStopsTheTransform() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(false).commitSize(5).run(rows(10, 7));

    assertEquals(ids(1, 2, 3, 4, 5), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 5), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
  }

  static Stream<Arguments> rowByRowFailures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {{}, {1}, {3}, {5}, {10}, {2, 3}, {3, 7}, {1, 5, 6, 10}};
    for (int commitSize : new int[] {1, 3, 5, 100}) {
      for (long[] bad : badSets) {
        arguments.add(Arguments.of(commitSize, bad));
      }
    }
    return arguments.stream();
  }

  /**
   * Batching off: a failed row aborts DuckDB's transaction, which has no savepoints. The transform
   * rolls it back and runs the updates of the rows that wait for the commit again, so only the
   * failed rows go to the error hop; the other rows are committed and passed on after their commit.
   * Before, every row after the first failure went to the error hop as well, and the rows in front
   * of it were lost with the transaction although they were passed on.
   */
  @ParameterizedTest(name = "commit size {0}, bad rows {1}")
  @MethodSource("rowByRowFailures")
  void batchOff_errorHop_onlyTheFailedRowsAreRejected(int commitSize, long[] bad) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(false)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, bad));

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertTrue(r.runAgainAfterFailure);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(Collections.nCopies(bad.length, "UPD001"), r.errorCodes());
    assertEquals(Collections.nCopies(bad.length, ROW), r.errorKinds());
    assertEquals(tableUpdatedExcept(10, bad), UpdateRun.table(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(Collections.nCopies(bad.length, UpdateRun.SEED), r.committedAtError);
    assertEquals(0, r.errorCount);
    assertEquals(counters(10, 10 - bad.length, 0, 10 - bad.length, bad.length), r.counters());
  }

  /** A lookup that fails aborts the transaction as well: only its own row is rejected. */
  @Test
  void batchOff_errorHop_aFailedLookupOnlyRejectsItsOwnRow() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(false)
            .errorHop(true)
            .commitSize(4)
            .watchCommits()
            .configure(UpdateRecoveredBatchDuckDBTest::lookUpByTheKeyAsWell)
            .run(
                UpdateRecoveredBatchDuckDBTest.keyTextRowMeta(),
                UpdateRecoveredBatchDuckDBTest.keyTextRows(10, 3, 7));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertTrue(
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("Conversion Error")),
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(counters(8, 8, 0, 8, 2), r.counters());
  }

  /** Rows that need no update or whose key is not found keep their place around a failed row. */
  @Test
  void batchOff_errorHop_rowsThatAreNotUpdatedKeepTheirPlace() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(false)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(UpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(ids(1, 2, 3, 4, 6), r.outputKeys());
    assertEquals(ids(11, 12, 5), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001", "UPD001"), r.errorCodes());
    assertEquals(List.of("[11]", "[12]"), r.keysNotFound());
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(counters(8, 3, 2, 5, 3), r.counters());
  }

  /**
   * Without an error hop a failed row stops the transform; the rows since the commit are dropped.
   */
  @Test
  void batchOff_noErrorHop_onlyCommittedRowsArePassedOn() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db).batch(false).errorHop(false).commitSize(4).watchCommits().run(rows(10, 7));

    assertFalse(r.runAgainAfterFailure);
    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), UpdateRun.table(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(1, r.errorCount);
    assertEquals(counters(7, 4, 0, 4, 0), r.counters());
  }

  /**
   * Buggy, as it still is with "As before" on the connection (an unvalidated contract): nothing
   * runs again. The failed row 3 aborts the transaction and DuckDB closes the UPDATE statement. The
   * lookups of rows 4 and 5 fail on the aborted transaction; the commit after row 5 ends it without
   * writing rows 1 and 2, which are passed on as if committed. Every row after it fails on the
   * closed UPDATE statement.
   */
  @Test
  void batchOff_errorHop_unknownOnTheConnection_Buggy_aFailedRowAbortsTheTransaction()
      throws Exception {
    DatabaseMeta db = target(10);
    db.setBatchFailureContractOverride("UNKNOWN");

    Result r =
        UpdateRun.on(db)
            .batch(false)
            .errorHop(true)
            .commitSize(5)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertTrue(r.holdUntilCommit);
    assertFalse(r.runAgainAfterFailure);
    assertEquals(ids(1, 2), r.outputKeys());
    assertEquals(ids(3, 4, 5, 6, 7, 8, 9, 10), r.errorKeys());
    assertEquals(tableWith(10, id -> false), UpdateRun.table(db));
    assertEquals(ids(1, 2), r.passedOnBeforeTheirCommit());
    assertEquals(counters(8, 2, 0, 2, 8), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: the failed row 3 aborts the
   * transaction. Every row after it goes to the error hop as well, and rows 1 and 2, passed on
   * right after their update, are lost with the transaction.
   */
  @Test
  void batchOff_errorHop_legacy_Buggy_aFailedRowAbortsTheTransaction() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .batch(false)
            .errorHop(true)
            .commitSize(5)
            .run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(ids(1, 2), r.outputKeys());
    assertEquals(ids(3, 4, 5, 6, 7, 8, 9, 10), r.errorKeys());
    assertEquals(Collections.nCopies(8, "UPD001"), r.errorCodes());
    assertEquals(tableWith(10, id -> false), UpdateRun.table(db));
    assertEquals(0, r.errorCount);
    assertEquals(counters(3, 2, 0, 2, 8), r.counters());
  }
}

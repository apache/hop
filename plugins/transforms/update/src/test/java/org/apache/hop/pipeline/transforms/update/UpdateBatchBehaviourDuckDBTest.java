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

import static org.apache.hop.pipeline.transforms.update.UpdateRun.ids;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.rows;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableUpdatedExcept;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
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
 * runs with HOP_DATABASE_BATCH_RECOVERY=LEGACY, which keeps the handling as it was. With batch
 * updates off the rows run one by one exactly as before, on every path.
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

  // ----------------------------------------------------------------------------------------------
  // Batch updates off
  //
  // #9722 changes only how a failed batch is recovered. Row by row, the default path, an
  // unvalidated contract on the connection and HOP_DATABASE_BATCH_RECOVERY=LEGACY all give what
  // main gives.
  // ----------------------------------------------------------------------------------------------

  /** Recovery variable and contract on the connection: default, LEGACY, and "As before". */
  static Stream<Arguments> rowByRowPaths() {
    return Stream.of(
        Arguments.of(null, null), Arguments.of("LEGACY", null), Arguments.of(null, "UNKNOWN"));
  }

  /**
   * Buggy: the failed row 3 aborts the transaction. Every row after it goes to the error hop as
   * well, and rows 1 and 2, passed on right after their update, are lost with the transaction.
   */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}")
  @MethodSource("rowByRowPaths")
  void batchOff_errorHop_Buggy_aFailedRowAbortsTheTransaction(String recovery, String contract)
      throws Exception {
    DatabaseMeta db = target(10);
    if (contract != null) {
      db.setBatchFailureContractOverride(contract);
    }

    Result r =
        UpdateRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(5)
            .run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertFalse(r.recoveredBatches);
    assertFalse(r.useSavepoints);
    assertEquals(ids(1, 2), r.outputKeys());
    assertEquals(ids(3, 4, 5, 6, 7, 8, 9, 10), r.errorKeys());
    assertEquals(Collections.nCopies(8, "UPD001"), r.errorCodes());
    assertEquals(tableWith(10, id -> false), UpdateRun.table(db));
    assertEquals(0, r.errorCount);
    assertEquals(counters(3, 2, 0, 2, 8), r.counters());
  }

  /**
   * Buggy: without an error hop a failed row stops the transform and the transaction is rolled
   * back; rows 5 and 6, already passed on, are not in the table.
   */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}")
  @MethodSource("rowByRowPaths")
  void batchOff_noErrorHop_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(
      String recovery, String contract) throws Exception {
    DatabaseMeta db = target(10);
    if (contract != null) {
      db.setBatchFailureContractOverride(contract);
    }

    Result r =
        UpdateRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(false)
            .commitSize(4)
            .run(rows(10, 7));

    assertEquals(idsWithout(6), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(7, 6, 0, 6, 0), r.counters());
  }
}

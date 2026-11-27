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

package org.apache.hop.pipeline.transforms.delete;

import static org.apache.hop.pipeline.transforms.delete.DeleteRun.BATCH;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.CLOSED;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ROW;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.counters;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ids;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.idsWithout;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.remaining;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.rows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test.Legacy;
import org.apache.hop.pipeline.transforms.delete.DeleteRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins what Delete does on DuckDB when a row fails. DuckDB refuses the delete of a row that a
 * foreign key still references, as the other databases do. Its driver loses the whole batch on a
 * failure, together with everything else in the transaction since the last commit, and closes the
 * statement; it has no savepoints. A failed statement aborts the transaction until it is rolled
 * back. Since #9768 that validated contract (LOSES_WHOLE_BATCH) recovers failed batches when batch
 * deletes are on; the tests whose name says legacy run with HOP_DATABASE_BATCH_RECOVERY=LEGACY and
 * with the contract "As before" (UNKNOWN) on the connection, which keep the handling as it was, and
 * DeleteRecoveredBatchDuckDBTest covers the default path. With batch deletes off the rows run one
 * by one exactly as before, on every path.
 */
class DeleteBatchBehaviourDuckDBTest {

  @TempDir static Path folder;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  /** A DuckDB database file with the target table holding ids 1..n, the given ids referenced. */
  static DatabaseMeta target(int n, long... referenced) throws Exception {
    String file = folder.resolve(UUID.randomUUID() + ".duckdb").toString();
    DatabaseMeta databaseMeta =
        new DatabaseMeta("duckdb", "DuckDB", "Native", "", file, "", "", "");
    DeleteRun.createTarget(databaseMeta, n, referenced);
    return databaseMeta;
  }

  static DeleteRun on(Legacy path, DatabaseMeta databaseMeta) {
    return DeleteBatchBehaviourH2Test.on(path, databaseMeta);
  }

  /** Whether error row {@code index} carries DuckDB's refusal by the foreign key. */
  static boolean foreignKeyViolation(Result r, int index) {
    return ((String) r.errorField(DeleteRun.ERROR_DESCRIPTIONS).get(index))
        .contains("Violates foreign key constraint");
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * without failures every row is deleted, but a row is passed on as soon as it is added to the
   * batch, before the batch runs.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#legacyPaths")
  void batchOn_errorHop_legacy_Buggy_withoutFailuresRowsLeaveBeforeTheirBatchRuns(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(4).watchCommits().run(rows(10));

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(ids(), remaining(db));
    assertEquals(ids(1, 2, 3, 5, 6, 7, 9, 10), r.passedOnBeforeTheirCommit());
    assertEquals(counters(10, 10, 10, 0), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * the second batch fails on row 8 although there is an error hop. Only the row that ran the
   * batch, 10, goes to the error hop; rows 6-9 were passed on already. DuckDB lost the whole batch
   * and closed the statement, so the transform fails when it is disposed, and only the batch
   * committed before, 1-5, is deleted.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#legacyPaths")
  void batchOn_errorHop_legacy_Buggy_aFailedBatchLosesItsRowsAndFailsTheTransform(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10, 8);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(5).run(rows(10));

    assertTrue(r.batchMode);
    assertEquals(idsWithout(9), r.outputKeys());
    assertEquals(ids(10), r.errorKeys());
    assertEquals(List.of("DEL001"), r.errorCodes());
    assertEquals(List.of(BATCH), r.errorKinds());
    assertTrue(foreignKeyViolation(r, 0));
    assertTrue(r.errorLog.stream().anyMatch(line -> line.contains("Statement was closed")));
    assertEquals(ids(6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(10, 9, 9, 1), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * a failure in the first batch. Row 5, which ran it, goes to the error hop, and so does every row
   * after it: DuckDB closed the statement, so binding their keys fails. Rows 1-4 were passed on,
   * and nothing is deleted.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#legacyPaths")
  void batchOn_errorHop_legacy_Buggy_aFailureInTheFirstBatchRejectsEveryRowAfterIt(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10, 3, 7);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(5).run(rows(10));

    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(ids(5, 6, 7, 8, 9, 10), r.errorKeys());
    assertEquals(Collections.nCopies(6, "DEL001"), r.errorCodes());
    List<String> kinds = new ArrayList<>(List.of(BATCH));
    kinds.addAll(Collections.nCopies(5, CLOSED));
    assertEquals(kinds, r.errorKinds());
    assertEquals(idsWithout(10), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(10, 4, 4, 6), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * without an error hop a failed batch stops the transform, but the rows of that batch, 6-9, were
   * passed on as they were added; they are still in the table.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#legacyPaths")
  void batchOn_noErrorHop_legacy_Buggy_theRowsOfTheFailedBatchWerePassedOn(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10, 7);

    Result r = on(path, db).batch(true).errorHop(false).commitSize(5).run(rows(10));

    assertEquals(idsWithout(9), r.outputKeys());
    assertEquals(ids(6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(10, 9, 9, 0), r.counters());
  }

  /**
   * Buggy: row by row, the failed row 3 closes the statement and aborts the transaction. Every row
   * after it goes to the error hop as well, and rows 1 and 2, passed on right after their delete,
   * are lost with the transaction.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#paths")
  void batchOff_errorHop_Buggy_aFailedRowAbortsTheTransaction(Legacy path) throws Exception {
    DatabaseMeta db = target(10, 3, 7);

    Result r = on(path, db).batch(false).errorHop(true).commitSize(5).run(rows(10));

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(ids(1, 2), r.outputKeys());
    assertEquals(ids(3, 4, 5, 6, 7, 8, 9, 10), r.errorKeys());
    assertEquals(Collections.nCopies(8, "DEL001"), r.errorCodes());
    List<String> kinds = new ArrayList<>(List.of(ROW));
    kinds.addAll(Collections.nCopies(7, CLOSED));
    assertEquals(kinds, r.errorKinds());
    assertTrue(foreignKeyViolation(r, 0));
    assertEquals(idsWithout(10), remaining(db));
    assertEquals(0, r.errorCount);
    assertEquals(counters(10, 2, 2, 8), r.counters());
  }

  /**
   * Buggy: without an error hop a failed row stops the transform and the transaction is rolled
   * back; rows 5 and 6, already passed on, are still in the table.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#paths")
  void batchOff_noErrorHop_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10, 7);

    Result r = on(path, db).batch(false).errorHop(false).commitSize(4).run(rows(10));

    assertEquals(idsWithout(6), r.outputKeys());
    assertEquals(ids(5, 6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(7, 6, 6, 0), r.counters());
  }
}

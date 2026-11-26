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

import static org.apache.hop.pipeline.transforms.update.UpdateRun.DRIVER;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.ERROR_FIELDS;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.NOT_FOUND;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.NR_ERRORS;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.ROW;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.ids;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.rows;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableUpdatedExcept;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.engines.local.transaction.SharedConnectionRun;
import org.apache.hop.pipeline.transforms.update.UpdateRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pins what Update does with batch updates and an error hop, end to end against H2: with and
 * without batch updates, with and without an error hop, for several commit sizes, with lookups that
 * find, miss or are skipped, at the end of a single-threaded iteration and when the transform is
 * stopped, and on a dialect that runs every row behind a savepoint the way PostgreSQL does.
 *
 * <p>Each test checks the rows on the main output (values and order), the rows on the error hop
 * (error codes, fields and the kind of message), what the table holds after the run, the counters,
 * and for the important cases when the rows left the transform and whether they were committed by
 * then. These are characterization tests: they describe current behaviour, also where it is wrong.
 * Those tests have Buggy in their name and say so. A failed row is a name the table refuses.
 *
 * <p>H2, like SQL Server and MySQL, runs the whole batch and marks the failed rows with {@code
 * EXECUTE_FAILED} in the update counts, chaining one exception per failed row. Since #9722 its
 * validated contract recovers failed batches with RecoverableBatch, which routes the rows of these
 * tests the same way; a test whose name says legacy runs with HOP_DATABASE_BATCH_RECOVERY=LEGACY.
 */
class UpdateBatchBehaviourH2Test {

  /** H2 that asks for a savepoint per row with an error hop, the way PostgreSQL does. */
  @DatabaseMetaPlugin(type = "H2_UPDATE_SAVEPOINTS", typeDescription = "H2 with savepoints")
  public static class H2SavepointsDatabaseMeta extends H2DatabaseMeta {
    @Override
    public boolean isUseSafePoints() {
      return true;
    }
  }

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2SavepointsDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    SharedConnectionRun.registerConnectionGroupExtensionPoint();
  }

  /** An H2 database of the given type with the target table, holding ids 1..n named x. */
  static DatabaseMeta target(String type, int n) throws Exception {
    String name =
        "mem:update" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta = new DatabaseMeta("h2", type, "Native", "", name, "", "sa", "");
    UpdateRun.createTarget(databaseMeta, n);
    return databaseMeta;
  }

  static DatabaseMeta target(int n) throws Exception {
    return target("H2", n);
  }

  static List<Object> repeat(Object value, int times) {
    return Collections.nCopies(times, value);
  }

  static List<String> repeatKind(String kind, int times) {
    return Collections.nCopies(times, kind);
  }

  /**
   * lines input, updated, skipped, written (to the output), rejected (to the error hop), and lines
   * output, which Update never counts.
   */
  static List<Long> counters(long input, long updated, long skipped, long written, long rejected) {
    return List.of(input, updated, skipped, written, rejected, 0L);
  }

  /** Every error row says one error, and Update only names the fields for a missing key. */
  static void assertNoErrorFields(Result r) {
    for (Object nrErrors : r.errorField(NR_ERRORS)) {
      assertEquals(1L, nrErrors);
    }
    for (Object fields : r.errorField(ERROR_FIELDS)) {
      assertNull(fields);
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Batch updates off
  //
  // #9722 changes only how a failed batch is recovered. Every test here runs on the default path
  // and with HOP_DATABASE_BATCH_RECOVERY=LEGACY, and both must give what main gives: row by row,
  // a row is passed on right after its update, before the commit that includes it.
  // ----------------------------------------------------------------------------------------------

  /** The default path, and HOP_DATABASE_BATCH_RECOVERY=LEGACY. */
  static Stream<String> recoveries() {
    return Stream.of(null, "LEGACY");
  }

  static Stream<Arguments> rowByRowCommits() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      arguments.add(Arguments.of(recovery, 1, ids()));
      // Every third row written commits the rows before it: 4, 8 and the rows before them
      arguments.add(Arguments.of(recovery, 3, ids(1, 2, 5, 6, 9, 10)));
      // One commit at the end
      arguments.add(Arguments.of(recovery, 100, idsWithout(10, 3, 7)));
    }
    return arguments.stream();
  }

  /**
   * Buggy: without batching a row is passed on as soon as it is updated, before the commit that the
   * commit size asks for.
   */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("rowByRowCommits")
  void batchOff_errorHop_Buggy_rowsLeaveBeforeTheirCommit(
      String recovery, int commitSize, List<Object> passedOnBeforeTheirCommit) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertFalse(r.recoveredBatches);
    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(repeat("UPD001", 2), r.errorCodes());
    assertEquals(repeatKind(ROW, 2), r.errorKinds());
    assertNoErrorFields(r);
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(counters(10, 8, 0, 8, 2), r.counters());
    assertEquals(0, r.errorCount);
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2), r.errorSizeAtRead);
    assertEquals(passedOnBeforeTheirCommit, r.passedOnBeforeTheirCommit());
  }

  /**
   * Buggy: without an error hop a failed row stops the transform and the transaction is rolled
   * back. Rows 1-4 were committed by the commit size of 4; rows 5 and 6, already passed on, are
   * rolled back with the transaction and not in the table.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_noErrorHop_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(String recovery)
      throws Exception {
    DatabaseMeta db = target(10);

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

  /**
   * Rows that need no update or whose key is not found are passed on at once, like the updated
   * rows.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_errorHop_rowsThatAreNotUpdatedLeaveAtOnce(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(3)
            .run(mixedRows());

    assertEquals(ids(1, 2, 3, 4, 6), r.outputKeys());
    assertEquals(ids(11, 12, 5), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001", "UPD001"), r.errorCodes());
    assertEquals(List.of(NOT_FOUND, NOT_FOUND, ROW), r.errorKinds());
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(counters(8, 3, 2, 5, 3), r.counters());
    assertEquals(List.of(0, 1, 1, 2, 3, 3, 4, 4, 5), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 1, 1, 1, 2, 2, 3, 3), r.errorSizeAtRead);
  }

  /**
   * A transform that is stopped commits the updates it ran when it is disposed: with commit size 5,
   * stopped after row 7, rows 1-7 are passed on and all seven are in the table. The review of #9723
   * caught a change that dropped rows 6 and 7 here; this pins it.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_stopped_everyRowPassedOnIsCommitted(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(5)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 7) {
                    transform.stopAll();
                  }
                })
            .run(rows(10));

    assertTrue(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(ids(1, 2, 3, 4, 5, 6, 7), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 7), UpdateRun.table(db));
    assertEquals(counters(7, 7, 0, 7, 0), r.counters());
  }

  /**
   * Buggy: a transform that failed rolls back what was not committed yet, although those rows were
   * passed on. With commit size 5, failed after row 7: rows 1-7 are passed on, rows 1-5 are in the
   * table.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_failed_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(String recovery)
      throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(5)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 7) {
                    transform.setErrors(1);
                    transform.stopAll();
                  }
                })
            .run(rows(10));

    assertEquals(ids(1, 2, 3, 4, 5, 6, 7), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 5), UpdateRun.table(db));
    assertEquals(counters(7, 7, 0, 7, 0), r.counters());
  }

  /**
   * At the end of a single-threaded iteration the open transaction is committed; the rows were
   * passed on right after their update already.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_batchComplete_commitsTheOpenTransaction(String recovery) throws Exception {
    DatabaseMeta db = target(5);
    List<List<String>> tableAtBatchComplete = new ArrayList<>();

    Result r =
        UpdateRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    tableAtBatchComplete.add(UpdateRun.table(db));
                  }
                })
            .run(rows(5, 2));

    assertEquals(List.of(tableWith(5, id -> id == 1 || id == 3)), tableAtBatchComplete);
    assertEquals(List.of(0, 1, 1, 2, 3, 4), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 1, 1, 1, 1), r.errorSizeAtRead);
    assertEquals(ids(1, 3, 4, 5), r.outputKeys());
    assertEquals(ids(2), r.errorKeys());
    assertEquals(tableUpdatedExcept(5, 2), UpdateRun.table(db));
    assertEquals(counters(5, 4, 0, 4, 1), r.counters());
  }

  /**
   * On a connection shared by a transactional pipeline the commit only comes at the end of the
   * pipeline; the rows are passed on right after their update.
   */
  @Test
  void batchOff_transactionalPipeline_rowsLeaveAtOnce() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(false)
            .errorHop(true)
            .commitSize(100)
            .connectionGroup("update-" + UUID.randomUUID())
            .run(rows(10, 3, 7));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2), r.errorSizeAtRead);
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(counters(10, 8, 0, 8, 2), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // Batch updates on, with an error hop
  // ----------------------------------------------------------------------------------------------

  static Stream<Arguments> batchFailures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {
      {}, {1}, {3}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}
    };
    for (int commitSize : new int[] {2, 3, 5, 100}) {
      for (long[] bad : badSets) {
        arguments.add(Arguments.of(commitSize, bad));
      }
    }
    return arguments.stream();
  }

  /**
   * H2 marks the failed rows of a batch, so only they go to the error hop, with the driver's own
   * message; the other rows of the batch are committed and passed on.
   */
  @ParameterizedTest(name = "commit size {0}, bad rows {1}")
  @MethodSource("batchFailures")
  void batchOn_errorHop_onlyTheFailedRowsAreRejected(int commitSize, long[] bad) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, bad));

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertFalse(r.stopped);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(repeat("UPD002", bad.length), r.errorCodes());
    assertEquals(repeatKind(DRIVER, bad.length), r.errorKinds());
    assertNoErrorFields(r);
    assertEquals(tableUpdatedExcept(10, bad), UpdateRun.table(db));
    assertEquals(counters(10, 10 - bad.length, 0, 10 - bad.length, bad.length), r.counters());
    assertEquals(0, r.errorCount);
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(repeat(UpdateRun.SEED, bad.length), new ArrayList<Object>(r.committedAtError));
  }

  /** The rows of a batch leave together, once the batch has run and is committed. */
  @Test
  void batchOn_errorHop_rowsLeaveABatchAtATime() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 3, 7));

    assertEquals(List.of(0, 0, 0, 0, 3, 3, 3, 3, 6, 6, 6), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2), r.errorSizeAtRead);
  }

  @Test
  void batchOn_errorHop_commitSizeOne_everyRowIsItsOwnBatch() throws Exception {
    DatabaseMeta db = target(5);

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(1).run(rows(5, 2, 4));

    assertTrue(r.batchMode);
    assertEquals(ids(1, 3, 5), r.outputKeys());
    assertEquals(ids(2, 4), r.errorKeys());
    assertEquals(List.of(0, 1, 1, 2, 2, 3), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 1, 1, 2, 2), r.errorSizeAtRead);
    assertEquals(tableUpdatedExcept(5, 2, 4), UpdateRun.table(db));
    assertEquals(counters(5, 3, 0, 3, 2), r.counters());
  }

  /** 10 rows with commit size 4: rows 9 and 10 are a partial batch, run at the end of the input. */
  @Test
  void batchOn_errorHop_theLastPartialBatchRunsAtTheEndOfTheInput() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 10));

    assertEquals(idsWithout(10, 10), r.outputKeys());
    assertEquals(ids(10), r.errorKeys());
    assertEquals(List.of(0, 0, 0, 0, 4, 4, 4, 4, 8, 8, 8), r.outputSizeAtRead);
    assertEquals(tableUpdatedExcept(10, 10), UpdateRun.table(db));
    assertEquals(counters(10, 9, 0, 9, 1), r.counters());
  }

  /**
   * Commit size 0 is auto-commit: no batches, every row is committed on its own and passed on right
   * after it.
   */
  @Test
  void batchOn_commitSizeZero_turnsBatchModeOff() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db).batch(true).errorHop(true).commitSize(0).watchCommits().run(rows(10, 3));

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10, 3), r.outputKeys());
    assertEquals(ids(3), r.errorKeys());
    assertEquals(List.of("UPD001"), r.errorCodes());
    assertEquals(List.of(ROW), r.errorKinds());
    // Every update is committed on its own, so every row leaves at once
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 6, 7, 8, 9), r.outputSizeAtRead);
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableUpdatedExcept(10, 3), UpdateRun.table(db));
    assertEquals(counters(10, 9, 0, 9, 1), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // Batch updates on, without an error hop
  // ----------------------------------------------------------------------------------------------

  /**
   * A failed batch is rolled back and stops the transform. The rows of the batches committed before
   * it were passed on and stay; none of the failed batch's rows is passed on.
   */
  @Test
  void batchOn_noErrorHop_aFailedBatchIsRolledBackAndStopsTheTransform() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10, 7));

    assertTrue(r.batchMode);
    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertNull(r.thrown);
    assertEquals(counters(8, 4, 0, 4, 0), r.counters());
    assertTrue(
        r.errorLog.stream()
            .anyMatch(line -> line.contains("Error updating a batch of rows in table")));
  }

  @Test
  void batchOn_noErrorHop_theFirstBatchFails() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10, 2));

    assertEquals(ids(), r.outputKeys());
    assertEquals(tableWith(10, id -> false), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(4, 0, 0, 0, 0), r.counters());
  }

  /** The partial batch at the end of the input fails: only the full batches before it stay. */
  @Test
  void batchOn_noErrorHop_theLastPartialBatchFails() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10, 9));

    assertEquals(idsWithout(8), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 8), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(10, 8, 0, 8, 0), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // Lookups: found, not found, ignored, skipped, nothing to update
  // ----------------------------------------------------------------------------------------------

  /**
   * 1 r1 (update), 11 r11 (not in the table), 2 x (no update needed), 3 r3 (update), 12 r12 (not in
   * the table), 4 r4 (update), 5 bad (update that fails), 6 x (no update needed).
   */
  static List<Object[]> mixedRows() {
    return List.of(
        new Object[] {1L, "r1"},
        new Object[] {11L, "r11"},
        new Object[] {2L, UpdateRun.SEED},
        new Object[] {3L, "r3"},
        new Object[] {12L, "r12"},
        new Object[] {4L, "r4"},
        new Object[] {5L, UpdateRun.BAD},
        new Object[] {6L, UpdateRun.SEED});
  }

  /**
   * A key that is not in the table goes to the error hop with the key field named, and a row that
   * needs no update goes to the output. Both wait for the rows of the batch in front of them, so
   * every stream keeps the order of the input.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 5, 100})
  void batchOn_errorHop_rowsOutsideTheBatchKeepTheirPlace(int commitSize) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(mixedRows());

    assertEquals(ids(1, 2, 3, 4, 6), r.outputKeys());
    assertEquals(ids(11, 12, 5), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001", "UPD002"), r.errorCodes());
    assertEquals(List.of(NOT_FOUND, NOT_FOUND, DRIVER), r.errorKinds());
    assertEquals(List.of("id", "id"), r.errorField(ERROR_FIELDS).subList(0, 2));
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(counters(8, 3, 2, 5, 3), r.counters());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
  }

  /**
   * The rows waiting behind a batch count towards the commit size: with commit size 3, rows 1, 11
   * and 2 make the first batch run although only row 1 is in it.
   */
  @Test
  void batchOn_errorHop_rowsWaitingBehindTheBatchCountTowardsTheCommitSize() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(3).run(mixedRows());

    assertEquals(List.of(0, 0, 0, 2, 2, 2, 4, 4, 4), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1, 2, 2, 2), r.errorSizeAtRead);
  }

  static Stream<Arguments> keyNotFoundPaths() {
    return Stream.of(
        // The recovered batch of H2's validated contract
        Arguments.of("H2", null, true),
        // The batch as Update always ran it
        Arguments.of("H2", "LEGACY", true),
        // Row by row
        Arguments.of("H2", null, false),
        Arguments.of("H2", "LEGACY", false),
        // A savepoint per row, batching switched off by the error hop
        Arguments.of("H2_UPDATE_SAVEPOINTS", null, true));
  }

  /**
   * Every row whose key is not in the table goes to the error hop with a message that names its own
   * key, whatever path the row takes. Before, every such row got the message of the first one.
   */
  @ParameterizedTest(name = "{0}, recovery {1}, batch {2}")
  @MethodSource("keyNotFoundPaths")
  void keyNotFound_eachMessageNamesItsOwnKey(String type, String recovery, boolean batch)
      throws Exception {
    DatabaseMeta db = target(type, 10);

    Result r =
        UpdateRun.on(db)
            .recovery(recovery)
            .batch(batch)
            .errorHop(true)
            .commitSize(100)
            .run(mixedRows());

    assertEquals(ids(11, 12, 5), r.errorKeys());
    assertEquals(List.of(NOT_FOUND, NOT_FOUND), r.errorKinds().subList(0, 2));
    assertEquals(List.of("[11]", "[12]"), r.keysNotFound());
    assertEquals(List.of("id", "id"), r.errorField(ERROR_FIELDS).subList(0, 2));
  }

  /**
   * A lookup that fails (its key is no number) goes to the error hop on its own. H2 keeps the
   * lookup statement open after the failure, so it is used for every row: only a driver that closed
   * it (DuckDB) gets it prepared again.
   */
  @ParameterizedTest(name = "batch {0}")
  @ValueSource(booleans = {true, false})
  void aFailedLookupKeepsTheStatementOfADriverThatKeepsItOpen(boolean batch) throws Exception {
    DatabaseMeta db = target(10);
    Set<PreparedStatement> lookupStatements = Collections.newSetFromMap(new IdentityHashMap<>());

    Result r =
        UpdateRun.on(db)
            .batch(batch)
            .errorHop(true)
            .commitSize(4)
            .configure(UpdateRecoveredBatchDuckDBTest::lookUpByTheKeyAsWell)
            .afterRow(
                (processed, transform) ->
                    lookupStatements.add(transform.getData().prepStatementLookup))
            .run(
                UpdateRecoveredBatchDuckDBTest.keyTextRowMeta(),
                UpdateRecoveredBatchDuckDBTest.keyTextRows(10, 3, 7));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(1, lookupStatements.size());
  }

  /**
   * Without an error hop a missing key stops the transform. The rows in the batch had not run yet:
   * they are rolled back with the transaction and never passed on.
   */
  @Test
  void batchOn_noErrorHop_aMissingKeyStopsTheTransform() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(false).commitSize(100).run(mixedRows());

    assertEquals(ids(), r.outputKeys());
    assertEquals(tableWith(10, id -> false), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(2, 0, 0, 0, 0), r.counters());
  }

  /**
   * "Ignore lookup failure": a missing key goes to the output, in its place, with the flag field
   * false; found rows get true.
   */
  @Test
  void batchOn_ignoreLookupFailure_aMissingKeyIsPassedOnWithTheFlagFalse() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .configure(
                meta -> {
                  meta.setErrorIgnored(true);
                  meta.setIgnoreFlagField("found");
                })
            .run(mixedRows());

    assertEquals(ids(1, 11, 2, 3, 12, 4, 6), r.outputKeys());
    assertEquals(ids(5), r.errorKeys());
    assertArrayEquals(new String[] {"id", "name", "found"}, r.outputRowMeta.getFieldNames());
    assertEquals(
        List.of(true, false, true, true, false, true, true),
        r.output.stream().map(row -> row[2]).toList());
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    // A missing key is neither updated nor skipped
    assertEquals(counters(8, 3, 2, 7, 1), r.counters());
  }

  /**
   * "Skip lookup": every row goes into the batch. A key that is not in the table updates nothing,
   * which is not a failure: the row is passed on and counted as updated.
   */
  @Test
  void batchOn_skipLookup_aMissingKeyIsCountedAsUpdated() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .configure(meta -> meta.setSkipLookup(true))
            .run(mixedRows());

    assertEquals(ids(1, 11, 2, 3, 12, 4, 6), r.outputKeys());
    assertEquals(ids(5), r.errorKeys());
    assertEquals(List.of("UPD002"), r.errorCodes());
    assertEquals(List.of(0, 0, 0, 3, 3, 3, 6, 6, 6), r.outputSizeAtRead);
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(counters(8, 7, 0, 7, 1), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // batchComplete() and dispose()
  // ----------------------------------------------------------------------------------------------

  /**
   * At the end of a single-threaded iteration the batch runs, is committed and its rows are passed
   * on, although it is not full.
   */
  @Test
  void batchComplete_runsCommitsAndPassesOnThePendingBatch() throws Exception {
    DatabaseMeta db = target(5);
    List<List<String>> tableAtBatchComplete = new ArrayList<>();

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .watchCommits()
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    tableAtBatchComplete.add(UpdateRun.table(db));
                  }
                })
            .run(rows(5, 2));

    assertEquals(List.of(tableWith(5, id -> id == 1 || id == 3)), tableAtBatchComplete);
    assertEquals(List.of(0, 0, 0, 2, 2, 2), r.outputSizeAtRead);
    assertEquals(ids(1, 3, 4, 5), r.outputKeys());
    assertEquals(ids(2), r.errorKeys());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(counters(5, 4, 0, 4, 1), r.counters());
  }

  /** Without an error hop a failed batch at the end of an iteration is rolled back and thrown. */
  @Test
  void batchComplete_noErrorHop_aFailedBatchIsRolledBackAndThrown() throws Exception {
    DatabaseMeta db = target(5);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                  }
                })
            .run(rows(5, 2));

    assertInstanceOf(HopDatabaseBatchException.class, r.thrown);
    assertEquals(ids(), r.outputKeys());
    assertEquals(tableWith(5, id -> false), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: a transform that is stopped with
   * rows in its batch runs and commits them when it is disposed, but a stopped transform passes no
   * rows on: rows 6 and 7 are in the table and counted as updated, yet were never passed on.
   */
  @Test
  void stopped_legacy_Buggy_thePendingBatchIsCommittedButNeverPassedOn() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 7) {
                    transform.stopAll();
                  }
                })
            .run(rows(10));

    assertTrue(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(5), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 7), UpdateRun.table(db));
    assertEquals(counters(7, 7, 0, 5, 0), r.counters());
  }

  /**
   * Fixed by #9722, where the batch failure contract is validated (H2): the rows a stopped
   * transform still has in its batch are dropped, not committed, so the table holds exactly the
   * rows that were passed on.
   */
  @Test
  void stopped_thePendingBatchIsDroppedAndNotCommitted() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 7) {
                    transform.stopAll();
                  }
                })
            .run(rows(10));

    assertTrue(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(5), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 5), UpdateRun.table(db));
    assertEquals(counters(7, 5, 0, 5, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.startsWith("2 rows of the batch")));
    assertTrue(r.recoveredBatches);
  }

  /** A transform that failed rolls back the rows of its batch when it is disposed. */
  @Test
  void failed_thePendingBatchIsRolledBackOnDispose() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 7) {
                    transform.setErrors(1);
                    transform.stopAll();
                  }
                })
            .run(rows(10));

    assertEquals(idsWithout(5), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 5), UpdateRun.table(db));
    assertEquals(counters(7, 5, 0, 5, 0), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // A dialect that runs every row behind a savepoint (PostgreSQL and its look-alikes)
  // ----------------------------------------------------------------------------------------------

  /**
   * With an error hop, "use batch update" is switched off and logged: every row runs behind a
   * savepoint, so only the failed rows go to the error hop. Buggy, as without batching: every row
   * leaves right after its savepoint section, before the commit that includes its update.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 100})
  void savepointDialect_errorHop_Buggy_batchingIsSwitchedOffForASavepointPerRow(int commitSize)
      throws Exception {
    DatabaseMeta db = target("H2_UPDATE_SAVEPOINTS", 10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(repeat("UPD001", 2), r.errorCodes());
    assertEquals(repeatKind(ROW, 2), r.errorKinds());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(counters(10, 8, 0, 8, 2), r.counters());
    List<Object> passedOnBeforeTheirCommit =
        switch (commitSize) {
          case 1 -> ids();
            // The transform commits every third row that is updated, after it passed the rows on
          case 3 -> ids(1, 2, 5, 6, 9, 10);
          default -> idsWithout(10, 3, 7);
        };
    assertEquals(passedOnBeforeTheirCommit, r.passedOnBeforeTheirCommit());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: every row leaves right after its
   * savepoint section, and the transform commits every third row that is updated, after it passed
   * the rows on.
   */
  @Test
  void savepointDialect_errorHop_legacy_Buggy_rowsLeaveBeforeTheirCommit() throws Exception {
    DatabaseMeta db = target("H2_UPDATE_SAVEPOINTS", 10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertTrue(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(ids(1, 2, 5, 6, 9, 10), r.passedOnBeforeTheirCommit());
  }

  /** Without an error hop no savepoints are used and the dialect batches as usual. */
  @Test
  void savepointDialect_noErrorHop_keepsBatching() throws Exception {
    DatabaseMeta db = target("H2_UPDATE_SAVEPOINTS", 10);

    Result r = UpdateRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10, 7));

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertFalse(r.logged("batch updates are switched off"));
    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
  }
}

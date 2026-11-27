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

package org.apache.hop.pipeline.transforms.insertupdate;

import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ERROR_FIELDS;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.NR_ERRORS;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ROW;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ids;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.row;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.rows;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWith;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWrittenExcept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.engines.local.transaction.SharedConnectionRun;
import org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins what Insert/Update does today, row by row, end to end against H2: with and without an error
 * hop, for several commit sizes, with rows that are inserted, updated, need no change or are
 * refused, with "Don't perform any updates", at the end of a single-threaded iteration, when the
 * transform is stopped or failed, in a transactional pipeline, and on a dialect that runs every row
 * behind a savepoint the way PostgreSQL does.
 *
 * <p>Each test checks the rows on the main output (values and order), the rows on the error hop
 * (error codes and the kind of message), what the table holds after the run, the counters, and for
 * the important cases when the rows left the transform and whether they were committed by then.
 * These are characterization tests: they describe current behaviour, also where it is wrong. Those
 * tests have Buggy in their name and say so.
 *
 * <p>The table holds the odd ids, so in rows 1..n the odd ids are updated and the even ones
 * inserted, one after the other. A failed row is a name the table refuses. Every test runs on the
 * default path and with HOP_DATABASE_BATCH_RECOVERY=LEGACY, and both must give the same.
 */
class InsertUpdateBatchBehaviourH2Test {

  /** H2 that asks for a savepoint per row with an error hop, the way PostgreSQL does. */
  @DatabaseMetaPlugin(type = "H2_INSUPD_SAVEPOINTS", typeDescription = "H2 with savepoints")
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

  /** An empty H2 database of the given type. */
  static DatabaseMeta database(String type) {
    String name =
        "mem:insupd" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    return new DatabaseMeta("h2", type, "Native", "", name, "", "sa", "");
  }

  /** An H2 database of the given type with the target table, holding the odd ids up to n. */
  static DatabaseMeta target(String type, int n) throws Exception {
    DatabaseMeta databaseMeta = database(type);
    InsertUpdateRun.createTarget(databaseMeta, n);
    return databaseMeta;
  }

  static DatabaseMeta target(int n) throws Exception {
    return target("H2", n);
  }

  static List<Object> repeat(Object value, int times) {
    return Collections.nCopies(times, value);
  }

  /**
   * lines input (looked up), output (inserted), updated, skipped (nothing to update), written (to
   * the main output), rejected (to the error hop).
   */
  static List<Long> counters(
      long input, long inserted, long updated, long skipped, long written, long rejected) {
    return List.of(input, inserted, updated, skipped, written, rejected);
  }

  /** Every error row says one error, and names no fields. */
  static void assertNoErrorFields(Result r) {
    for (Object nrErrors : r.errorField(NR_ERRORS)) {
      assertEquals(1L, nrErrors);
    }
    for (Object fields : r.errorField(ERROR_FIELDS)) {
      assertNull(fields);
    }
  }

  /** The default path, and HOP_DATABASE_BATCH_RECOVERY=LEGACY. */
  static Stream<String> recoveries() {
    return Stream.of(null, "LEGACY");
  }

  /**
   * 1 r1 (update), 2 r2 (insert), 3 x (found, nothing to update), 4 bad (insert that fails), 5 r5
   * (update), 6 r6 (insert), 7 bad (update that fails), 9 x (found, nothing to update).
   */
  static List<Object[]> mixedRows() {
    return List.of(
        row(1, "r1"),
        row(2, "r2"),
        row(3, InsertUpdateRun.SEED),
        row(4, InsertUpdateRun.BAD),
        row(5, "r5"),
        row(6, "r6"),
        row(7, InsertUpdateRun.BAD),
        row(9, InsertUpdateRun.SEED));
  }

  /** The table after {@link #mixedRows()} on a table that held the odd ids up to 10. */
  static List<String> mixedTable() {
    return List.of("1:r1", "2:r2", "3:x", "5:r5", "6:r6", "7:x", "9:x");
  }

  // ----------------------------------------------------------------------------------------------
  // With an error hop
  // ----------------------------------------------------------------------------------------------

  static Stream<Arguments> rowByRowCommits() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      arguments.add(Arguments.of(recovery, 1, ids()));
      // Every third insert or update that succeeds commits: rows 4 and 8, with the rows before them
      arguments.add(Arguments.of(recovery, 3, ids(1, 2, 5, 7, 9, 10)));
      // One commit at the end
      arguments.add(Arguments.of(recovery, 100, idsWithout(10, 3, 6)));
    }
    return arguments.stream();
  }

  /**
   * Buggy: a row is passed on as soon as its insert or update ran, before the commit that the
   * commit size asks for. Row 3 is a failed update and row 6 a failed insert: each goes to the
   * error hop on its own, and the rows after it are written.
   */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("rowByRowCommits")
  void errorHop_Buggy_rowsLeaveBeforeTheirCommit(
      String recovery, int commitSize, List<Object> passedOnBeforeTheirCommit) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, 3, 6));

    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(repeat("ISU001", 2), r.errorCodes());
    assertEquals(repeat(ROW, 2), r.errorKinds());
    assertNoErrorFields(r);
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertEquals(counters(10, 4, 4, 0, 8, 2), r.counters());
    assertEquals(0, r.errorCount);
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1, 2, 2, 2, 2, 2), r.errorSizeAtRead);
    assertEquals(passedOnBeforeTheirCommit, r.passedOnBeforeTheirCommit());
  }

  /**
   * A row that needs no change is passed on at once and counted as skipped; it writes nothing, so
   * it does not count towards the commit size: with commit size 3 the third write (row 5) commits.
   * Buggy, as above: rows 1, 2 and 6 are passed on before their commit.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void errorHop_Buggy_rowsThatNeedNoChangeLeaveAtOnce(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(mixedRows());

    assertEquals(ids(1, 2, 3, 5, 6, 9), r.outputKeys());
    assertEquals(ids(4, 7), r.errorKeys());
    assertEquals(repeat("ISU001", 2), r.errorCodes());
    assertEquals(repeat(ROW, 2), r.errorKinds());
    assertEquals(mixedTable(), InsertUpdateRun.table(db));
    assertEquals(counters(8, 2, 2, 2, 6, 2), r.counters());
    assertEquals(List.of(0, 1, 2, 3, 3, 4, 5, 5, 6), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 2, 2), r.errorSizeAtRead);
    assertEquals(ids(1, 2, 6), r.passedOnBeforeTheirCommit());
  }

  /**
   * The lookup runs in the transaction that holds the earlier inserts, so a key that comes twice is
   * inserted once and then updated, and a third time needs no change.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void aKeyThatComesTwiceIsInsertedOnceThenUpdated(String recovery) throws Exception {
    DatabaseMeta db = database("H2");
    InsertUpdateRun.createTarget(db, 1L);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(100)
            .run(
                List.of(
                    row(2, "first"),
                    row(2, "second"),
                    row(1, "r1"),
                    row(2, "second"),
                    row(1, "r1")));

    assertEquals(ids(2, 2, 1, 2, 1), r.outputKeys());
    assertEquals(ids(), r.errorKeys());
    assertEquals(List.of("1:r1", "2:second"), InsertUpdateRun.table(db));
    assertEquals(counters(5, 1, 2, 2, 5, 0), r.counters());
  }

  /** code (String), name (String). */
  static IRowMeta codeNameRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("code"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  /** Looks up table ci_target by code = code, and inserts or updates its name. */
  static void byCode(InsertUpdateMeta meta) {
    InsertUpdateLookupField lookup = meta.getInsertUpdateLookupField();
    lookup.setTableName("ci_target");
    lookup.getLookupKeys().clear();
    lookup.getLookupKeys().add(new InsertUpdateKeyField("code", "code", "="));
    lookup.getValueFields().clear();
    lookup.getValueFields().add(new InsertUpdateValue("code", "code", false));
    lookup.getValueFields().add(new InsertUpdateValue("name", "name", true));
  }

  /** A table whose key column compares without case. */
  static DatabaseMeta caseInsensitiveTarget() throws Exception {
    DatabaseMeta db = database("H2");
    InsertUpdateRun.execute(
        db, "CREATE TABLE ci_target (code VARCHAR_IGNORECASE(20) PRIMARY KEY, name VARCHAR(20))");
    return db;
  }

  /** On a key column without case, "a" finds the "A" inserted just before it, and updates it. */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void aKeyThatComesTwiceInAnotherCaseIsUpdatedOnACaseInsensitiveColumn(String recovery)
      throws Exception {
    DatabaseMeta db = caseInsensitiveTarget();

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(100)
            .configure(InsertUpdateBatchBehaviourH2Test::byCode)
            .run(
                codeNameRowMeta(),
                List.of(
                    new Object[] {"A", "first"},
                    new Object[] {"a", "second"},
                    new Object[] {"B", "b"}));

    assertEquals(List.of("A", "a", "B"), r.outputKeys());
    assertEquals(
        List.of("A:second", "B:b"),
        InsertUpdateRun.query(db, "SELECT code, name FROM ci_target ORDER BY code"));
    assertEquals(counters(3, 2, 1, 0, 3, 0), r.counters());
  }

  /**
   * "Don't perform any updates": a row that is found is skipped and passed on as it is, whatever
   * its values; only the missing ids are inserted.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void updatesBypassed_foundRowsAreSkipped(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(3)
            .configure(meta -> meta.setUpdateBypassed(true))
            .run(rows(10, 6));

    assertEquals(idsWithout(10, 6), r.outputKeys());
    assertEquals(ids(6), r.errorKeys());
    assertEquals(List.of("ISU001"), r.errorCodes());
    assertEquals(tableWith(10, id -> id % 2 == 0 && id != 6), InsertUpdateRun.table(db));
    assertEquals(counters(10, 4, 0, 5, 9, 1), r.counters());
    assertEquals(List.of(0, 1, 2, 3, 4, 5, 5, 6, 7, 8, 9), r.outputSizeAtRead);
  }

  /**
   * Commit size 0 is auto-commit: every row is committed on its own, and passed on right after it.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void commitSizeZero_everyRowIsCommittedOnItsOwn(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(0)
            .watchCommits()
            .run(rows(10, 3, 6));

    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(repeat(ROW, 2), r.errorKinds());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertEquals(counters(10, 4, 4, 0, 8, 2), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // Without an error hop
  // ----------------------------------------------------------------------------------------------

  /**
   * Buggy: without an error hop a failed row stops the transform and the transaction is rolled
   * back. Rows 1-4 were committed by the commit size of 4; rows 5 and 6, already passed on, are
   * rolled back with the transaction and not in the table.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void noErrorHop_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(String recovery)
      throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(false)
            .commitSize(4)
            .watchCommits()
            .run(rows(10, 7));

    assertEquals(idsWithout(6), r.outputKeys());
    // Rows 1-3 are committed later, with row 4; rows 5 and 6 never
    assertEquals(ids(1, 2, 3, 5, 6), r.passedOnBeforeTheirCommit());
    assertEquals(tableWith(10, id -> id <= 4), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(7, 3, 3, 0, 6, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.contains("Error inserting/updating row")));
  }

  /** A failed insert stops the transform the same way. */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void noErrorHop_aFailedInsertStopsTheTransform(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db).recovery(recovery).errorHop(false).commitSize(4).run(rows(10, 6));

    assertEquals(idsWithout(5), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(6, 2, 3, 0, 5, 0), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // batchComplete(), dispose(), a stopped or failed transform, a transactional pipeline
  // ----------------------------------------------------------------------------------------------

  /**
   * At the end of a single-threaded iteration the open transaction is committed; the rows were
   * passed on right after their insert or update already.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchComplete_commitsTheOpenTransaction(String recovery) throws Exception {
    DatabaseMeta db = target(5);
    List<List<String>> tableAtBatchComplete = new ArrayList<>();

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    tableAtBatchComplete.add(InsertUpdateRun.table(db));
                  }
                })
            .run(rows(5, 2));

    assertEquals(List.of(tableWith(5, id -> id == 1 || id == 3)), tableAtBatchComplete);
    assertEquals(List.of(0, 1, 1, 2, 3, 4), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 1, 1, 1, 1), r.errorSizeAtRead);
    assertEquals(ids(1, 3, 4, 5), r.outputKeys());
    assertEquals(ids(2), r.errorKeys());
    assertEquals(tableWrittenExcept(5, 2), InsertUpdateRun.table(db));
    assertEquals(counters(5, 1, 3, 0, 4, 1), r.counters());
  }

  /**
   * A transform that is stopped commits what it wrote when it is disposed: with commit size 5,
   * stopped after row 7, rows 1-7 are passed on and all seven are in the table.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void stopped_everyRowPassedOnIsCommitted(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
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
    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 7), InsertUpdateRun.table(db));
    assertEquals(counters(7, 3, 4, 0, 7, 0), r.counters());
  }

  /**
   * Buggy: a transform that failed rolls back what was not committed yet, although those rows were
   * passed on. With commit size 5, failed after row 7: rows 1-7 are passed on, rows 1-5 are in the
   * table.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void failed_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
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

    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 5), InsertUpdateRun.table(db));
    assertEquals(counters(7, 3, 4, 0, 7, 0), r.counters());
  }

  /**
   * On a connection shared by a transactional pipeline the commit only comes at the end of the
   * pipeline; the rows are passed on right after their insert or update.
   */
  @Test
  void transactionalPipeline_rowsLeaveAtOnce() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .errorHop(true)
            .commitSize(100)
            .connectionGroup("insert-update-" + UUID.randomUUID())
            .run(rows(10, 3, 6));

    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertEquals(counters(10, 4, 4, 0, 8, 2), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // A dialect that runs every row behind a savepoint (PostgreSQL and its look-alikes)
  // ----------------------------------------------------------------------------------------------

  static Stream<Arguments> savepointCommits() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      arguments.add(Arguments.of(recovery, 1, ids()));
      arguments.add(Arguments.of(recovery, 3, ids(1, 2, 5, 7, 9, 10)));
      arguments.add(Arguments.of(recovery, 100, idsWithout(10, 3, 6)));
    }
    return arguments.stream();
  }

  /**
   * With an error hop every row runs behind a savepoint, so only the failed rows go to the error
   * hop. Buggy: every row leaves right after its savepoint section, before the commit that includes
   * it.
   */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("savepointCommits")
  void savepointDialect_errorHop_Buggy_rowsLeaveBeforeTheirCommit(
      String recovery, int commitSize, List<Object> passedOnBeforeTheirCommit) throws Exception {
    DatabaseMeta db = target("H2_INSUPD_SAVEPOINTS", 10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, 3, 6));

    assertTrue(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(repeat("ISU001", 2), r.errorCodes());
    assertEquals(repeat(ROW, 2), r.errorKinds());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertEquals(counters(10, 4, 4, 0, 8, 2), r.counters());
    assertEquals(passedOnBeforeTheirCommit, r.passedOnBeforeTheirCommit());
  }

  /**
   * Behind a savepoint the transform commits itself, and counts every row that went through,
   * including the rows that needed no change: with commit size 3 rows 3 and 9 commit. Buggy: rows
   * 1, 2, 5 and 6 are passed on before their commit.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void savepointDialect_errorHop_Buggy_rowsThatNeedNoChangeCountTowardsTheCommit(String recovery)
      throws Exception {
    DatabaseMeta db = target("H2_INSUPD_SAVEPOINTS", 10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(mixedRows());

    assertTrue(r.useSavepoints);
    assertEquals(ids(1, 2, 3, 5, 6, 9), r.outputKeys());
    assertEquals(ids(4, 7), r.errorKeys());
    assertEquals(mixedTable(), InsertUpdateRun.table(db));
    assertEquals(counters(8, 2, 2, 2, 6, 2), r.counters());
    assertEquals(List.of(0, 1, 2, 3, 3, 4, 5, 5, 6), r.outputSizeAtRead);
    assertEquals(ids(1, 2, 5, 6), r.passedOnBeforeTheirCommit());
  }

  /** Without an error hop no savepoints are used: a failed row stops the transform. */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void savepointDialect_noErrorHop_noSavepoints(String recovery) throws Exception {
    DatabaseMeta db = target("H2_INSUPD_SAVEPOINTS", 10);

    Result r =
        InsertUpdateRun.on(db).recovery(recovery).errorHop(false).commitSize(4).run(rows(10, 7));

    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(6), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
  }

  /** Commit size 0 is auto-commit: no savepoints either, and every row leaves after its commit. */
  @Test
  void savepointDialect_autoCommit_noSavepoints() throws Exception {
    DatabaseMeta db = target("H2_INSUPD_SAVEPOINTS", 10);

    Result r =
        InsertUpdateRun.on(db).errorHop(true).commitSize(0).watchCommits().run(rows(10, 3, 6));

    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
  }
}

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

import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.BATCH;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.DRIVER;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.ERROR_FIELDS;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.NOT_ATTEMPTED;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.NOT_FOUND;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.ROW;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.bad;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.delete;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.insert;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.labels;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.nothing;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.rows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.engines.local.transaction.SharedConnectionRun;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pins what Synchronize after merge does with its batches and an error hop, end to end against H2:
 * with and without batch updates, with and without an error hop, for several commit sizes, at the
 * end of a single-threaded iteration and when the transform is stopped, and on dialects with the
 * flags of PostgreSQL and Oracle.
 *
 * <p>Each test checks the rows on the main output (values and order), the rows on the error hop
 * (error codes, fields and the kind of message), what the table holds after the run, the counters,
 * and for the important cases when the rows left the transform and whether what they did was
 * committed by then. These are characterization tests: they describe current behaviour, also where
 * it is wrong. Those tests have Buggy in their name and say so.
 *
 * <p>How the transform runs its three statements in batch mode today: every row is added to the
 * JDBC batch of the INSERT, UPDATE or DELETE of its table. A batch runs, and is committed, when
 *
 * <ul>
 *   <li>the database counts {@code commit size} rows added to any of the batches: the batch of the
 *       row that makes the count runs, the others don't;
 *   <li>the statement has taken {@code commit size + 1} rows since its batch last ran this way;
 *   <li>the input ends, or a single-threaded iteration ends: every batch runs, in the order in
 *       which a Hashtable of "table name + statement" happens to list them.
 * </ul>
 *
 * <p>Rows are passed on when one of the statements commits by its own count, all the rows waiting
 * by then whatever statement they are for; a skipped row is passed on at once. H2, like SQL Server
 * and MySQL, runs the whole batch and marks the failed rows with {@code EXECUTE_FAILED}.
 *
 * <p>With an error hop H2 switches batching off: its {@code
 * IsSupportsErrorHandlingOnBatchUpdates()} is true, which the transform reads as "can't handle a
 * failed batch". The dialects that answer false (PostgreSQL, Oracle) keep batching with an error
 * hop. The H2 subclasses below have those flags; they declare no batch failure contract, so they
 * keep today's handling.
 *
 * <p>Since #9768 H2's validated batch failure contract recovers failed batches with
 * RecoverableBatch when batch updates are on, also with an error hop: see
 * SynchronizeAfterMergeRecoveredBatchH2Test. The tests here whose name says legacy run with
 * HOP_DATABASE_BATCH_RECOVERY=LEGACY and pin the handling as it was; the others give the same on
 * both paths.
 */
class SynchronizeAfterMergeBatchBehaviourH2Test {

  /** H2 with PostgreSQL's flags: a savepoint per row with an error hop, and batching kept on. */
  @DatabaseMetaPlugin(type = "H2_SAM_POSTGRES_FLAGS", typeDescription = "H2, PostgreSQL flags")
  public static class H2PostgresFlagsDatabaseMeta extends H2DatabaseMeta {
    @Override
    public boolean isUseSafePoints() {
      return true;
    }

    @Override
    public boolean IsSupportsErrorHandlingOnBatchUpdates() {
      return false;
    }
  }

  /** H2 with Oracle's flags: batching kept on with an error hop, no savepoints. */
  @DatabaseMetaPlugin(type = "H2_SAM_ORACLE_FLAGS", typeDescription = "H2, Oracle flags")
  public static class H2OracleFlagsDatabaseMeta extends H2DatabaseMeta {
    @Override
    public boolean IsSupportsErrorHandlingOnBatchUpdates() {
      return false;
    }
  }

  /** H2 that asks for a savepoint per row with an error hop, and turns batching off. */
  @DatabaseMetaPlugin(type = "H2_SAM_SAVEPOINTS", typeDescription = "H2 with savepoints")
  public static class H2SavepointsDatabaseMeta extends H2DatabaseMeta {
    @Override
    public boolean isUseSafePoints() {
      return true;
    }
  }

  static final String POSTGRES_FLAGS = "H2_SAM_POSTGRES_FLAGS";
  static final String ORACLE_FLAGS = "H2_SAM_ORACLE_FLAGS";
  static final String SAVEPOINTS = "H2_SAM_SAVEPOINTS";

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2PostgresFlagsDatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2OracleFlagsDatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2SavepointsDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    SharedConnectionRun.registerConnectionGroupExtensionPoint();
  }

  /** A new H2 database of the given type, without tables. */
  static DatabaseMeta database(String type) {
    String name = "mem:sam" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    return new DatabaseMeta("h2", type, "Native", "", name, "", "sa", "");
  }

  /** An H2 database of the given type with the target table, holding the given ids named x. */
  static DatabaseMeta target(String type, long... ids) throws Exception {
    DatabaseMeta databaseMeta = database(type);
    SynchronizeAfterMergeRun.createTarget(databaseMeta, SynchronizeAfterMergeRun.TARGET, ids);
    return databaseMeta;
  }

  /** The default path, and HOP_DATABASE_BATCH_RECOVERY=LEGACY. */
  static Stream<String> recoveries() {
    return Stream.of(null, "LEGACY");
  }

  /**
   * lines input (lookups), output (inserts), updated (updates and deletes), skipped, written (to
   * the main output), rejected (to the error hop).
   */
  static List<Long> counters(
      long input, long output, long updated, long skipped, long written, long rejected) {
    return List.of(input, output, updated, skipped, written, rejected);
  }

  /**
   * Ten rows on a table holding ids 1-6, of which 5 has a child row: update 1, insert 7, insert 2
   * (a duplicate key), a row to skip (8), delete 3, update 4 to the name the table refuses, delete
   * 5 (the child row refers to it), insert 9, update 6, insert 10. Three rows fail: i2, u4 and d5.
   */
  static List<Object[]> mixedRows() {
    return rows(
        update(1),
        insert(7),
        insert(2),
        nothing(8),
        delete(3),
        bad(update(4)),
        delete(5),
        insert(9),
        update(6),
        insert(10));
  }

  /** The table {@link #mixedRows()} start from: ids 1-6, a child row referring to 5. */
  static DatabaseMeta mixedTarget(String type) throws Exception {
    DatabaseMeta db = target(type, 1, 2, 3, 4, 5, 6);
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);
    return db;
  }

  /** The table after {@link #mixedRows()} when exactly i2, u4 and d5 fail. */
  static final List<String> MIXED_DONE =
      List.of("1:r1", "2:x", "4:x", "5:x", "6:r6", "7:r7", "9:r9", "10:r10");

  /** The table {@link #mixedRows()} start from. */
  static final List<String> MIXED_UNTOUCHED = List.of("1:x", "2:x", "3:x", "4:x", "5:x", "6:x");

  static final List<String> MIXED_WRITTEN = labels("u1", "i7", "s8", "d3", "i9", "u6", "i10");
  static final List<String> MIXED_FAILED = labels("i2", "u4", "d5");

  static List<Object> repeat(Object value, int times) {
    return Collections.nCopies(times, value);
  }

  static void assertNoErrorFields(Result r) {
    for (Object nrErrors : r.errorField(SynchronizeAfterMergeRun.NR_ERRORS)) {
      assertEquals(1L, nrErrors);
    }
    for (Object fields : r.errorField(ERROR_FIELDS)) {
      assertNull(fields);
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Batch updates off
  //
  // #9768 changes only how batches are run and recovered. Every test here runs on the default path
  // and with HOP_DATABASE_BATCH_RECOVERY=LEGACY, and both must give what main gives: row by row, a
  // row is passed on right after its statement, before the commit that includes it.
  // ----------------------------------------------------------------------------------------------

  static Stream<Arguments> rowByRowCommits() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      // The database commits every third statement that succeeds: d3, then i10
      arguments.add(Arguments.of(recovery, 3, labels("u1", "i7", "i9", "u6")));
      // One commit at the end
      arguments.add(Arguments.of(recovery, 100, labels("u1", "i7", "d3", "i9", "u6", "i10")));
    }
    return arguments.stream();
  }

  /**
   * Buggy: without batching a row is passed on as soon as its statement ran, before the commit that
   * the commit size asks for. A failed row goes to the error hop with SUYNC001.
   */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("rowByRowCommits")
  void batchOff_errorHop_Buggy_rowsLeaveBeforeTheirCommit(
      String recovery, int commitSize, List<String> passedOnBeforeTheirCommit) throws Exception {
    DatabaseMeta db = mixedTarget("H2");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(mixedRows());

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(repeat("SUYNC001", 3), r.errorCodes());
    assertEquals(repeat(ROW, 3), r.errorKinds());
    assertNoErrorFields(r);
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertEquals(0, r.errorCount);
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 4, 5, 6, 7), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1, 2, 3, 3, 3, 3), r.errorSizeAtRead);
    assertEquals(passedOnBeforeTheirCommit, r.passedOnBeforeTheirCommit());
  }

  /**
   * Buggy: without an error hop the first failed row (i2) stops the transform and rolls the
   * transaction back. u1 and i7, already passed on, are not in the table.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_noErrorHop_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(String recovery)
      throws Exception {
    DatabaseMeta db = mixedTarget("H2");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(false)
            .commitSize(100)
            .run(mixedRows());

    assertEquals(labels("u1", "i7"), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(0, 1, 1, 0, 2, 0), r.counters());
    assertTrue(r.loggedError("Error inserting row into table [target]"));
  }

  /**
   * Buggy: with "perform lookup" a key that is not in the table goes to the error hop with SUYNC001
   * and the key field named, but every such row gets the message of the first one: here both name
   * key 11.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_lookup_Buggy_everyMissingKeyNamesTheFirstOne(String recovery) throws Exception {
    DatabaseMeta db = target("H2", 1, 2, 3);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .lookup(true)
            .commitSize(100)
            .run(rows(update(1), update(11), update(2), delete(12), delete(3)));

    assertEquals(labels("u1", "u2", "d3"), r.outputLabels());
    assertEquals(labels("u11", "d12"), r.errorLabels());
    assertEquals(repeat("SUYNC001", 2), r.errorCodes());
    assertEquals(repeat(NOT_FOUND, 2), r.errorKinds());
    assertEquals(repeat("id", 2), r.errorField(ERROR_FIELDS));
    for (Object description : r.errorField(SynchronizeAfterMergeRun.ERROR_DESCRIPTIONS)) {
      assertTrue(((String) description).endsWith("[11]"), (String) description);
    }
    assertEquals(List.of("1:r1", "2:r2"), SynchronizeAfterMergeRun.table(db));
    // Every lookup counts as a line read, the found and the missing ones
    assertEquals(counters(5, 0, 3, 0, 3, 2), r.counters());
  }

  /**
   * A transform that is stopped commits the statements it ran when it is disposed: with commit size
   * 100, stopped after row 3, rows 1-3 are passed on and in the table.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_stopped_everyRowPassedOnIsCommitted(String recovery) throws Exception {
    DatabaseMeta db = target("H2", 1, 2, 3, 4, 5);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.stopAll();
                  }
                })
            .run(rows(update(1), insert(6), delete(2), update(3), update(4)));

    assertTrue(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(labels("u1", "i6", "d2"), r.outputLabels());
    assertEquals(List.of("1:r1", "3:x", "4:x", "5:x", "6:r6"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 1, 2, 0, 3, 0), r.counters());
  }

  /**
   * Buggy: a transform that failed rolls back what was not committed yet, although those rows were
   * passed on.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_failed_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(String recovery)
      throws Exception {
    DatabaseMeta db = target("H2", 1, 2, 3, 4, 5);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.setErrors(1);
                    transform.stopAll();
                  }
                })
            .run(rows(update(1), insert(6), delete(2), update(3), update(4)));

    assertEquals(labels("u1", "i6", "d2"), r.outputLabels());
    assertEquals(List.of("1:x", "2:x", "3:x", "4:x", "5:x"), SynchronizeAfterMergeRun.table(db));
  }

  /** At the end of a single-threaded iteration the open transaction is committed. */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOff_batchComplete_commitsTheOpenTransaction(String recovery) throws Exception {
    DatabaseMeta db = target("H2", 1, 2, 3);
    List<List<String>> tableAtBatchComplete = new ArrayList<>();

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 2) {
                    transform.batchComplete();
                    tableAtBatchComplete.add(SynchronizeAfterMergeRun.table(db));
                  }
                })
            .run(rows(update(1), insert(4), delete(2)));

    assertEquals(List.of(List.of("1:r1", "2:x", "3:x", "4:r4")), tableAtBatchComplete);
    assertEquals(labels("u1", "i4", "d2"), r.outputLabels());
    assertEquals(List.of("1:r1", "3:x", "4:r4"), SynchronizeAfterMergeRun.table(db));
  }

  static Stream<Arguments> commitSizeZeroCases() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      for (boolean batch : new boolean[] {true, false}) {
        arguments.add(Arguments.of(recovery, batch));
      }
    }
    return arguments.stream();
  }

  /**
   * Buggy: commit size 0 is auto-commit, no batches, every statement is committed on its own and
   * the row is passed on right after it. But the second row of a statement that succeeds, i9 here,
   * divides by the commit size: the transform fails with an ArithmeticException after i9 is in the
   * table, without passing it on.
   */
  @ParameterizedTest(name = "recovery {0}, batch {1}")
  @MethodSource("commitSizeZeroCases")
  void commitSizeZero_Buggy_theSecondRowOfAStatementDividesByZero(String recovery, boolean batch)
      throws Exception {
    DatabaseMeta db = mixedTarget("H2");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(batch)
            .errorHop(true)
            .commitSize(0)
            .watchCommits()
            .run(mixedRows());

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertInstanceOf(ArithmeticException.class, r.thrown);
    assertEquals(1, r.errorCount);
    assertEquals(labels("u1", "i7", "s8", "d3"), r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(repeat("SUYNC001", 3), r.errorCodes());
    assertEquals(labels(), r.passedOnBeforeTheirCommit());
    assertEquals(
        List.of("1:r1", "2:x", "4:x", "5:x", "6:x", "7:r7", "9:r9"),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 2, 2, 1, 4, 3), r.counters());
  }

  /**
   * Buggy: on a connection shared by a transactional pipeline the database runs every statement at
   * once instead of batching it, but the transform still holds the rows back as if they were in a
   * batch, and passes them on when the input ends; the skipped row leaves at once. The commit only
   * comes at the end of the pipeline.
   */
  @Test
  void batchOn_transactionalPipeline_Buggy_rowsWaitForBatchesThatAreNotThere() throws Exception {
    DatabaseMeta db = target("H2", 1, 2, 3);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .connectionGroup("sam-" + UUID.randomUUID())
            .run(rows(update(1), nothing(9), insert(4), delete(2)));

    assertTrue(r.batchMode);
    assertEquals(labels("s9", "u1", "i4", "d2"), r.outputLabels());
    assertEquals(List.of(0, 0, 1, 1, 1), r.outputSizeAtRead);
    assertEquals(List.of("1:r1", "3:x", "4:r4"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 1, 2, 1, 4, 0), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // Batch updates on, H2 with an error hop: batching is switched off
  // ----------------------------------------------------------------------------------------------

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: with an error hop H2 switches
   * batching off and logs it: the rows run one by one exactly as with batch updates off, and leave
   * before their commit (Buggy, as there). See SynchronizeAfterMergeRecoveredBatchH2Test for the
   * default path.
   */
  @Test
  void batchOn_errorHop_legacy_Buggy_batchingIsSwitchedOff() throws Exception {
    DatabaseMeta db = mixedTarget("H2");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(mixedRows());

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertTrue(r.logged("Batch mode is disabled!"));
    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(repeat("SUYNC001", 3), r.errorCodes());
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 4, 5, 6, 7), r.outputSizeAtRead);
    assertEquals(labels("u1", "i7", "i9", "u6"), r.passedOnBeforeTheirCommit());
  }

  // ----------------------------------------------------------------------------------------------
  // Batch updates on, without an error hop
  // ----------------------------------------------------------------------------------------------

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY, the rows of other statements
   * leave with the batch that runs: with commit size 2 the second update makes the database count
   * two rows and runs the update batch, the fourth row does it again, and the third update is the
   * statement's third row since its last commit, which commits and passes every waiting row on: i1
   * as well, whose insert has not run yet. It only runs at the end of the input. See
   * SynchronizeAfterMergeRecoveredBatchH2Test for the default path.
   */
  @Test
  void batchOn_noErrorHop_legacy_Buggy_rowsLeaveWithTheBatchOfAnotherStatement() throws Exception {
    DatabaseMeta db = target("H2", 2, 3, 4);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(false)
            .commitSize(2)
            .watchCommits()
            .run(rows(insert(1), update(2), update(3), update(4), insert(5)));

    assertTrue(r.batchMode);
    assertEquals(labels("i1", "u2", "u3", "u4", "i5"), r.outputLabels());
    assertEquals(List.of(0, 0, 0, 0, 4, 4), r.outputSizeAtRead);
    assertEquals(labels("i1"), r.passedOnBeforeTheirCommit());
    assertEquals(
        List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 2, 3, 0, 5, 0), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: a skipped row is passed on at
   * once, ahead of the rows that wait for their batch; and once a batch waits, every {@code commit
   * size + 1}-th skipped row passes the waiting rows on without running any batch: here i1 leaves
   * with the third skipped row, before its batch runs at the end of the input. (u2 is committed by
   * then: the database ran the update batch when it counted two rows.) See
   * SynchronizeAfterMergeRecoveredBatchH2Test for the default path.
   */
  @Test
  void batchOn_noErrorHop_legacy_Buggy_skippedRowsJumpTheQueueAndReleaseIt() throws Exception {
    DatabaseMeta db = target("H2", 2);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(false)
            .commitSize(2)
            .watchCommits()
            .run(rows(insert(1), update(2), nothing(7), nothing(8), nothing(9), insert(3)));

    assertEquals(labels("s7", "s8", "i1", "u2", "s9", "i3"), r.outputLabels());
    assertEquals(labels("i1"), r.passedOnBeforeTheirCommit());
    assertEquals(List.of("1:r1", "2:r2", "3:r3"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 2, 1, 3, 6, 0), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: a failed batch at the end of the
   * input stops the transform. The update batch runs first on this table and fails on u4; the other
   * batches never run. Only the skipped row, passed on at once, reached the output, and all five
   * updates and deletes are counted as updated. See SynchronizeAfterMergeRecoveredBatchH2Test for
   * the default path.
   */
  @Test
  void batchOn_noErrorHop_legacy_Buggy_aFailedBatchCountsRowsThatWereNeverWritten()
      throws Exception {
    DatabaseMeta db = mixedTarget("H2");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .run(mixedRows());

    assertTrue(r.batchMode);
    assertEquals(labels("s8"), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(0, 0, 5, 1, 1, 0), r.counters());
    assertTrue(r.loggedError("Unexpected batch update error committing the database connection"));
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: with commit size 3 the database
   * runs the insert batch at the third row added, i2, which is a duplicate: the transaction is
   * rolled back and the transform stops; nothing is passed on, but u1, whose batch never ran, is
   * counted as updated. See SynchronizeAfterMergeRecoveredBatchH2Test for the default path.
   */
  @Test
  void batchOn_noErrorHop_legacy_Buggy_aBatchThatFailsInTheMiddleCountsAnUpdateNeverWritten()
      throws Exception {
    DatabaseMeta db = mixedTarget("H2");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(false)
            .commitSize(3)
            .run(mixedRows());

    assertEquals(labels(), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(List.of(0, 0, 0), r.outputSizeAtRead);
    assertEquals(counters(0, 0, 1, 0, 0, 0), r.counters());
  }

  /**
   * Without failures every row is written; the last partial batches run at the end of the input.
   */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("commitSizes")
  void batchOn_noErrorHop_withoutFailuresEveryRowIsWritten(String recovery, int commitSize)
      throws Exception {
    DatabaseMeta db = target("H2", range(1, 10));

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(false)
            .commitSize(commitSize)
            .run(manyRows());

    assertEquals(manyLabels(), r.outputLabels().stream().sorted().toList());
    assertEquals(MANY_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(0, r.errorCount);
    assertEquals(counters(0, 5, 10, 5, 20, 0), r.counters());
  }

  static Stream<Arguments> commitSizes() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      for (int commitSize : new int[] {1, 2, 3, 7, 100}) {
        arguments.add(Arguments.of(recovery, commitSize));
      }
    }
    return arguments.stream();
  }

  static long[] range(long from, long to) {
    return SynchronizeAfterMergeRun.range(from, to);
  }

  /**
   * Twenty rows on a table holding 1-10: update 1-5, delete 6-10, insert 11-15, and five rows to
   * skip, interleaved.
   */
  static List<Object[]> manyRows() {
    List<Object[]> rows = new ArrayList<>();
    for (long i = 1; i <= 5; i++) {
      rows.add(update(i));
      rows.add(insert(10 + i));
      rows.add(delete(5 + i));
      rows.add(nothing(20 + i));
    }
    return rows;
  }

  static List<String> manyLabels() {
    return manyRows().stream().map(SynchronizeAfterMergeRun::label).sorted().toList();
  }

  static final List<String> MANY_DONE =
      List.of(
          "1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "11:r11", "12:r12", "13:r13", "14:r14", "15:r15");

  /** Updates 1-10 on a table holding 1-10, the given ones to the name the table refuses. */
  static List<Object[]> updates(long... bad) {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 10; id++) {
      boolean refused = false;
      for (long b : bad) {
        refused |= b == id;
      }
      rows.add(refused ? bad(update(id)) : update(id));
    }
    return rows;
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: ten updates with commit size 4,
   * u9 refused. The database runs the batch at u4 and u8, the transform at u5, which passes u1-u5
   * on; at u10 the transform runs the last batch, u9 and u10, which fails: the transform stops.
   * u6-u8 are committed, but never passed on. See SynchronizeAfterMergeRecoveredBatchH2Test for the
   * default path.
   */
  @Test
  void batchOn_noErrorHop_legacy_Buggy_aFailedBatchLeavesCommittedRowsUnreported()
      throws Exception {
    DatabaseMeta db = target("H2", range(1, 10));

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(false)
            .commitSize(4)
            .run(updates(9));

    assertEquals(labels("u1", "u2", "u3", "u4", "u5"), r.outputLabels());
    assertEquals(
        List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "6:r6", "7:r7", "8:r8", "9:x", "10:x"),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(0, 0, 10, 0, 5, 0), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // batchComplete() and dispose()
  // ----------------------------------------------------------------------------------------------

  /**
   * At the end of a single-threaded iteration every batch runs, is committed and its rows are
   * passed on, although none is full.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOn_batchComplete_runsCommitsAndPassesOnThePendingBatches(String recovery)
      throws Exception {
    DatabaseMeta db = target("H2", 1, 2, 3);
    List<List<String>> tableAtBatchComplete = new ArrayList<>();

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .watchCommits()
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    tableAtBatchComplete.add(SynchronizeAfterMergeRun.table(db));
                  }
                })
            .run(rows(update(1), insert(4), delete(2), update(3)));

    assertEquals(List.of(List.of("1:r1", "3:x", "4:r4")), tableAtBatchComplete);
    assertEquals(List.of(0, 0, 0, 3, 3), r.outputSizeAtRead);
    assertEquals(labels("u1", "i4", "d2", "u3"), r.outputLabels());
    assertEquals(labels(), r.passedOnBeforeTheirCommit());
    assertEquals(List.of("1:r1", "3:r3", "4:r4"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 1, 3, 0, 4, 0), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: a transform that is stopped with
   * rows in its batches runs and commits them when it is disposed, but a stopped transform passes
   * no rows on: they are in the table, and the insert is counted as output, yet none was passed on.
   * See SynchronizeAfterMergeRecoveredBatchH2Test for the default path.
   */
  @Test
  void batchOn_stopped_legacy_Buggy_thePendingBatchesAreCommittedButNeverPassedOn()
      throws Exception {
    DatabaseMeta db = target("H2", 1, 2, 3);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.stopAll();
                  }
                })
            .run(rows(update(1), insert(4), delete(2), update(3)));

    assertTrue(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(labels(), r.outputLabels());
    assertEquals(List.of("1:r1", "3:x", "4:r4"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 1, 2, 0, 0, 0), r.counters());
  }

  /** A transform that failed rolls back the rows of its batches when it is disposed. */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void batchOn_failed_thePendingBatchesAreRolledBack(String recovery) throws Exception {
    DatabaseMeta db = target("H2", 1, 2, 3);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.setErrors(1);
                    transform.stopAll();
                  }
                })
            .run(rows(update(1), insert(4), delete(2), update(3)));

    assertEquals(labels(), r.outputLabels());
    assertEquals(List.of("1:x", "2:x", "3:x"), SynchronizeAfterMergeRun.table(db));
  }

  // ----------------------------------------------------------------------------------------------
  // Batching with an error hop: the flags of Oracle and PostgreSQL
  // ----------------------------------------------------------------------------------------------

  /**
   * Buggy: with Oracle's flags batching stays on with an error hop. With commit size 3 the database
   * runs the insert batch (i7, i2) at the third row added; H2 marks i2 as failed. The other batches
   * are cleared, so u1 never runs, and the update counts of the insert batch are laid over the
   * waiting rows of all three statements: u1 goes to the output although it never ran, i7 goes to
   * the error hop although it is committed, and i2 gets the message of the whole batch. Then u4 and
   * d5 fail on their own when their batches run at the end of the input.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void oracleFlags_batchOn_errorHop_Buggy_aFailedBatchIsAttributedByPosition(String recovery)
      throws Exception {
    DatabaseMeta db = mixedTarget(ORACLE_FLAGS);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(mixedRows());

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertFalse(r.logged("Batch mode is disabled!"));
    assertEquals(labels("u1", "s8", "d3", "i9", "u6", "i10"), r.outputLabels());
    assertEquals(labels("i7", "i2", "u4", "d5"), r.errorLabels());
    assertEquals(repeat("SUYNC002", 4), r.errorCodes());
    assertEquals(List.of(DRIVER, BATCH, DRIVER, BATCH), r.errorKinds());
    assertEquals(
        List.of("1:x", "2:x", "4:x", "5:x", "6:r6", "7:r7", "9:r9", "10:r10"),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(labels("u1"), r.passedOnBeforeTheirCommit());
    // u1 counted as an insert, a row on the output
    assertEquals(counters(0, 4, 4, 1, 6, 4), r.counters());
  }

  /**
   * Buggy: the same ten updates with Oracle's flags and an error hop. u1-u5 leave as before. The
   * last batch, u9 and u10, fails; H2 runs u10 and marks u9. Its two counts are laid over the rows
   * waiting since u5, u6 to u10, although u6-u8 ran in the batch before: u6 goes to the error hop
   * with u9's message, u7 to the output, u8 to the error hop with the batch's message, u9 and u10
   * as never sent, SUYNC003. In the table u6, u7, u8 and u10 are updated.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void oracleFlags_batchOn_errorHop_Buggy_theCountsOfTheLastBatchAreLaidOverEarlierRows(
      String recovery) throws Exception {
    DatabaseMeta db = target(ORACLE_FLAGS, range(1, 10));

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(updates(9));

    assertEquals(labels("u1", "u2", "u3", "u4", "u5", "u7"), r.outputLabels());
    assertEquals(labels("u6", "u8", "u9", "u10"), r.errorLabels());
    assertEquals(List.of("SUYNC002", "SUYNC002", "SUYNC003", "SUYNC003"), r.errorCodes());
    assertEquals(List.of(DRIVER, BATCH, NOT_ATTEMPTED, NOT_ATTEMPTED), r.errorKinds());
    assertEquals(
        List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "6:r6", "7:r7", "8:r8", "9:x", "10:r10"),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(0, r.errorCount);
    // u7 counted as an insert
    assertEquals(counters(0, 1, 10, 0, 6, 4), r.counters());
  }

  /**
   * Buggy: the same with one commit at the end of the input. The update batch runs first on this
   * table and H2 marks u4; its three counts are laid over the first three waiting rows (u1, i7, i2)
   * and every row after them is reported as never sent, SUYNC003, although the insert and delete
   * batches were simply not run. Only u1 and u6 are written; i2, a duplicate, is reported on the
   * output and counted as inserted.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void oracleFlags_batchOn_errorHop_Buggy_theCountsOfOneStatementDecideForAllRows(String recovery)
      throws Exception {
    DatabaseMeta db = mixedTarget(ORACLE_FLAGS);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .run(mixedRows());

    assertEquals(labels("s8", "u1", "i2"), r.outputLabels());
    assertEquals(labels("i7", "d3", "u4", "d5", "i9", "u6", "i10"), r.errorLabels());
    assertEquals(
        List.of("SUYNC002", "SUYNC002", "SUYNC003", "SUYNC003", "SUYNC003", "SUYNC003", "SUYNC003"),
        r.errorCodes());
    assertEquals(
        List.of(
            DRIVER,
            BATCH,
            NOT_ATTEMPTED,
            NOT_ATTEMPTED,
            NOT_ATTEMPTED,
            NOT_ATTEMPTED,
            NOT_ATTEMPTED),
        r.errorKinds());
    assertEquals(
        List.of("1:r1", "2:x", "3:x", "4:x", "5:x", "6:r6"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 2, 5, 1, 3, 7), r.counters());
    assertEquals(0, r.errorCount);
  }

  /**
   * Buggy: with Oracle's flags and "perform lookup", a key that is not in the table goes to the
   * error hop at once with SUYNC002, the code of a row refused in a batch, without the key field;
   * the rows waiting in front of it come later.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void oracleFlags_batchOn_lookup_Buggy_aMissingKeyIsReportedAsARefusedBatchRow(String recovery)
      throws Exception {
    DatabaseMeta db = target(ORACLE_FLAGS, 1, 2, 3);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(true)
            .lookup(true)
            .commitSize(100)
            .run(rows(update(1), update(11), update(2), delete(3)));

    assertEquals(labels("u1", "u2", "d3"), r.outputLabels());
    assertEquals(labels("u11"), r.errorLabels());
    assertEquals(List.of("SUYNC002"), r.errorCodes());
    assertEquals(List.of(NOT_FOUND), r.errorKinds());
    assertNoErrorFields(r);
    assertEquals(List.of(0, 0, 0, 0, 0), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 1, 1, 1), r.errorSizeAtRead);
    assertEquals(List.of("1:r1", "2:r2"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(4, 0, 3, 0, 3, 1), r.counters());
  }

  /**
   * Buggy: with PostgreSQL's flags an error hop keeps batching on and adds a savepoint per row: the
   * rows run at once, one by one, but wait in the batch buffer as if they were batched. A failed
   * row and a skipped row are passed on at once, ahead of the waiting rows, so the streams don't
   * keep the input order. Only the failed rows go to the error hop, with SUYNC002.
   */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("postgresFlagsCommitSizes")
  void postgresFlags_batchOn_errorHop_Buggy_aSavepointPerRowAndRowsOutOfOrder(
      String recovery, int commitSize) throws Exception {
    DatabaseMeta db = mixedTarget(POSTGRES_FLAGS);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(mixedRows());

    assertTrue(r.batchMode);
    assertTrue(r.useSavepoints);
    assertEquals(labels("s8", "u1", "i7", "d3", "i9", "u6", "i10"), r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(repeat("SUYNC002", 3), r.errorCodes());
    assertEquals(repeat(ROW, 3), r.errorKinds());
    assertNoErrorFields(r);
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1, 2, 3, 3, 3, 3), r.errorSizeAtRead);
    assertEquals(labels(), r.passedOnBeforeTheirCommit());
  }

  static Stream<Arguments> postgresFlagsCommitSizes() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      for (int commitSize : new int[] {3, 100}) {
        arguments.add(Arguments.of(recovery, commitSize));
      }
    }
    return arguments.stream();
  }

  /**
   * With PostgreSQL's flags and commit size 1 the transform commits at the second row of a
   * statement, as it counts commit size + 1 rows, and passes every waiting row on: u1 and u2 at u2.
   * The counter starts over, so u4 and i5 wait for the end of the input.
   */
  @Test
  void postgresFlags_batchOn_errorHop_commitSizeOne() throws Exception {
    DatabaseMeta db = target(POSTGRES_FLAGS, 1, 2, 3, 4);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(1)
            .run(rows(update(1), update(2), bad(update(3)), update(4), insert(5)));

    assertEquals(labels("u1", "u2", "u4", "i5"), r.outputLabels());
    assertEquals(labels("u3"), r.errorLabels());
    assertEquals(List.of(0, 0, 2, 2, 2, 2), r.outputSizeAtRead);
    assertEquals(
        List.of("1:r1", "2:r2", "3:x", "4:r4", "5:r5"), SynchronizeAfterMergeRun.table(db));
  }

  /** Without an error hop PostgreSQL's flags batch as usual, without savepoints. */
  @Test
  void postgresFlags_batchOn_noErrorHop_keepsBatching() throws Exception {
    DatabaseMeta db = mixedTarget(POSTGRES_FLAGS);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .run(mixedRows());

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(labels("s8"), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
  }

  /**
   * With a savepoint per row and batching that handles errors, an error hop switches batching off:
   * every row runs behind a savepoint and is passed on right after it, before its commit (Buggy).
   */
  @ParameterizedTest(name = "batch {0}")
  @ValueSource(booleans = {true, false})
  void savepoints_errorHop_Buggy_aSavepointPerRowRowsLeaveBeforeTheirCommit(boolean batch)
      throws Exception {
    DatabaseMeta db = mixedTarget(SAVEPOINTS);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(batch)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(mixedRows());

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertEquals(batch, r.logged("Batch mode is disabled!"));
    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(repeat("SUYNC001", 3), r.errorCodes());
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 4, 5, 6, 7), r.outputSizeAtRead);
    // The transform commits every fourth row of a statement, never reached here
    assertEquals(labels("u1", "i7", "d3", "i9", "u6", "i10"), r.passedOnBeforeTheirCommit());
  }
}

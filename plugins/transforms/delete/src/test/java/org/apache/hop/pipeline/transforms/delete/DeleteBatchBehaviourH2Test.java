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
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.BIND;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.DRIVER;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ERROR_FIELDS;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.NR_ERRORS;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ROW;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.counters;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ids;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.idsWithout;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.remaining;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.rows;
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
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.engines.local.transaction.SharedConnectionRun;
import org.apache.hop.pipeline.transforms.delete.DeleteRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pins what Delete does with batch deletes and an error hop, end to end against H2: with and
 * without batch deletes, with and without an error hop, for several commit sizes, for the last
 * partial batch, at the end of a single-threaded iteration, when the transform is stopped or
 * failed, on a connection shared by a transactional pipeline, and on a dialect that runs every row
 * behind a savepoint the way PostgreSQL does.
 *
 * <p>Each test checks the rows on the main output (values and order), the rows on the error hop
 * (error codes, fields and the kind of message), which ids are left in the table after the run, the
 * counters, and for the important cases when the rows left the transform and whether their delete
 * was committed by then. These are characterization tests: they describe current behaviour, also
 * where it is wrong. Those tests have Buggy in their name and say so. A row fails when its id is
 * still referenced by a child row: the foreign key refuses the delete.
 *
 * <p>H2, like SQL Server and MySQL, runs the whole batch and marks the failed rows with {@code
 * EXECUTE_FAILED} in the update counts. Since #9768 that validated contract recovers failed batches
 * with RecoverableBatch when batch deletes are on. The tests whose name says legacy run with
 * HOP_DATABASE_BATCH_RECOVERY=LEGACY and with the contract "As before" (UNKNOWN) on the connection,
 * which keep the handling as it was; each is followed by what the default path does now. The other
 * tests run on the default path as well: they give what main gives.
 */
class DeleteBatchBehaviourH2Test {

  /** H2 that asks for a savepoint per row with an error hop, the way PostgreSQL does. */
  @DatabaseMetaPlugin(type = "H2_DELETE_SAVEPOINTS", typeDescription = "H2 with savepoints")
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

  /**
   * An H2 database of the given type with the target table holding ids 1..n, the given ids still
   * referenced by a child row.
   */
  static DatabaseMeta target(String type, int n, long... referenced) throws Exception {
    String name =
        "mem:delete" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta = new DatabaseMeta("h2", type, "Native", "", name, "", "sa", "");
    DeleteRun.createTarget(databaseMeta, n, referenced);
    return databaseMeta;
  }

  static DatabaseMeta target(int n, long... referenced) throws Exception {
    return target("H2", n, referenced);
  }

  /** A path that handles a failed batch as main always did: its variable and connection option. */
  record Legacy(String recovery, String contract) {
    @Override
    public String toString() {
      if (recovery != null) {
        return "HOP_DATABASE_BATCH_RECOVERY=" + recovery;
      }
      return contract == null ? "default" : "contract " + contract + " on the connection";
    }

    DatabaseMeta apply(DatabaseMeta databaseMeta) {
      if (contract != null) {
        databaseMeta.setBatchFailureContractOverride(contract);
      }
      return databaseMeta;
    }
  }

  /** The default path, HOP_DATABASE_BATCH_RECOVERY=LEGACY and "As before" on the connection. */
  static Stream<Legacy> paths() {
    return Stream.of(
        new Legacy(null, null), new Legacy("LEGACY", null), new Legacy(null, "UNKNOWN"));
  }

  /** HOP_DATABASE_BATCH_RECOVERY=LEGACY and "As before" on the connection. */
  static Stream<Legacy> legacyPaths() {
    return Stream.of(new Legacy("LEGACY", null), new Legacy(null, "UNKNOWN"));
  }

  /** Recovered batches: batching on, also with an error hop, and no savepoint per row. */
  static void assertRecovered(Result r) {
    assertTrue(r.recoveredBatches);
    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertFalse(r.logged("batch updates are switched off"));
  }

  /**
   * Every row on the main output was deleted and committed when it was passed on, and every row on
   * the error hop was still in the table.
   */
  static void assertPassedOnAfterTheCommit(Result r) {
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(ids(), r.deletedBeforeTheErrorHop());
  }

  static DeleteRun on(Legacy path, DatabaseMeta databaseMeta) {
    return DeleteRun.on(path.apply(databaseMeta)).recovery(path.recovery());
  }

  static List<Object> repeat(Object value, int times) {
    return Collections.nCopies(times, value);
  }

  /** Every error row says one error, and Delete never names the fields. */
  static void assertNoErrorFields(Result r) {
    for (Object nrErrors : r.errorField(NR_ERRORS)) {
      assertEquals(1L, nrErrors);
    }
    for (Object fields : r.errorField(ERROR_FIELDS)) {
      assertNull(fields);
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Batch deletes off
  // ----------------------------------------------------------------------------------------------

  static Stream<Arguments> rowByRowCommits() {
    List<Arguments> arguments = new ArrayList<>();
    paths()
        .forEach(
            path -> {
              arguments.add(Arguments.of(path, 1, ids()));
              // Every third row deleted commits the rows before it: 4, 8 and the rows before them
              arguments.add(Arguments.of(path, 3, ids(1, 2, 5, 6, 9, 10)));
              // One commit at the end
              arguments.add(Arguments.of(path, 100, idsWithout(10, 3, 7)));
            });
    return arguments.stream();
  }

  /**
   * Buggy: without batching a row is passed on as soon as it is deleted, before the commit that the
   * commit size asks for. Only the rows the database refused go to the error hop.
   */
  @ParameterizedTest(name = "{0}, commit size {1}")
  @MethodSource("rowByRowCommits")
  void batchOff_errorHop_Buggy_rowsLeaveBeforeTheirCommit(
      Legacy path, int commitSize, List<Object> passedOnBeforeTheirCommit) throws Exception {
    DatabaseMeta db = target(10, 3, 7);

    Result r =
        on(path, db)
            .batch(false)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10));

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(repeat("DEL001", 2), r.errorCodes());
    assertEquals(repeat(ROW, 2), r.errorKinds());
    assertNoErrorFields(r);
    assertEquals(ids(3, 7), remaining(db));
    assertEquals(counters(10, 8, 8, 2), r.counters());
    assertEquals(0, r.errorCount);
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2), r.errorSizeAtRead);
    assertEquals(passedOnBeforeTheirCommit, r.passedOnBeforeTheirCommit());
    assertEquals(ids(), r.deletedBeforeTheErrorHop());
  }

  /**
   * Buggy: without an error hop a failed row stops the transform and the transaction is rolled
   * back. Rows 1-4 were committed by the commit size of 4; rows 5 and 6, already passed on, are
   * rolled back with the transaction and still in the table.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("paths")
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

  /** A transform that is stopped commits the deletes it ran when it is disposed. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("paths")
  void batchOff_stopped_everyRowPassedOnIsCommitted(Legacy path) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        on(path, db)
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
    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(ids(8, 9, 10), remaining(db));
    assertEquals(counters(7, 7, 7, 0), r.counters());
  }

  /**
   * Buggy: a transform that failed rolls back what was not committed yet, although those rows were
   * passed on. With commit size 5, failed after row 7: rows 1-7 are passed on, rows 6 and 7 are
   * still in the table.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("paths")
  void batchOff_failed_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        on(path, db)
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

    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(ids(6, 7, 8, 9, 10), remaining(db));
    assertEquals(counters(7, 7, 7, 0), r.counters());
  }

  /**
   * At the end of a single-threaded iteration the open transaction is committed; the rows were
   * passed on right after their delete already.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("paths")
  void batchOff_batchComplete_commitsTheOpenTransaction(Legacy path) throws Exception {
    DatabaseMeta db = target(5, 2);
    List<List<Object>> remainingAtBatchComplete = new ArrayList<>();

    Result r =
        on(path, db)
            .batch(false)
            .errorHop(true)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    remainingAtBatchComplete.add(remaining(db));
                  }
                })
            .run(rows(5));

    assertEquals(List.of(ids(2, 4, 5)), remainingAtBatchComplete);
    assertEquals(List.of(0, 1, 1, 2, 3, 4), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 1, 1, 1, 1), r.errorSizeAtRead);
    assertEquals(ids(1, 3, 4, 5), r.outputKeys());
    assertEquals(ids(2), r.errorKeys());
    assertEquals(ids(2), remaining(db));
    assertEquals(counters(5, 4, 4, 1), r.counters());
  }

  /**
   * A key Hop can't bind to the DELETE (here an id that is no number) goes to the error hop at
   * once, in its place, without the database seeing it.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("paths")
  void batchOff_errorHop_aKeyThatCannotBeBoundGoesToTheErrorHop(Legacy path) throws Exception {
    DatabaseMeta db = target(10, 7);

    Result r =
        on(path, db)
            .batch(false)
            .errorHop(true)
            .commitSize(5)
            .run(DeleteRun.rowsWithUnboundKeys(10, 4));

    assertEquals(idsWithout(10, 4, 7), r.outputKeys());
    assertEquals(List.of("no number 4", 7L), r.errorKeys());
    assertEquals(repeat("DEL001", 2), r.errorCodes());
    assertEquals(List.of(BIND, ROW), r.errorKinds());
    assertEquals(ids(4, 7), remaining(db));
    assertEquals(counters(10, 8, 8, 2), r.counters());
  }

  /**
   * On a connection shared by a transactional pipeline the commit only comes at the end of the
   * pipeline; the rows are passed on right after their delete.
   */
  @ParameterizedTest(name = "batch {0}")
  @ValueSource(booleans = {false, true})
  void transactionalPipeline_rowByRow_rowsLeaveAtOnce(boolean batch) throws Exception {
    DatabaseMeta db = target(10, 3, 7);

    Result r =
        DeleteRun.on(db)
            .batch(batch)
            .errorHop(true)
            .commitSize(4)
            .connectionGroup("delete-" + UUID.randomUUID())
            .run(rows(10));

    assertEquals(batch, r.batchMode);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(repeat(ROW, 2), r.errorKinds());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2), r.errorSizeAtRead);
    assertEquals(ids(3, 7), remaining(db));
    assertEquals(counters(10, 8, 8, 2), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // Batch deletes on, with an error hop
  // ----------------------------------------------------------------------------------------------

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * without failures every row is deleted, but passed on as soon as it is added to the batch.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void batchOn_errorHop_legacy_Buggy_rowsLeaveBeforeTheirBatchRuns(Legacy path) throws Exception {
    DatabaseMeta db = target(10);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(4).watchCommits().run(rows(10));

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(ids(), remaining(db));
    assertEquals(counters(10, 10, 10, 0), r.counters());
    assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), r.outputSizeAtRead);
    // The batch runs and is committed by the row that fills it, before that row is passed on.
    assertEquals(ids(1, 2, 3, 5, 6, 7, 9, 10), r.passedOnBeforeTheirCommit());
  }

  /**
   * Fixed by #9768, where the batch failure contract is validated (H2): the rows of a batch leave
   * together, once the batch has run and is committed.
   */
  @Test
  void batchOn_errorHop_rowsLeaveABatchAtATimeAfterTheirCommit() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        DeleteRun.on(db).batch(true).errorHop(true).commitSize(4).watchCommits().run(rows(10));

    assertRecovered(r);
    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(ids(), remaining(db));
    assertEquals(counters(10, 10, 10, 0), r.counters());
    assertEquals(List.of(0, 0, 0, 0, 4, 4, 4, 4, 8, 8, 8), r.outputSizeAtRead);
    assertPassedOnAfterTheCommit(r);
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * when the batch fails, the row that ran it (the last one added, at the commit size) goes to the
   * error hop, whichever row the database refused. H2 runs the other rows of the batch, so row 3,
   * which was refused, was passed on and is still in the table, and row 5, on the error hop, is
   * deleted.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void batchOn_errorHop_legacy_Buggy_theRowThatRunsTheBatchIsRejectedInsteadOfTheFailedOne(
      Legacy path) throws Exception {
    DatabaseMeta db = target(10, 3);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(5).watchCommits().run(rows(10));

    assertEquals(idsWithout(10, 5), r.outputKeys());
    assertEquals(ids(5), r.errorKeys());
    assertEquals(List.of("DEL001"), r.errorCodes());
    assertEquals(List.of(BATCH), r.errorKinds());
    assertNoErrorFields(r);
    assertEquals(ids(3), remaining(db));
    assertEquals(counters(10, 9, 9, 1), r.counters());
    assertEquals(0, r.errorCount);
    assertEquals(List.of(0, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1), r.errorSizeAtRead);
  }

  /**
   * Fixed by #9768: H2 marks the refused row of the batch, so only that row goes to the error hop,
   * with the driver's own message and code DEL002; the other rows of the batch are committed and
   * passed on, in input order.
   */
  @Test
  void batchOn_errorHop_onlyTheRefusedRowIsRejected() throws Exception {
    DatabaseMeta db = target(10, 3);

    Result r =
        DeleteRun.on(db).batch(true).errorHop(true).commitSize(5).watchCommits().run(rows(10));

    assertRecovered(r);
    assertEquals(idsWithout(10, 3), r.outputKeys());
    assertEquals(ids(3), r.errorKeys());
    assertEquals(List.of("DEL002"), r.errorCodes());
    assertEquals(List.of(DRIVER), r.errorKinds());
    assertNoErrorFields(r);
    assertEquals(ids(3), remaining(db));
    assertEquals(counters(10, 9, 9, 1), r.counters());
    assertEquals(0, r.errorCount);
    assertEquals(List.of(0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 9), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1), r.errorSizeAtRead);
    assertPassedOnAfterTheCommit(r);
  }

  /**
   * When the refused row happens to be the one that runs the batch, the error hop is right also
   * with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection. On the default path
   * the batch failure contract finds the refused rows wherever they are.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("paths")
  void batchOn_errorHop_theRefusedRowRunsTheBatch(Legacy path) throws Exception {
    DatabaseMeta db = target(10, 5, 10);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(5).run(rows(10));

    assertEquals(idsWithout(10, 5, 10), r.outputKeys());
    assertEquals(ids(5, 10), r.errorKeys());
    assertEquals(ids(5, 10), remaining(db));
    assertEquals(counters(10, 8, 8, 2), r.counters());
    assertEquals(0, r.errorCount);
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * two batches fail, each sends its last row to the error hop. Rows 3 and 8, refused, were passed
   * on; rows 5 and 10, on the error hop, are deleted. See DeleteRecoveredBatchH2Test for the
   * default path.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void batchOn_errorHop_legacy_Buggy_everyFailedBatchRejectsItsLastRow(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10, 3, 8);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(5).run(rows(10));

    assertEquals(ids(1, 2, 3, 4, 6, 7, 8, 9), r.outputKeys());
    assertEquals(ids(5, 10), r.errorKeys());
    assertEquals(repeat(BATCH, 2), r.errorKinds());
    assertEquals(ids(3, 8), remaining(db));
    assertEquals(counters(10, 8, 8, 2), r.counters());
    assertEquals(0, r.errorCount);
  }

  /**
   * With commit size 1 every row is a batch of its own, which runs before the row is passed on:
   * only the refused rows go to the error hop. With HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As
   * before" on the connection they carry the message of the failed batch and code DEL001; on the
   * default path the driver's own message and code DEL002.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("paths")
  void batchOn_errorHop_commitSizeOne_everyRowIsItsOwnBatch(Legacy path) throws Exception {
    DatabaseMeta db = target(5, 2, 4);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(1).watchCommits().run(rows(5));

    assertTrue(r.batchMode);
    assertEquals(ids(1, 3, 5), r.outputKeys());
    assertEquals(ids(2, 4), r.errorKeys());
    boolean recovered = path.recovery() == null && path.contract() == null;
    assertEquals(recovered, r.recoveredBatches);
    assertEquals(repeat(recovered ? "DEL002" : "DEL001", 2), r.errorCodes());
    assertEquals(repeat(recovered ? DRIVER : BATCH, 2), r.errorKinds());
    assertEquals(List.of(0, 1, 1, 2, 2, 3), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 1, 1, 2, 2), r.errorSizeAtRead);
    assertEquals(ids(2, 4), remaining(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(counters(5, 3, 3, 2), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * the last partial batch runs when the transform is disposed. When it fails there, no row goes to
   * the error hop although there is one: the transform fails, and its rows were passed on before.
   * H2 ran the other rows of that batch, and the connection commits them when it closes: row 9 is
   * deleted, row 10 is not.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void batchOn_errorHop_legacy_Buggy_aFailedLastPartialBatchFailsTheTransform(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10, 10);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(4).run(rows(10));

    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(ids(), r.errorKeys());
    assertEquals(ids(10), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(10, 10, 10, 0), r.counters());
    assertTrue(
        r.errorLog.stream().anyMatch(line -> line.contains("Unable to commit Update connection")));
  }

  /**
   * Fixed by #9768: the last partial batch runs at the end of the input, and its refused row goes
   * to the error hop like any other.
   */
  @Test
  void batchOn_errorHop_theLastPartialBatchRunsAtTheEndOfTheInput() throws Exception {
    DatabaseMeta db = target(10, 10);

    Result r =
        DeleteRun.on(db).batch(true).errorHop(true).commitSize(4).watchCommits().run(rows(10));

    assertRecovered(r);
    assertEquals(idsWithout(9), r.outputKeys());
    assertEquals(ids(10), r.errorKeys());
    assertEquals(List.of("DEL002"), r.errorCodes());
    assertEquals(ids(10), remaining(db));
    assertEquals(0, r.errorCount);
    assertEquals(counters(10, 9, 9, 1), r.counters());
    assertEquals(List.of(0, 0, 0, 0, 4, 4, 4, 4, 8, 8, 8), r.outputSizeAtRead);
    assertPassedOnAfterTheCommit(r);
  }

  /**
   * With HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection, a key Hop can't bind
   * goes to the error hop at once; the rows in the batch around it are passed on as they are added.
   * The row does not count towards the commit size of the batch.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void batchOn_errorHop_legacy_aKeyThatCannotBeBoundGoesToTheErrorHopAtOnce(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        on(path, db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(DeleteRun.rowsWithUnboundKeys(10, 4));

    assertEquals(idsWithout(10, 4), r.outputKeys());
    assertEquals(List.of("no number 4"), r.errorKeys());
    assertEquals(List.of("DEL001"), r.errorCodes());
    assertEquals(List.of(BIND), r.errorKinds());
    assertEquals(List.of(0, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1), r.errorSizeAtRead);
    assertEquals(ids(4), remaining(db));
    assertEquals(counters(10, 9, 9, 1), r.counters());
  }

  /**
   * On the default path a key Hop can't bind goes to the error hop in its place, after the rows of
   * the batch in front of it, and counts towards the commit size like the rows of the batch.
   */
  @Test
  void batchOn_errorHop_aKeyThatCannotBeBoundKeepsItsPlace() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        DeleteRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(DeleteRun.rowsWithUnboundKeys(10, 4));

    assertRecovered(r);
    assertEquals(idsWithout(10, 4), r.outputKeys());
    assertEquals(List.of("no number 4"), r.errorKeys());
    assertEquals(List.of("DEL001"), r.errorCodes());
    assertEquals(List.of(BIND), r.errorKinds());
    assertEquals(List.of(0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 9), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1), r.errorSizeAtRead);
    assertEquals(ids(4), remaining(db));
    assertEquals(counters(10, 9, 9, 1), r.counters());
  }

  /**
   * Commit size 0 is auto-commit: batch mode stays on, but the driver runs and commits every delete
   * on its own, and every row is passed on right after it.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("paths")
  void batchOn_commitSizeZero_everyRowIsCommittedOnItsOwn(Legacy path) throws Exception {
    DatabaseMeta db = target(10, 3);

    Result r = on(path, db).batch(true).errorHop(true).commitSize(0).watchCommits().run(rows(10));

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10, 3), r.outputKeys());
    assertEquals(ids(3), r.errorKeys());
    assertEquals(List.of("DEL001"), r.errorCodes());
    assertEquals(List.of(ROW), r.errorKinds());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 6, 7, 8, 9), r.outputSizeAtRead);
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(ids(3), remaining(db));
    assertEquals(counters(10, 9, 9, 1), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // Batch deletes on, without an error hop
  // ----------------------------------------------------------------------------------------------

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * a failed batch is rolled back and stops the transform, but the rows of that batch were passed
   * on as they were added: rows 5-7 are on the main output and still in the table.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void batchOn_noErrorHop_legacy_Buggy_theRowsOfTheFailedBatchWerePassedOn(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10, 7);

    Result r = on(path, db).batch(true).errorHop(false).commitSize(4).run(rows(10));

    assertTrue(r.batchMode);
    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(ids(5, 6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertNull(r.thrown);
    assertEquals(counters(8, 7, 7, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.contains("Error updating batch")));
  }

  /**
   * Fixed by #9768: a failed batch is rolled back and stops the transform, and none of its rows is
   * passed on. The rows of the batches committed before it were passed on and stay deleted.
   */
  @Test
  void batchOn_noErrorHop_aFailedBatchIsRolledBackAndNoneOfItsRowsIsPassedOn() throws Exception {
    DatabaseMeta db = target(10, 7);

    Result r =
        DeleteRun.on(db).batch(true).errorHop(false).commitSize(4).watchCommits().run(rows(10));

    assertTrue(r.recoveredBatches);
    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(ids(5, 6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertNull(r.thrown);
    assertEquals(counters(8, 4, 4, 0), r.counters());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertTrue(
        r.errorLog.stream()
            .anyMatch(line -> line.contains("Error deleting a batch of rows in table")));
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * the first batch fails, and the rows before the one that ran it were passed on.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void batchOn_noErrorHop_legacy_Buggy_theFirstBatchFails(Legacy path) throws Exception {
    DatabaseMeta db = target(10, 2);

    Result r = on(path, db).batch(true).errorHop(false).commitSize(4).run(rows(10));

    assertEquals(ids(1, 2, 3), r.outputKeys());
    assertEquals(idsWithout(10), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(4, 3, 3, 0), r.counters());
  }

  /** Fixed by #9768: the first batch fails, and no row is passed on. */
  @Test
  void batchOn_noErrorHop_theFirstBatchFails() throws Exception {
    DatabaseMeta db = target(10, 2);

    Result r = DeleteRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10));

    assertEquals(ids(), r.outputKeys());
    assertEquals(idsWithout(10), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(4, 0, 0, 0), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * the partial batch at the end of the input fails when the transform is disposed. Its rows were
   * passed on; H2 ran the other rows of the batch and the connection commits them when it closes.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void batchOn_noErrorHop_legacy_Buggy_theLastPartialBatchFails(Legacy path) throws Exception {
    DatabaseMeta db = target(10, 9);

    Result r = on(path, db).batch(true).errorHop(false).commitSize(4).run(rows(10));

    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(ids(9), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(10, 10, 10, 0), r.counters());
  }

  /**
   * Fixed by #9768: the partial batch at the end of the input fails at the end of the input and is
   * rolled back; only the full batches before it were passed on and stay deleted.
   */
  @Test
  void batchOn_noErrorHop_theLastPartialBatchFails() throws Exception {
    DatabaseMeta db = target(10, 9);

    Result r = DeleteRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10));

    assertEquals(idsWithout(8), r.outputKeys());
    assertEquals(ids(9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(10, 8, 8, 0), r.counters());
  }

  // ----------------------------------------------------------------------------------------------
  // batchComplete() and dispose()
  // ----------------------------------------------------------------------------------------------

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * at the end of a single-threaded iteration the transaction is committed, but the batch is not
   * run, so the rows passed on in that iteration are still in the table. The batch runs when the
   * transform is disposed, and when it fails there the transform fails without a row on the error
   * hop: row 2 was passed on and is still in the table.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void batchComplete_legacy_Buggy_commitsWithoutRunningTheBatch(Legacy path) throws Exception {
    DatabaseMeta db = target(5, 2);
    List<List<Object>> remainingAtBatchComplete = new ArrayList<>();

    Result r =
        on(path, db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    remainingAtBatchComplete.add(remaining(db));
                  }
                })
            .run(rows(5));

    assertEquals(List.of(idsWithout(5)), remainingAtBatchComplete);
    assertEquals(List.of(0, 1, 2, 3, 4, 5), r.outputSizeAtRead);
    assertEquals(idsWithout(5), r.outputKeys());
    assertEquals(ids(), r.errorKeys());
    assertEquals(ids(2), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(5, 5, 5, 0), r.counters());
  }

  /**
   * Fixed by #9768: at the end of a single-threaded iteration the batch runs, is committed and its
   * rows are passed on, although it is not full; its refused row goes to the error hop.
   */
  @Test
  void batchComplete_runsCommitsAndPassesOnThePendingBatch() throws Exception {
    DatabaseMeta db = target(5, 2);
    List<List<Object>> remainingAtBatchComplete = new ArrayList<>();

    Result r =
        DeleteRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .watchCommits()
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    remainingAtBatchComplete.add(remaining(db));
                  }
                })
            .run(rows(5));

    assertEquals(List.of(ids(2, 4, 5)), remainingAtBatchComplete);
    assertEquals(List.of(0, 0, 0, 2, 2, 2), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1), r.errorSizeAtRead);
    assertEquals(ids(1, 3, 4, 5), r.outputKeys());
    assertEquals(ids(2), r.errorKeys());
    assertEquals(ids(2), remaining(db));
    assertEquals(0, r.errorCount);
    assertEquals(counters(5, 4, 4, 1), r.counters());
    assertPassedOnAfterTheCommit(r);
  }

  /** Without an error hop a failed batch at the end of an iteration is rolled back and thrown. */
  @Test
  void batchComplete_noErrorHop_aFailedBatchIsRolledBackAndThrown() throws Exception {
    DatabaseMeta db = target(5, 2);

    Result r =
        DeleteRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                  }
                })
            .run(rows(5));

    assertInstanceOf(HopDatabaseBatchException.class, r.thrown);
    assertEquals(ids(), r.outputKeys());
    assertEquals(idsWithout(5), remaining(db));
    assertEquals(1, r.errorCount);
  }

  /**
   * With HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection, a transform that is
   * stopped with rows in its batch runs and commits them when it is disposed: the rows were passed
   * on as they were added, and are deleted.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void stopped_legacy_thePendingBatchIsCommittedOnDispose(Legacy path) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        on(path, db)
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
    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(ids(8, 9, 10), remaining(db));
    assertEquals(counters(7, 7, 7, 0), r.counters());
  }

  /**
   * On the default path the rows a stopped transform still has in its batch have not been passed
   * on: they are dropped, not deleted, so the table holds exactly the rows that were passed on.
   */
  @Test
  void stopped_thePendingBatchIsDroppedAndNotDeleted() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        DeleteRun.on(db)
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
    assertEquals(ids(6, 7, 8, 9, 10), remaining(db));
    assertEquals(counters(7, 5, 5, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.startsWith("2 rows of the batch")));
    assertRecovered(r);
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * a transform that failed rolls back the rows of its batch when it is disposed, although they
   * were passed on: rows 6 and 7 are on the main output and still in the table.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyPaths")
  void failed_legacy_Buggy_thePendingBatchIsRolledBackButWasPassedOn(Legacy path) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        on(path, db)
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

    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(ids(6, 7, 8, 9, 10), remaining(db));
    assertEquals(counters(7, 7, 7, 0), r.counters());
  }

  /** Fixed by #9768: a transform that failed drops the rows of its batch, none was passed on. */
  @Test
  void failed_thePendingBatchIsDroppedAndNotPassedOn() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        DeleteRun.on(db)
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
    assertEquals(ids(6, 7, 8, 9, 10), remaining(db));
    assertEquals(counters(7, 5, 5, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.startsWith("2 rows of the batch")));
  }

  // ----------------------------------------------------------------------------------------------
  // A dialect that runs every row behind a savepoint (PostgreSQL and its look-alikes)
  //
  // The subclass of H2 declares no batch failure contract, so it keeps this handling on every path.
  // ----------------------------------------------------------------------------------------------

  /**
   * With an error hop, "use batch deletes" is switched off and logged: every row runs behind a
   * savepoint, so only the failed rows go to the error hop. Buggy, as without batching: every row
   * leaves right after its savepoint section, before the commit that includes its delete.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 100})
  void savepointDialect_errorHop_Buggy_batchingIsSwitchedOffForASavepointPerRow(int commitSize)
      throws Exception {
    DatabaseMeta db = target("H2_DELETE_SAVEPOINTS", 10, 3, 7);

    Result r =
        DeleteRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10));

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(repeat("DEL001", 2), r.errorCodes());
    assertEquals(repeat(ROW, 2), r.errorKinds());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(ids(3, 7), remaining(db));
    assertEquals(counters(10, 8, 8, 2), r.counters());
    List<Object> passedOnBeforeTheirCommit =
        switch (commitSize) {
          case 1 -> ids();
            // The transform commits every third row that is deleted, after it passed the rows on
          case 3 -> ids(1, 2, 5, 6, 9, 10);
          default -> idsWithout(10, 3, 7);
        };
    assertEquals(passedOnBeforeTheirCommit, r.passedOnBeforeTheirCommit());
  }

  /** Batch deletes off: the same savepoint per row, without the log line. */
  @Test
  void savepointDialect_batchOff_errorHop_aSavepointPerRow() throws Exception {
    DatabaseMeta db = target("H2_DELETE_SAVEPOINTS", 10, 3, 7);

    Result r = DeleteRun.on(db).batch(false).errorHop(true).commitSize(4).run(rows(10));

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertFalse(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(ids(3, 7), remaining(db));
  }

  /** Without an error hop no savepoints are used and the dialect batches as usual. */
  @Test
  void savepointDialect_noErrorHop_keepsBatching() throws Exception {
    DatabaseMeta db = target("H2_DELETE_SAVEPOINTS", 10, 7);

    Result r = DeleteRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10));

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertFalse(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(ids(5, 6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
  }
}

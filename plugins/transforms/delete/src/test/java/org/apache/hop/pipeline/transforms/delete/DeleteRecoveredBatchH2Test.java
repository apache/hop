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

import static org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test.assertPassedOnAfterTheCommit;
import static org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test.assertRecovered;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.BATCH;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.BIND;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.DRIVER;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.counters;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ids;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.idsWithout;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.remaining;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.rows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.engines.local.transaction.SharedConnectionRun;
import org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test.H2SavepointsDatabaseMeta;
import org.apache.hop.pipeline.transforms.delete.DeleteRun.Result;
import org.apache.hop.pipeline.transforms.delete.SimulatedBatchDriver.Shape;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Delete on a connection whose batch failure contract is known: failed batches are recovered with
 * RecoverableBatch, also with an error hop. Every row must reach exactly one stream, in input
 * order, once its delete is committed or refused; the table must have lost exactly the rows on the
 * main output, and the counters must be exact.
 *
 * <p>H2 declares MARKS_FAILED_ROWS. The other contracts are set on the connection, each with H2 and
 * with a simulated driver that behaves the way the contract describes, because each of them has to
 * route the rows correctly on any driver whose behaviour it covers.
 */
class DeleteRecoveredBatchH2Test {

  /** H2 as a dialect whose driver has not been measured: it declares no batch failure contract. */
  @DatabaseMetaPlugin(type = "H2_DELETE_UNVALIDATED", typeDescription = "H2, not measured")
  public static class UnvalidatedH2DatabaseMeta extends H2DatabaseMeta {}

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2SavepointsDatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(UnvalidatedH2DatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    SharedConnectionRun.registerConnectionGroupExtensionPoint();
  }

  /** A contract on the connection (null: H2's own) and a simulated driver (null: H2 itself). */
  record Setup(String contract, Shape driver) {
    @Override
    public String toString() {
      return (contract == null ? "MARKS_FAILED_ROWS (H2)" : contract)
          + " on "
          + (driver == null ? "H2" : driver.name());
    }
  }

  static Stream<Setup> setups() {
    return Stream.of(
        new Setup(null, null),
        new Setup(null, Shape.PREFIX_WITHOUT_COUNTS),
        new Setup("KEEPS_PREFIX", Shape.KEEPS_PREFIX),
        new Setup("KEEPS_PREFIX", null),
        new Setup("LOSES_WHOLE_BATCH", null),
        new Setup("LOSES_WHOLE_BATCH", Shape.PREFIX_WITHOUT_COUNTS));
  }

  static DatabaseMeta target(String type, String contract, int n, long... referenced)
      throws Exception {
    DatabaseMeta databaseMeta = DeleteBatchBehaviourH2Test.target(type, n, referenced);
    if (contract != null) {
      databaseMeta.setBatchFailureContractOverride(contract);
    }
    return databaseMeta;
  }

  static DatabaseMeta target(Setup setup, int n, long... referenced) throws Exception {
    return target("H2", setup.contract(), n, referenced);
  }

  static DeleteRun recovered(DatabaseMeta db, Setup setup) {
    return DeleteRun.on(db).driver(setup.driver()).batch(true).errorHop(true).watchCommits();
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {
      {}, {1}, {3}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}
    };
    setups()
        .forEach(
            setup -> {
              for (long[] bad : badSets) {
                arguments.add(Arguments.of(setup, bad));
              }
            });
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0}, bad rows {1}")
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(Setup setup, long[] bad) throws Exception {
    DatabaseMeta db = target(setup, 10, bad);

    Result r = recovered(db, setup).commitSize(5).run(rows(10));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(Collections.nCopies(bad.length, "DEL002"), r.errorCodes());
    assertEquals(Collections.nCopies(bad.length, DRIVER), r.errorKinds());
    assertEquals(ids(bad), remaining(db));
    assertEquals(counters(10, 10 - bad.length, 10 - bad.length, bad.length), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  static Stream<Arguments> commitSizes() {
    List<Arguments> arguments = new ArrayList<>();
    setups()
        .forEach(
            setup -> {
              for (int commitSize : new int[] {1, 2, 3, 4, 7, 100}) {
                arguments.add(Arguments.of(setup, commitSize));
              }
            });
    return arguments.stream();
  }

  /**
   * Any commit size routes the rows the same, and the batches hold exactly the commit size: a batch
   * runs, is committed and passes its rows on once it is full, and the last one at the end of the
   * input.
   */
  @ParameterizedTest(name = "{0}, commit size {1}")
  @MethodSource("commitSizes")
  void everyBatchHoldsTheCommitSize(Setup setup, int commitSize) throws Exception {
    DatabaseMeta db = target(setup, 10, 2, 5, 9);

    Result r = recovered(db, setup).commitSize(commitSize).run(rows(10));

    assertEquals(idsWithout(10, 2, 5, 9), r.outputKeys());
    assertEquals(ids(2, 5, 9), r.errorKeys());
    assertEquals(ids(2, 5, 9), remaining(db));
    assertEquals(counters(10, 7, 7, 3), r.counters());
    // Before read k (0-based) the rows of every full batch before it have left, no more
    List<Integer> settledAtRead = new ArrayList<>();
    for (int read = 0; read < r.outputSizeAtRead.size(); read++) {
      settledAtRead.add(r.outputSizeAtRead.get(read) + r.errorSizeAtRead.get(read));
    }
    List<Integer> expected = new ArrayList<>();
    for (int read = 0; read <= 10; read++) {
      expected.add(read / commitSize * commitSize);
    }
    assertEquals(expected, settledAtRead);
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  static Stream<Arguments> unboundKeyCases() {
    List<Arguments> arguments = new ArrayList<>();
    setups()
        .forEach(
            setup -> {
              for (int commitSize : new int[] {1, 3, 5, 100}) {
                arguments.add(Arguments.of(setup, commitSize));
              }
            });
    return arguments.stream();
  }

  /**
   * A key Hop can't bind never reaches the batch: it goes to the error hop with code DEL001, in its
   * place between the rows of the batch, also when the batch fails.
   */
  @ParameterizedTest(name = "{0}, commit size {1}")
  @MethodSource("unboundKeyCases")
  void aKeyThatCannotBeBoundKeepsItsPlace(Setup setup, int commitSize) throws Exception {
    DatabaseMeta db = target(setup, 10, 2, 7);

    Result r =
        recovered(db, setup).commitSize(commitSize).run(DeleteRun.rowsWithUnboundKeys(10, 4));

    assertEquals(idsWithout(10, 2, 4, 7), r.outputKeys());
    assertEquals(List.of(2L, "no number 4", 7L), r.errorKeys());
    assertEquals(List.of("DEL002", "DEL001", "DEL002"), r.errorCodes());
    assertEquals(List.of(DRIVER, BIND, DRIVER), r.errorKinds());
    assertEquals(ids(2, 4, 7), remaining(db));
    assertEquals(counters(10, 7, 7, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * A DELETE that matches no row is not a failure: the row is passed on and counted as updated, as
   * Delete always counted it.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void aKeyThatMatchesNoRowIsNoFailure(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 5, 2);
    List<Object[]> input =
        List.of(
            new Object[] {1L, "r1"},
            new Object[] {11L, "r11"},
            new Object[] {2L, "r2"},
            new Object[] {12L, "r12"},
            new Object[] {3L, "r3"});

    Result r = recovered(db, setup).commitSize(3).run(input);

    assertEquals(ids(1, 11, 12, 3), r.outputKeys());
    assertEquals(ids(2), r.errorKeys());
    assertEquals(ids(2, 4, 5), remaining(db));
    assertEquals(counters(5, 4, 4, 1), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * Without an error hop a failed batch is rolled back and stops the transform, whatever the
   * contract: only the rows of the batches committed before it were passed on and counted.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10, 7);

    Result r =
        DeleteRun.on(db)
            .driver(setup.driver())
            .batch(true)
            .errorHop(false)
            .commitSize(4)
            .watchCommits()
            .run(rows(10));

    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(ids(5, 6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(8, 4, 4, 0), r.counters());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertTrue(
        r.errorLog.stream()
            .anyMatch(line -> line.contains("Error deleting a batch of rows in table")));
    assertTrue(r.recoveredBatches);
  }

  /** At the end of a single-threaded iteration a failed batch is recovered like any other. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void batchCompleteRecoversTheBatch(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 5, 2, 4);
    List<List<Object>> remainingAtBatchComplete = new ArrayList<>();

    Result r =
        recovered(db, setup)
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
    assertEquals(List.of(0, 0, 0, 2, 2, 2), r.outputSizeAtRead);
    assertEquals(ids(1, 3, 5), r.outputKeys());
    assertEquals(ids(2, 4), r.errorKeys());
    assertEquals(ids(2, 4), remaining(db));
    assertEquals(counters(5, 3, 3, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** A transform that fails drops the rows of its batch: none of them is deleted. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void aFailedTransformDropsItsBatch(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        recovered(db, setup)
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
    assertRecovered(r);
  }

  /**
   * A contract that does not trust the counts recovers by running rows again, so a trigger that is
   * not transactional fires more than once for a row. With H2's own contract the counts say which
   * rows were deleted, and every row runs once: ten times, since H2 fires the trigger of the
   * refused row too, before its foreign key refuses the delete.
   */
  @Test
  void aRecoveredBatchCanRunARowMoreThanOnce() throws Exception {
    int marks = triggerFiresFor(null);
    int losesWholeBatch = triggerFiresFor("LOSES_WHOLE_BATCH");

    assertEquals(10, marks);
    assertTrue(losesWholeBatch > 10, "fired " + losesWholeBatch + " times");
  }

  private static int triggerFiresFor(String contract) throws Exception {
    DatabaseMeta db = target("H2", contract, 10, 5);
    DeleteRun.execute(
        db,
        "CREATE TRIGGER target_deletes AFTER DELETE ON target FOR EACH ROW CALL '"
            + DeleteCountingTrigger.class.getName()
            + "'");
    DeleteCountingTrigger.DELETES.set(0);

    Result r = DeleteRun.on(db).batch(true).errorHop(true).commitSize(10).run(rows(10));

    assertTrue(r.recoveredBatches);
    assertEquals(idsWithout(10, 5), r.outputKeys());
    assertEquals(ids(5), remaining(db));
    return DeleteCountingTrigger.DELETES.get();
  }

  /**
   * A dialect that runs every row behind a savepoint (as PostgreSQL's look-alikes do) batches with
   * an error hop once its contract is set on the connection.
   */
  @Test
  void theConnectionOptionRecoversBatchesOnAnyDialect() throws Exception {
    DatabaseMeta db = target("H2_DELETE_SAVEPOINTS", "LOSES_WHOLE_BATCH", 10, 3, 7);

    Result r =
        DeleteRun.on(db).batch(true).errorHop(true).commitSize(4).watchCommits().run(rows(10));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(ids(3, 7), remaining(db));
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * Without a validated contract a failed batch is handled as before, see
   * DeleteBatchBehaviourH2Test: the row that ran the batch goes to the error hop instead of the row
   * the database refused.
   */
  private static void assertHandledAsBefore(Result r, DatabaseMeta db) throws Exception {
    assertFalse(r.recoveredBatches);
    assertTrue(r.batchMode);
    assertEquals(idsWithout(10, 5), r.outputKeys());
    assertEquals(ids(5), r.errorKeys());
    assertEquals(List.of("DEL001"), r.errorCodes());
    assertEquals(List.of(BATCH), r.errorKinds());
    assertEquals(ids(3), remaining(db));
  }

  @Test
  void theLegacyVariableKeepsTheHandlingAsBefore() throws Exception {
    DatabaseMeta db = target("H2", null, 10, 3);

    Result r =
        DeleteRun.on(db).recovery("LEGACY").batch(true).errorHop(true).commitSize(5).run(rows(10));

    assertHandledAsBefore(r, db);
  }

  @Test
  void unknownOnTheConnectionKeepsTheHandlingAsBefore() throws Exception {
    DatabaseMeta db = target("H2", "UNKNOWN", 10, 3);

    Result r = DeleteRun.on(db).batch(true).errorHop(true).commitSize(5).run(rows(10));

    assertHandledAsBefore(r, db);
  }

  /** A dialect that extends H2 declares no contract: it has not been measured. */
  @Test
  void anUnvalidatedDialectKeepsTheHandlingAsBefore() throws Exception {
    DatabaseMeta db = target("H2_DELETE_UNVALIDATED", null, 10, 3);

    Result r = DeleteRun.on(db).batch(true).errorHop(true).commitSize(5).run(rows(10));

    assertHandledAsBefore(r, db);
  }

  /**
   * Batch deletes off, a commit size of 0 (auto-commit) or a connection shared by a transactional
   * pipeline: the rows run one by one as before.
   */
  @Test
  void batchDeletesOffAutoCommitOrASharedConnectionKeepTheRowByRowPath() throws Exception {
    Result off =
        DeleteRun.on(target("H2", null, 3)).batch(false).errorHop(true).commitSize(5).run(rows(3));
    assertFalse(off.recoveredBatches);
    assertFalse(off.batchMode);

    Result autoCommit =
        DeleteRun.on(target("H2", null, 3)).batch(true).errorHop(true).commitSize(0).run(rows(3));
    assertFalse(autoCommit.recoveredBatches);

    Result shared =
        DeleteRun.on(target("H2", null, 3))
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .connectionGroup("delete-" + UUID.randomUUID())
            .run(rows(3));
    assertFalse(shared.recoveredBatches);
    assertEquals(List.of(0, 1, 2, 3), shared.outputSizeAtRead);
  }

  static Stream<Arguments> batchSupportLines() {
    return Stream.of(
        Arguments.of(null, null, "batch failure contract MARKS_FAILED_ROWS (from DIALECT)"),
        Arguments.of("LEGACY", null, "batch failure contract UNKNOWN (from VARIABLE)"),
        Arguments.of(null, "KEEPS_PREFIX", "batch failure contract KEEPS_PREFIX (from CONNECTION)"),
        Arguments.of(null, "UNKNOWN", "batch failure contract UNKNOWN (from CONNECTION)"));
  }

  /** At the detailed log level the transform says how a failed batch is handled, and why. */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}")
  @MethodSource("batchSupportLines")
  void theBatchSupportIsLogged(String recovery, String contract, String line) throws Exception {
    DatabaseMeta db = target("H2", contract, 3);

    Result r =
        DeleteRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .logLevel(LogLevel.DETAILED)
            .run(rows(3));

    assertTrue(r.detailedLog.contains("Batch support: " + line), r.detailedLog.toString());
  }
}

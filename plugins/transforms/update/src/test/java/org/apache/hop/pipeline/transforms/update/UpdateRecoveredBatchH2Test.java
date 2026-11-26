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

import static org.apache.hop.pipeline.transforms.update.UpdateRun.BATCH;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.ids;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.rows;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableUpdatedExcept;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.transforms.update.SimulatedBatchDriver.Shape;
import org.apache.hop.pipeline.transforms.update.UpdateBatchBehaviourH2Test.H2SavepointsDatabaseMeta;
import org.apache.hop.pipeline.transforms.update.UpdateBatchShapesH2Test.UnvalidatedH2DatabaseMeta;
import org.apache.hop.pipeline.transforms.update.UpdateRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Update on a connection whose batch failure contract is known: failed batches are recovered with
 * RecoverableBatch, also with an error hop. Every row must reach exactly one stream, in input
 * order, once it is committed or refused; the table must hold exactly the rows on the main output,
 * and the counters must be exact.
 *
 * <p>H2 declares MARKS_FAILED_ROWS. The other contracts are set on the connection, each with H2 and
 * with a simulated driver that behaves the way the contract describes, because each of them has to
 * route the rows correctly on any driver whose behaviour it covers.
 */
class UpdateRecoveredBatchH2Test {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2SavepointsDatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(UnvalidatedH2DatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
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

  static DatabaseMeta target(String type, String contract, int n) throws Exception {
    DatabaseMeta databaseMeta = UpdateBatchBehaviourH2Test.target(type, n);
    if (contract != null) {
      databaseMeta.setBatchFailureContractOverride(contract);
    }
    return databaseMeta;
  }

  static DatabaseMeta target(Setup setup, int n) throws Exception {
    return target("H2", setup.contract(), n);
  }

  static UpdateRun recovered(DatabaseMeta db, Setup setup) {
    return UpdateRun.on(db).driver(setup.driver()).batch(true).errorHop(true).watchCommits();
  }

  private static List<Long> counters(
      long input, long updated, long skipped, long written, long rejected) {
    return UpdateBatchBehaviourH2Test.counters(input, updated, skipped, written, rejected);
  }

  /**
   * Every row is committed when it goes to the output, and unchanged when it goes to the error hop.
   */
  static void assertPassedOnAfterTheCommit(Result r) {
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    for (int i = 0; i < r.errors.size(); i++) {
      String committed = r.committedAtError.get(i);
      Object name = r.errors.get(i)[1];
      assertFalse(
          name.equals(committed) && !UpdateRun.SEED.equals(committed),
          "row " + r.errors.get(i)[0] + " went to the error hop, but its update is committed");
    }
  }

  /** Recovered batches: batching on, also with an error hop, and no savepoint per row. */
  static void assertRecovered(Result r) {
    assertTrue(r.recoveredBatches);
    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertFalse(r.logged("batch updates are switched off"));
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
    DatabaseMeta db = target(setup, 10);

    Result r = recovered(db, setup).commitSize(5).run(rows(10, bad));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(Collections.nCopies(bad.length, "UPD002"), r.errorCodes());
    assertEquals(tableUpdatedExcept(10, bad), UpdateRun.table(db));
    assertEquals(counters(10, 10 - bad.length, 0, 10 - bad.length, bad.length), r.counters());
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
    DatabaseMeta db = target(setup, 10);

    Result r = recovered(db, setup).commitSize(commitSize).run(rows(10, 2, 5, 9));

    assertEquals(idsWithout(10, 2, 5, 9), r.outputKeys());
    assertEquals(ids(2, 5, 9), r.errorKeys());
    assertEquals(tableUpdatedExcept(10, 2, 5, 9), UpdateRun.table(db));
    assertEquals(counters(10, 7, 0, 7, 3), r.counters());
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

  static Stream<Arguments> mixedRowCases() {
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
   * A missing key goes to the error hop and a row that needs no update to the output, each in its
   * place between the rows of the batch, also when the batch fails.
   */
  @ParameterizedTest(name = "{0}, commit size {1}")
  @MethodSource("mixedRowCases")
  void rowsOutsideTheBatchKeepTheirPlace(Setup setup, int commitSize) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        recovered(db, setup).commitSize(commitSize).run(UpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(ids(1, 2, 3, 4, 6), r.outputKeys());
    assertEquals(ids(11, 12, 5), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001", "UPD002"), r.errorCodes());
    assertEquals(List.of("id", "id"), r.errorField(UpdateRun.ERROR_FIELDS).subList(0, 2));
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(counters(8, 3, 2, 5, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** Skip lookup: every row is in the batch, a missing key updates nothing and is no failure. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void skipLookup(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        recovered(db, setup)
            .commitSize(3)
            .configure(meta -> meta.setSkipLookup(true))
            .run(UpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(ids(1, 11, 2, 3, 12, 4, 6), r.outputKeys());
    assertEquals(ids(5), r.errorKeys());
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(counters(8, 7, 0, 7, 1), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** Ignore lookup failure, with the flag field: the missing keys go to the output in place. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void ignoreLookupFailureWithAFlagField(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        recovered(db, setup)
            .commitSize(3)
            .configure(
                meta -> {
                  meta.setErrorIgnored(true);
                  meta.setIgnoreFlagField("found");
                })
            .run(UpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(ids(1, 11, 2, 3, 12, 4, 6), r.outputKeys());
    assertEquals(
        List.of(true, false, true, true, false, true, true),
        r.output.stream().map(row -> row[2]).toList());
    assertEquals(ids(5), r.errorKeys());
    assertEquals(counters(8, 3, 2, 7, 1), r.counters());
    assertRecovered(r);
  }

  /**
   * Without an error hop a failed batch is rolled back and stops the transform, whatever the
   * contract: only the rows of the batches committed before it were passed on and counted.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        UpdateRun.on(db)
            .driver(setup.driver())
            .batch(true)
            .errorHop(false)
            .commitSize(4)
            .watchCommits()
            .run(rows(10, 7));

    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(8, 4, 0, 4, 0), r.counters());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertTrue(
        r.errorLog.stream()
            .anyMatch(line -> line.contains("Error updating a batch of rows in table")));
    assertTrue(r.recoveredBatches);
  }

  /** At the end of a single-threaded iteration a failed batch is recovered like any other. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void batchCompleteRecoversTheBatch(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 5);
    List<List<String>> tableAtBatchComplete = new ArrayList<>();

    Result r =
        recovered(db, setup)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    tableAtBatchComplete.add(UpdateRun.table(db));
                  }
                })
            .run(rows(5, 2, 4));

    assertEquals(List.of(tableWith(5, id -> id == 1 || id == 3)), tableAtBatchComplete);
    assertEquals(List.of(0, 0, 0, 2, 2, 2), r.outputSizeAtRead);
    assertEquals(ids(1, 3, 5), r.outputKeys());
    assertEquals(ids(2, 4), r.errorKeys());
    assertEquals(counters(5, 3, 0, 3, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** A transform that fails drops the rows of its batch: none of them is committed. */
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
    assertEquals(tableWith(10, id -> id <= 5), UpdateRun.table(db));
    assertEquals(counters(7, 5, 0, 5, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.startsWith("2 rows of the batch")));
    assertRecovered(r);
  }

  /**
   * A contract that does not trust the counts recovers by running rows again, so a trigger that is
   * not transactional fires more than once for a row. With H2's own contract the counts say which
   * rows were applied, and every updated row fires once.
   */
  @Test
  void aRecoveredBatchCanRunARowMoreThanOnce() throws Exception {
    int marks = triggerFiresFor(null);
    int losesWholeBatch = triggerFiresFor("LOSES_WHOLE_BATCH");

    assertEquals(9, marks);
    assertTrue(losesWholeBatch > 9, "fired " + losesWholeBatch + " times");
  }

  private static int triggerFiresFor(String contract) throws Exception {
    DatabaseMeta db = target("H2", contract, 10);
    UpdateRun.execute(
        db,
        "CREATE TRIGGER target_updates AFTER UPDATE ON target FOR EACH ROW CALL '"
            + UpdateCountingTrigger.class.getName()
            + "'");
    UpdateCountingTrigger.UPDATES.set(0);

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(10).run(rows(10, 5));

    assertTrue(r.recoveredBatches);
    assertEquals(idsWithout(10, 5), r.outputKeys());
    assertEquals(tableUpdatedExcept(10, 5), UpdateRun.table(db));
    return UpdateCountingTrigger.UPDATES.get();
  }

  /**
   * A dialect that runs every row behind a savepoint (as PostgreSQL's look-alikes do) batches with
   * an error hop once its contract is set on the connection.
   */
  @Test
  void theConnectionOptionRecoversBatchesOnAnyDialect() throws Exception {
    DatabaseMeta db = target("H2_UPDATE_SAVEPOINTS", "LOSES_WHOLE_BATCH", 10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * Without a validated contract a failed batch is handled as before, see UpdateBatchShapesH2Test:
   * here a driver without counts sends every row of the batch to the error hop again, also rows
   * that are committed.
   */
  private static void assertHandledAsBefore(Result r, DatabaseMeta db) throws Exception {
    assertFalse(r.recoveredBatches);
    assertTrue(r.batchMode);
    assertEquals(ids(), r.outputKeys());
    assertEquals(idsWithout(10), r.errorKeys());
    assertEquals(Collections.nCopies(10, BATCH), r.errorKinds());
    assertEquals(tableWith(10, id -> id == 1 || id == 2 || id == 6), UpdateRun.table(db));
  }

  @Test
  void theLegacyVariableKeepsTheHandlingAsBefore() throws Exception {
    DatabaseMeta db = target("H2", null, 10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .driver(Shape.PREFIX_WITHOUT_COUNTS)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(rows(10, 3, 7));

    assertHandledAsBefore(r, db);
  }

  @Test
  void unknownOnTheConnectionKeepsTheHandlingAsBefore() throws Exception {
    DatabaseMeta db = target("H2", "UNKNOWN", 10);

    Result r =
        UpdateRun.on(db)
            .driver(Shape.PREFIX_WITHOUT_COUNTS)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(rows(10, 3, 7));

    assertHandledAsBefore(r, db);
  }

  /** A dialect that extends H2 declares no contract: it has not been measured. */
  @Test
  void anUnvalidatedDialectKeepsTheHandlingAsBefore() throws Exception {
    DatabaseMeta db = target("H2_UPDATE_UNVALIDATED", null, 10);

    Result r =
        UpdateRun.on(db)
            .driver(Shape.PREFIX_WITHOUT_COUNTS)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(rows(10, 3, 7));

    assertHandledAsBefore(r, db);
  }

  /** Batch updates off, or commit size 0 (auto-commit): the rows run one by one as before. */
  @Test
  void batchUpdatesOffOrAutoCommitKeepTheRowByRowPath() throws Exception {
    Result off =
        UpdateRun.on(target("H2", null, 3)).batch(false).errorHop(true).commitSize(5).run(rows(3));
    assertFalse(off.recoveredBatches);
    assertFalse(off.batchMode);

    Result autoCommit =
        UpdateRun.on(target("H2", null, 3)).batch(true).errorHop(true).commitSize(0).run(rows(3));
    assertFalse(autoCommit.recoveredBatches);
    assertFalse(autoCommit.batchMode);
  }
}

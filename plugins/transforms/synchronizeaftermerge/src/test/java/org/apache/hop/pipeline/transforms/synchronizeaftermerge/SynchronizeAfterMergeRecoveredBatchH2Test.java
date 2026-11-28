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

import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_DONE;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_FAILED;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_UNTOUCHED;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_WRITTEN;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.counters;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.mixedRows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.updates;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.DRIVER;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.NOT_FOUND;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.SEED;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.bad;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.delete;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.in;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.insert;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.labels;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.nothing;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.range;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.rows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SimulatedBatchDriver.Shape;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.H2OracleFlagsDatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.H2PostgresFlagsDatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.H2SavepointsDatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Synchronize after merge on a connection whose batch failure contract is known: the INSERT, UPDATE
 * and DELETE batches are recovered with RecoverableBatch, also with an error hop. Every row must
 * reach exactly one stream, in input order, once it is committed or refused; the table must hold
 * exactly the rows on the main output, and the counters must be exact. The batches run at the same
 * moments as before, see SynchronizeAfterMergeBatchBehaviourH2Test, so in the same order.
 *
 * <p>H2 declares MARKS_FAILED_ROWS. The other contracts are set on the connection, each with H2 and
 * with a simulated driver that behaves the way the contract describes, because each of them has to
 * route the rows correctly on any driver whose behaviour it covers.
 */
class SynchronizeAfterMergeRecoveredBatchH2Test {

  /** H2 that declares no contract, as a dialect that extends it does. */
  @DatabaseMetaPlugin(type = "H2_SAM_UNVALIDATED", typeDescription = "H2, not validated")
  public static class UnvalidatedH2DatabaseMeta extends H2DatabaseMeta {}

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2PostgresFlagsDatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2OracleFlagsDatabaseMeta.class);
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

  static DatabaseMeta database(String type, String contract) {
    DatabaseMeta databaseMeta = SynchronizeAfterMergeBatchBehaviourH2Test.database(type);
    if (contract != null) {
      databaseMeta.setBatchFailureContractOverride(contract);
    }
    return databaseMeta;
  }

  static SynchronizeAfterMergeRun recovered(DatabaseMeta db, Setup setup) {
    return SynchronizeAfterMergeRun.on(db)
        .driver(setup.driver())
        .batch(true)
        .errorHop(true)
        .watchCommits();
  }

  /**
   * Every row on the main output is committed when it leaves, and the statement of every row the
   * database refused on the error hop is not.
   */
  static void assertPassedOnAfterTheCommit(Result r) {
    assertEquals(labels(), r.passedOnBeforeTheirCommit());
    List<Object> codes = r.errorCodes();
    for (int i = 0; i < r.errors.size(); i++) {
      Object[] row = r.errors.get(i);
      String committed = r.committedAtError.get(i);
      boolean applied =
          switch ((String) row[2]) {
              // A delete whose key was not found deleted nothing either
            case SynchronizeAfterMergeRun.DELETE ->
                committed == null && !"SUYNC001".equals(codes.get(i));
            case SynchronizeAfterMergeRun.UPDATE, SynchronizeAfterMergeRun.INSERT ->
                row[1].equals(committed);
            default -> false;
          };
      assertFalse(applied, SynchronizeAfterMergeRun.label(row) + " is on the error hop, but done");
    }
  }

  /** Recovered batches: batching on, also with an error hop, and no savepoint per row. */
  static void assertRecovered(Result r) {
    assertTrue(r.recoveredBatches);
    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertFalse(r.logged("Batch mode is disabled!"));
  }

  // ----------------------------------------------------------------------------------------------
  // Twelve rows, the three statements taking turns
  // ----------------------------------------------------------------------------------------------

  /** id % 3: 1 updates, 2 inserts, 0 deletes. */
  static String operation(long id) {
    return switch ((int) (id % 3)) {
      case 1 -> SynchronizeAfterMergeRun.UPDATE;
      case 2 -> SynchronizeAfterMergeRun.INSERT;
      default -> SynchronizeAfterMergeRun.DELETE;
    };
  }

  static boolean contains(long[] ids, long id) {
    for (long candidate : ids) {
      if (candidate == id) {
        return true;
      }
    }
    return false;
  }

  /**
   * Rows 1-12, the operation by {@link #operation(long)}. The bad ones fail: an update sets the
   * name the table refuses, an insert's id is in the table already, a delete's row has a child.
   */
  static List<Object[]> cycle(long... bad) {
    return cycle(12, bad);
  }

  /** The same for rows 1 to n. */
  static List<Object[]> cycle(int n, long... bad) {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      Object[] row = SynchronizeAfterMergeRun.row(operation(id), id, "r" + id);
      rows.add(
          contains(bad, id) && SynchronizeAfterMergeRun.UPDATE.equals(operation(id))
              ? bad(row)
              : row);
    }
    return rows;
  }

  /** The table {@link #cycle(long...)} runs on: the updates, deletes and bad inserts are in it. */
  static void createCycleTarget(DatabaseMeta db, long... bad) throws Exception {
    createCycleTarget(db, 12, bad);
  }

  /** The same for rows 1 to n. */
  static void createCycleTarget(DatabaseMeta db, int n, long... bad) throws Exception {
    List<Long> seeded = new ArrayList<>();
    List<Long> referred = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      String operation = operation(id);
      if (!SynchronizeAfterMergeRun.INSERT.equals(operation) || contains(bad, id)) {
        seeded.add(id);
      }
      if (SynchronizeAfterMergeRun.DELETE.equals(operation) && contains(bad, id)) {
        referred.add(id);
      }
    }
    SynchronizeAfterMergeRun.createTarget(
        db, SynchronizeAfterMergeRun.TARGET, seeded.stream().mapToLong(Long::longValue).toArray());
    SynchronizeAfterMergeRun.refer(
        db,
        SynchronizeAfterMergeRun.TARGET,
        referred.stream().mapToLong(Long::longValue).toArray());
  }

  /** The table after {@link #cycle(long...)} when exactly the bad rows fail. */
  static List<String> cycleTable(long... bad) {
    return cycleTable(12, bad);
  }

  /** The same for rows 1 to n. */
  static List<String> cycleTable(int n, long... bad) {
    List<String> lines = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      boolean failed = contains(bad, id);
      if (failed) {
        lines.add(id + ":" + SEED);
      } else if (!SynchronizeAfterMergeRun.DELETE.equals(operation(id))) {
        lines.add(id + ":r" + id);
      }
    }
    return lines;
  }

  static List<String> cycleLabels(boolean failed, long... bad) {
    return cycleLabels(12, failed, bad);
  }

  /** The same for rows 1 to n. */
  static List<String> cycleLabels(int n, boolean failed, long... bad) {
    List<String> labels = new ArrayList<>();
    for (Object[] row : cycle(n, bad)) {
      if (contains(bad, (Long) row[0]) == failed) {
        labels.add(SynchronizeAfterMergeRun.label(row));
      }
    }
    return labels;
  }

  /** lines input, output (inserts), updated, skipped, written, rejected of {@link #cycle}. */
  static List<Long> cycleCounters(long... bad) {
    return cycleCounters(12, bad);
  }

  /** The same for rows 1 to n. */
  static List<Long> cycleCounters(int n, long... bad) {
    long inserts = 0;
    long updates = 0;
    for (long id = 1; id <= n; id++) {
      if (!contains(bad, id)) {
        if (SynchronizeAfterMergeRun.INSERT.equals(operation(id))) {
          inserts++;
        } else {
          updates++;
        }
      }
    }
    return counters(0, inserts, updates, 0, inserts + updates, bad.length);
  }

  static final long[][] BAD_SETS = {
    {}, {1}, {2}, {3}, {12}, {1, 2, 3}, {4, 5, 6, 7}, {2, 5, 8, 11}, {3, 7, 11}, range(1, 12)
  };

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    setups()
        .forEach(
            setup -> {
              for (int commitSize : new int[] {1, 2, 3, 5, 100}) {
                for (long[] bad : BAD_SETS) {
                  arguments.add(Arguments.of(setup, commitSize, bad));
                }
              }
            });
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0}, commit size {1}, bad rows {2}")
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(Setup setup, int commitSize, long[] bad)
      throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    createCycleTarget(db, bad);

    Result r = recovered(db, setup).commitSize(commitSize).run(cycle(bad));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(cycleLabels(false, bad), r.outputLabels());
    assertEquals(cycleLabels(true, bad), r.errorLabels());
    assertEquals(Collections.nCopies(bad.length, "SUYNC002"), r.errorCodes());
    assertEquals(Collections.nCopies(bad.length, DRIVER), r.errorKinds());
    assertEquals(cycleTable(bad), SynchronizeAfterMergeRun.table(db));
    assertEquals(cycleCounters(bad), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  // ----------------------------------------------------------------------------------------------
  // Rows outside the batches
  // ----------------------------------------------------------------------------------------------

  static Stream<Arguments> setupsAndCommitSizes() {
    List<Arguments> arguments = new ArrayList<>();
    setups()
        .forEach(
            setup -> {
              for (int commitSize : new int[] {1, 2, 3, 100}) {
                arguments.add(Arguments.of(setup, commitSize));
              }
            });
    return arguments.stream();
  }

  /**
   * Fixed by #9768: a skipped row keeps its place between the rows of the batches, and only the
   * rows the database refuses go to the error hop, in input order. With an error hop H2 used to
   * switch batching off, see SynchronizeAfterMergeBatchBehaviourH2Test.
   */
  @ParameterizedTest(name = "{0}, commit size {1}")
  @MethodSource("setupsAndCommitSizes")
  void skippedRowsKeepTheirPlace(Setup setup, int commitSize) throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1, 2, 3, 4, 5, 6);
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);

    Result r = recovered(db, setup).commitSize(commitSize).run(mixedRows());

    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(Collections.nCopies(3, "SUYNC002"), r.errorCodes());
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * With "perform lookup" a key that is not in the table goes to the error hop with SUYNC001, the
   * key field named and a message that names its own key, in its place between the rows of the
   * batches. A row to skip is looked up as well: s6 is found, and passed on in its place.
   */
  @ParameterizedTest(name = "{0}, commit size {1}")
  @MethodSource("setupsAndCommitSizes")
  void aMissingKeyKeepsItsPlaceAndNamesItsOwnKey(Setup setup, int commitSize) throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1, 2, 3, 4, 6);

    Result r =
        recovered(db, setup)
            .lookup(true)
            .commitSize(commitSize)
            .run(
                rows(
                    update(1),
                    update(11),
                    insert(5),
                    delete(12),
                    bad(update(2)),
                    delete(3),
                    nothing(6),
                    update(4)));

    assertEquals(labels("u1", "i5", "d3", "s6", "u4"), r.outputLabels());
    assertEquals(labels("u11", "d12", "u2"), r.errorLabels());
    assertEquals(List.of("SUYNC001", "SUYNC001", "SUYNC002"), r.errorCodes());
    assertEquals(List.of(NOT_FOUND, NOT_FOUND, DRIVER), r.errorKinds());
    List<Object> descriptions = r.errorField(SynchronizeAfterMergeRun.ERROR_DESCRIPTIONS);
    assertTrue(((String) descriptions.get(0)).endsWith("[11]"), (String) descriptions.get(0));
    assertTrue(((String) descriptions.get(1)).endsWith("[12]"), (String) descriptions.get(1));
    assertEquals(
        List.of("id", "id"), r.errorField(SynchronizeAfterMergeRun.ERROR_FIELDS).subList(0, 2));
    assertEquals(List.of("1:r1", "2:x", "4:r4", "5:r5", "6:x"), SynchronizeAfterMergeRun.table(db));
    // Every row but the insert is looked up
    assertEquals(counters(7, 1, 3, 1, 5, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * When commit size rows wait behind a batch that has not run yet, every batch runs, as at the end
   * of a single-threaded iteration, so that they don't pile up: with commit size 5 the fifth
   * skipped row behind d1 lets d1 run, and the rows after it leave at once.
   */
  @Test
  void rowsWaitingBehindABatchMakeItRun() throws Exception {
    DatabaseMeta db = database("H2", null);
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1, 2);
    List<Object[]> rows = new ArrayList<>();
    rows.add(delete(1));
    for (long id = 10; id <= 21; id++) {
      rows.add(nothing(id));
    }
    rows.add(delete(2));

    Result r = recovered(db, new Setup(null, null)).commitSize(5).run(rows);

    assertEquals(List.of(0, 0, 0, 0, 0, 0, 6, 7, 8, 9, 10, 11, 12, 13, 13), r.outputSizeAtRead);
    assertEquals(14, r.output.size());
    assertEquals(List.of(), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 0, 2, 12, 14, 0), r.counters());
    assertPassedOnAfterTheCommit(r);
  }

  /**
   * The batches run where batch mode ran them: the database's count of commit size rows runs the
   * batch at u4 and u8, the statement's count of commit size + 1 rows at u5 and u10. Every row
   * leaves once its batch is committed.
   */
  @Test
  void theBatchesRunWhereBatchModeRanThem() throws Exception {
    DatabaseMeta db = database("H2", null);
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, range(1, 10));

    Result r = recovered(db, new Setup(null, null)).commitSize(4).run(updates(9));

    assertEquals(List.of(0, 0, 0, 0, 4, 5, 5, 5, 8, 8, 9), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1), r.errorSizeAtRead);
    assertEquals(labels("u9"), r.errorLabels());
    assertPassedOnAfterTheCommit(r);
  }

  // ----------------------------------------------------------------------------------------------
  // Without an error hop
  // ----------------------------------------------------------------------------------------------

  /**
   * Without an error hop a failed batch is rolled back and stops the transform, whatever the
   * contract. Fixed by #9768: the rows of the batches committed before it are passed on and
   * counted, u6-u8 too, none of the failed batch.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops(Setup setup) throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, range(1, 10));

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .driver(setup.driver())
            .batch(true)
            .errorHop(false)
            .commitSize(4)
            .watchCommits()
            .run(updates(9));

    assertEquals(labels("u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8"), r.outputLabels());
    assertEquals(
        List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "6:r6", "7:r7", "8:r8", "9:x", "10:x"),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(0, 0, 8, 0, 8, 0), r.counters());
    assertEquals(labels(), r.passedOnBeforeTheirCommit());
    assertTrue(r.loggedError("Error running the update batch of table [target]"));
    assertTrue(r.recoveredBatches);
  }

  /**
   * Without an error hop, a failed batch at the end of the input: the update batch runs first on
   * this table and fails on u4. Fixed by #9768: no row is counted that was not written; the skipped
   * row, which waited behind u1, is passed on before the transform stops.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void withoutAnErrorHopNothingIsCountedThatWasNotWritten(Setup setup) throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1, 2, 3, 4, 5, 6);
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .driver(setup.driver())
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .run(mixedRows());

    assertEquals(labels("s8"), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(0, 0, 0, 1, 1, 0), r.counters());
  }

  /**
   * Fixed by #9768: rows leave only once their own batch is committed, here i1 when the many rows
   * waiting behind it make every batch run. Without an error hop, as without failures.
   */
  @Test
  void withoutAnErrorHopRowsLeaveOnlyWithTheirOwnCommit() throws Exception {
    DatabaseMeta db = database("H2", null);
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 2, 3, 4);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(2)
            .watchCommits()
            .run(rows(insert(1), update(2), update(3), update(4), insert(5)));

    assertEquals(labels("i1", "u2", "u3", "u4", "i5"), r.outputLabels());
    assertEquals(labels(), r.passedOnBeforeTheirCommit());
    assertEquals(
        List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 2, 3, 0, 5, 0), r.counters());
  }

  /** Fixed by #9768: a skipped row keeps its place, also without an error hop. */
  @Test
  void withoutAnErrorHopSkippedRowsKeepTheirPlace() throws Exception {
    DatabaseMeta db = database("H2", null);
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 2);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(2)
            .watchCommits()
            .run(rows(insert(1), update(2), nothing(7), nothing(8), nothing(9), insert(3)));

    assertEquals(labels("i1", "u2", "s7", "s8", "s9", "i3"), r.outputLabels());
    assertEquals(labels(), r.passedOnBeforeTheirCommit());
    assertEquals(List.of("1:r1", "2:r2", "3:r3"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 2, 1, 3, 6, 0), r.counters());
  }

  /**
   * Fixed by #9768: a batch that fails in the middle without an error hop counts no row: u1, whose
   * batch never ran, is dropped.
   */
  @Test
  void withoutAnErrorHopABatchThatFailsInTheMiddleCountsNothing() throws Exception {
    DatabaseMeta db = database("H2", null);
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1, 2, 3, 4, 5, 6);
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);

    Result r =
        SynchronizeAfterMergeRun.on(db).batch(true).errorHop(false).commitSize(3).run(mixedRows());

    assertEquals(labels(), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(0, 0, 0, 0, 0, 0), r.counters());
    assertTrue(r.loggedError("rows of the batches were not written"));
  }

  // ----------------------------------------------------------------------------------------------
  // batchComplete() and dispose()
  // ----------------------------------------------------------------------------------------------

  /** At the end of a single-threaded iteration the batches are recovered like any other. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void batchCompleteRecoversTheBatches(Setup setup) throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1, 2, 3, 4);
    List<List<String>> tableAtBatchComplete = new ArrayList<>();

    Result r =
        recovered(db, setup)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 4) {
                    transform.batchComplete();
                    tableAtBatchComplete.add(SynchronizeAfterMergeRun.table(db));
                  }
                })
            .run(rows(update(1), insert(2), bad(update(3)), delete(4), insert(5), update(3)));

    assertEquals(List.of(List.of("1:r1", "2:x", "3:x")), tableAtBatchComplete);
    assertEquals(List.of(0, 0, 0, 0, 2, 2, 2), r.outputSizeAtRead);
    assertEquals(labels("u1", "d4", "i5", "u3"), r.outputLabels());
    assertEquals(labels("i2", "u3"), r.errorLabels());
    assertEquals(List.of("1:r1", "2:x", "3:r3", "5:r5"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 1, 3, 0, 4, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * Fixed by #9768: the rows a stopped transform still has in its batches are dropped, not
   * committed, so the table holds exactly the rows that were passed on.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void aStoppedTransformDropsItsBatches(Setup setup) throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1, 2, 3);

    Result r =
        recovered(db, setup)
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
    assertEquals(List.of("1:x", "2:x", "3:x"), SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 0, 0, 0, 0, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.startsWith("3 rows of the batches")));
    assertRecovered(r);
  }

  /** A transform that fails drops the rows of its batches: none of them is committed. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void aFailedTransformDropsItsBatches(Setup setup) throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, range(1, 10));

    Result r =
        recovered(db, setup)
            .commitSize(4)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 7) {
                    transform.setErrors(1);
                    transform.stopAll();
                  }
                })
            .run(updates());

    assertEquals(labels("u1", "u2", "u3", "u4", "u5"), r.outputLabels());
    assertEquals(
        List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "6:x", "7:x", "8:x", "9:x", "10:x"),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 0, 5, 0, 5, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.startsWith("2 rows of the batches")));
  }

  // ----------------------------------------------------------------------------------------------
  // Two tables, the table name in a field
  // ----------------------------------------------------------------------------------------------

  /**
   * Every table has batches of its own; a refused row in each goes to the error hop in its place.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void twoTablesFromAField(Setup setup) throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    SynchronizeAfterMergeRun.createTarget(db, "sam_a", 1, 2);
    SynchronizeAfterMergeRun.createTarget(db, "sam_b", 1, 2);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .driver(setup.driver())
            .tableNameInField(true)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .run(
                rows(
                    in("sam_a", update(1)),
                    in("sam_b", insert(1)),
                    in("sam_a", insert(3)),
                    in("sam_b", bad(update(2))),
                    in("sam_a", delete(2)),
                    in("sam_b", update(1)),
                    in("sam_a", insert(4)),
                    in("sam_b", delete(1))));

    assertEquals(
        labels("u1@sam_a", "i3@sam_a", "d2@sam_a", "u1@sam_b", "i4@sam_a", "d1@sam_b"),
        r.outputLabels());
    assertEquals(labels("i1@sam_b", "u2@sam_b"), r.errorLabels());
    assertEquals(List.of("1:r1", "3:r3", "4:r4"), SynchronizeAfterMergeRun.table(db, "sam_a"));
    assertEquals(List.of("2:x"), SynchronizeAfterMergeRun.table(db, "sam_b"));
    assertEquals(counters(0, 2, 4, 0, 6, 2), r.counters());
    assertRecovered(r);
  }

  // ----------------------------------------------------------------------------------------------
  // The statement order
  // ----------------------------------------------------------------------------------------------

  static Stream<Arguments> orderCases() {
    List<Arguments> arguments = new ArrayList<>();
    setups()
        .forEach(
            setup -> {
              for (boolean errorHop : new boolean[] {true, false}) {
                for (String table : new String[] {"target", "t"}) {
                  for (int commitSize : new int[] {100, 2}) {
                    for (String name :
                        SynchronizeAfterMergeStatementOrderH2Test.CASES.keySet().stream()
                            .sorted()
                            .toList()) {
                      arguments.add(Arguments.of(setup, errorHop, table, commitSize, name));
                    }
                  }
                }
              }
            });
    return arguments.stream();
  }

  /**
   * Whatever the contract, the statements of one key run in input order, see
   * SynchronizeAfterMergeStatementOrderH2Test: a row whose key waits in a batch of another
   * statement makes the batches run first. With an error hop a refused row goes to it.
   */
  @ParameterizedTest(name = "{0}, error hop {1}, table {2}, commit size {3}: {4}")
  @MethodSource("orderCases")
  void operationsOnOneKeyRunInInputOrder(
      Setup setup, boolean errorHop, String table, int commitSize, String name) throws Exception {
    DatabaseMeta db = database("H2", setup.contract());
    SynchronizeAfterMergeRun.createTarget(db, table, 2);

    Result r =
        recovered(db, setup)
            .errorHop(errorHop)
            .table(table)
            .commitSize(commitSize)
            .run(SynchronizeAfterMergeStatementOrderH2Test.CASES.get(name));

    List<String> expected = SynchronizeAfterMergeStatementOrderH2Test.inInputOrder(name, errorHop);
    if (expected == SynchronizeAfterMergeStatementOrderH2Test.FAILS) {
      assertEquals(1, r.errorCount);
      assertEquals(List.of("2:x"), SynchronizeAfterMergeRun.table(db, table));
    } else {
      assertEquals(0, r.errorCount);
      assertEquals(expected, SynchronizeAfterMergeRun.table(db, table));
    }
    assertTrue(r.recoveredBatches);
  }

  // ----------------------------------------------------------------------------------------------
  // Which path a connection takes
  // ----------------------------------------------------------------------------------------------

  /**
   * A dialect with PostgreSQL's flags, which would run every row behind a savepoint with an error
   * hop, batches and recovers once its contract is set on the connection.
   */
  @Test
  void theConnectionOptionRecoversBatchesOnAnyDialect() throws Exception {
    DatabaseMeta db =
        database(SynchronizeAfterMergeBatchBehaviourH2Test.POSTGRES_FLAGS, "LOSES_WHOLE_BATCH");
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1, 2, 3, 4, 5, 6);
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);

    Result r = recovered(db, new Setup(null, null)).commitSize(4).run(mixedRows());

    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
    assertTrue(r.loggedDetailed("batch failure contract LOSES_WHOLE_BATCH (from CONNECTION)"));
  }

  /** H2's own contract, logged on the detailed level. */
  @Test
  void theBatchSupportIsLogged() throws Exception {
    DatabaseMeta db = database("H2", null);
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1);

    Result r = recovered(db, new Setup(null, null)).run(rows(update(1)));

    assertTrue(r.loggedDetailed("Batch support: batch failure contract MARKS_FAILED_ROWS"));
  }

  /**
   * Without a validated contract the batches are handled as before, see
   * SynchronizeAfterMergeBatchBehaviourH2Test: with an error hop H2 switches batching off.
   */
  private static void assertHandledAsBefore(Result r) {
    assertFalse(r.recoveredBatches);
    assertFalse(r.batchMode);
    assertTrue(r.logged("Batch mode is disabled!"));
    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(Collections.nCopies(3, "SUYNC001"), r.errorCodes());
  }

  static Stream<Arguments> legacyPaths() {
    return Stream.of(
        Arguments.of("H2", null, "LEGACY"),
        Arguments.of("H2", "UNKNOWN", null),
        Arguments.of("H2_SAM_UNVALIDATED", null, null));
  }

  /**
   * HOP_DATABASE_BATCH_RECOVERY=LEGACY, "As before" on the connection, and a dialect that extends
   * H2 (it has not been measured) keep the handling as before.
   */
  @ParameterizedTest(name = "{0}, contract on the connection {1}, recovery {2}")
  @MethodSource("legacyPaths")
  void withoutAValidatedContractTheBatchesAreHandledAsBefore(
      String type, String contract, String recovery) throws Exception {
    DatabaseMeta db = database(type, contract);
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, 1, 2, 3, 4, 5, 6);
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .run(mixedRows());

    assertHandledAsBefore(r);
  }

  /** Batch updates off, or commit size 0 (auto-commit): the rows run one by one as before. */
  @Test
  void batchUpdatesOffOrAutoCommitKeepTheRowByRowPath() throws Exception {
    DatabaseMeta off = database("H2", null);
    SynchronizeAfterMergeRun.createTarget(off, SynchronizeAfterMergeRun.TARGET, 1);
    Result rowByRow =
        SynchronizeAfterMergeRun.on(off)
            .batch(false)
            .errorHop(true)
            .commitSize(5)
            .run(rows(update(1)));
    assertFalse(rowByRow.recoveredBatches);
    assertFalse(rowByRow.batchMode);

    DatabaseMeta autoCommitDb = database("H2", null);
    SynchronizeAfterMergeRun.createTarget(autoCommitDb, SynchronizeAfterMergeRun.TARGET, 1);
    Result autoCommit =
        SynchronizeAfterMergeRun.on(autoCommitDb)
            .batch(true)
            .errorHop(true)
            .commitSize(0)
            .run(rows(update(1)));
    assertFalse(autoCommit.recoveredBatches);
    assertFalse(autoCommit.batchMode);
  }
}

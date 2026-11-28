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
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourPostgresIT.assertPostgresMessages;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourPostgresIT.mixedTarget;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourPostgresIT.target;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.assertPassedOnAfterTheCommit;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.assertRecovered;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.createCycleTarget;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.cycle;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.cycleCounters;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.cycleLabels;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.cycleTable;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.labels;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Synchronize after merge on PostgreSQL, whose contract LOSES_WHOLE_BATCH is validated: a failed
 * batch keeps none of its rows and aborts the transaction. With an error hop the rows are still
 * batched, without a savepoint per row, and a failed batch is recovered: every row reaches exactly
 * one stream, in input order, after its commit.
 */
class SynchronizeAfterMergeRecoveredBatchPostgresIT {

  @BeforeAll
  static void setUpOnce() throws Exception {
    SynchronizeAfterMergeBatchBehaviourPostgresIT.setUpOnce();
  }

  @AfterAll
  static void tearDownOnce() {
    SynchronizeAfterMergeBatchBehaviourPostgresIT.tearDownOnce();
  }

  static DatabaseMeta database(String type, String contract) {
    DatabaseMeta databaseMeta = SynchronizeAfterMergeBatchBehaviourPostgresIT.databaseMeta(type);
    if (contract != null) {
      databaseMeta.setBatchFailureContractOverride(contract);
    }
    return databaseMeta;
  }

  /** The table of SynchronizeAfterMergeRecoveredBatchH2Test.cycle(n, bad), created anew. */
  static DatabaseMeta cycleTarget(int n, long... bad) throws Exception {
    DatabaseMeta db = database("POSTGRESQL", null);
    SynchronizeAfterMergeRun.execute(
        db,
        "DROP TABLE IF EXISTS " + SynchronizeAfterMergeRun.TARGET + "_child",
        "DROP TABLE IF EXISTS " + SynchronizeAfterMergeRun.TARGET);
    createCycleTarget(db, n, bad);
    return db;
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    for (int commitSize : new int[] {1, 3, 5, 100}) {
      for (long[] bad : SynchronizeAfterMergeRecoveredBatchH2Test.BAD_SETS) {
        arguments.add(Arguments.of(commitSize, bad));
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "commit size {0}, bad rows {1}")
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(int commitSize, long[] bad) throws Exception {
    DatabaseMeta db = cycleTarget(12, bad);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(cycle(bad));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(cycleLabels(false, bad), r.outputLabels());
    assertEquals(cycleLabels(true, bad), r.errorLabels());
    assertEquals(Collections.nCopies(bad.length, "SUYNC002"), r.errorCodes());
    assertEquals(cycleTable(bad), SynchronizeAfterMergeRun.table(db));
    assertEquals(cycleCounters(bad), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * Fixed by #9768: the skipped row and the failed rows keep their place, and the failed rows carry
   * PostgreSQL's own message.
   */
  @ParameterizedTest(name = "commit size {0}")
  @MethodSource("commitSizes")
  void skippedRowsKeepTheirPlace(int commitSize) throws Exception {
    DatabaseMeta db = mixedTarget("POSTGRESQL");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(mixedRows());

    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(Collections.nCopies(3, "SUYNC002"), r.errorCodes());
    assertPostgresMessages(r);
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  static Stream<Integer> commitSizes() {
    return Stream.of(1, 3, 100);
  }

  /** id, name, op, tbl (unused), pattern: a LIKE pattern on the current name. */
  static IRowMeta patternRowMeta() {
    IRowMeta rowMeta = SynchronizeAfterMergeRun.idNameOpTableRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("pattern"));
    return rowMeta;
  }

  /**
   * The row with a pattern on the current name, or one PostgreSQL refuses: a pattern must not end
   * with the escape character. So the lookup of such a row fails.
   */
  static Object[] patterned(Object[] row, boolean badPattern) {
    return new Object[] {row[0], row[1], row[2], null, badPattern ? "\\" : "x%"};
  }

  /**
   * A lookup that fails aborts the transaction. It holds no INSERT, UPDATE or DELETE between two
   * batches, so it is rolled back, and only that row goes to the error hop: the rows waiting in the
   * three batches, and the rows after it, are written as usual.
   */
  @ParameterizedTest(name = "commit size {0}")
  @MethodSource("commitSizes")
  void aFailedLookupOnlyRejectsItsOwnRow(int commitSize) throws Exception {
    DatabaseMeta db = target("POSTGRESQL", SynchronizeAfterMergeRun.TARGET, range(1, 6));

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .lookup(true)
            .commitSize(commitSize)
            .watchCommits()
            .configure(
                meta -> {
                  SynchronizeAfterMergeMeta.KeyCondition key =
                      new SynchronizeAfterMergeMeta.KeyCondition();
                  key.setColumnName("name");
                  key.setCondition("LIKE");
                  key.setFieldName("pattern");
                  key.setFieldName2("");
                  meta.getLookup().getKeyConditions().add(key);
                })
            .run(
                patternRowMeta(),
                List.of(
                    patterned(SynchronizeAfterMergeRun.update(1), false),
                    patterned(SynchronizeAfterMergeRun.insert(7), false),
                    patterned(SynchronizeAfterMergeRun.delete(3), false),
                    patterned(SynchronizeAfterMergeRun.update(4), true),
                    patterned(SynchronizeAfterMergeRun.insert(8), false),
                    patterned(SynchronizeAfterMergeRun.delete(5), true),
                    patterned(SynchronizeAfterMergeRun.update(6), false)));

    assertEquals(labels("u1", "i7", "d3", "i8", "u6"), r.outputLabels());
    assertEquals(labels("u4", "d5"), r.errorLabels());
    assertEquals(List.of("SUYNC001", "SUYNC001"), r.errorCodes());
    assertTrue(
        r.errorField(SynchronizeAfterMergeRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("LIKE pattern must not end with escape")),
        r.errorField(SynchronizeAfterMergeRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(
        List.of("1:r1", "2:x", "4:x", "5:x", "6:r6", "7:r7", "8:r8"),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(3, 2, 3, 0, 5, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** Without an error hop a failed batch is rolled back and stops the transform. */
  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = target("POSTGRESQL", SynchronizeAfterMergeRun.TARGET, range(1, 10));

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(4)
            .watchCommits()
            .run(updates(9));

    assertEquals(labels("u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8"), r.outputLabels());
    assertEquals(labels(), r.passedOnBeforeTheirCommit());
    assertEquals(
        List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "6:r6", "7:r7", "8:r8", "9:x", "10:x"),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(0, 0, 8, 0, 8, 0), r.counters());
    assertTrue(r.recoveredBatches);
  }

  /**
   * Fixed by #9768: without an error hop no row is counted that was not written. The update batch
   * runs first on this table and fails; the skipped row waited behind u1, and is passed on.
   */
  @Test
  void withoutAnErrorHopNothingIsCountedThatWasNotWritten() throws Exception {
    DatabaseMeta db = mixedTarget("POSTGRESQL");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .run(mixedRows());

    assertEquals(labels("s8"), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(0, 0, 0, 1, 1, 0), r.counters());
  }

  /** Larger batches with a few bad rows, as in a real load. */
  @Test
  void largerBatches() throws Exception {
    long[] bad = {1, 97, 194, 500, 501, 1_000, 1_999, 2_000};
    DatabaseMeta db = cycleTarget(2_000, bad);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(500)
            .run(cycle(2_000, bad));

    assertEquals(cycleLabels(2_000, false, bad), r.outputLabels());
    assertEquals(cycleLabels(2_000, true, bad), r.errorLabels());
    assertEquals(cycleTable(2_000, bad), SynchronizeAfterMergeRun.table(db));
    assertEquals(cycleCounters(2_000, bad), r.counters());
    assertRecovered(r);
  }

  /** HOP_DATABASE_BATCH_RECOVERY=LEGACY goes back to a savepoint per row. */
  @Test
  void theLegacyVariableGoesBackToASavepointPerRow() throws Exception {
    DatabaseMeta db = mixedTarget("POSTGRESQL");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(mixedRows());

    assertFalse(r.recoveredBatches);
    assertTrue(r.useSavepoints);
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
  }

  /** "As before" on the connection does the same, for that connection. */
  @Test
  void unknownOnTheConnectionGoesBackToASavepointPerRow() throws Exception {
    DatabaseMeta db = mixedTarget("POSTGRESQL");
    db.setBatchFailureContractOverride("UNKNOWN");

    Result r =
        SynchronizeAfterMergeRun.on(db).batch(true).errorHop(true).commitSize(4).run(mixedRows());

    assertFalse(r.recoveredBatches);
    assertTrue(r.useSavepoints);
  }

  /**
   * A dialect that extends PostgreSQL declares no contract and keeps a savepoint per row, unless
   * the contract is set on the connection.
   */
  @Test
  void aLookAlikeRecoversBatchesWithTheContractOnTheConnection() throws Exception {
    DatabaseMeta plain = mixedTarget("POSTGRESQL_SAM_LOOKALIKE");
    Result before =
        SynchronizeAfterMergeRun.on(plain)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(mixedRows());
    assertFalse(before.recoveredBatches);
    assertTrue(before.useSavepoints);

    DatabaseMeta db = mixedTarget("POSTGRESQL_SAM_LOOKALIKE");
    db.setBatchFailureContractOverride("LOSES_WHOLE_BATCH");
    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .watchCommits()
            .run(mixedRows());

    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  static Stream<Arguments> orderCases() {
    List<Arguments> arguments = new ArrayList<>();
    for (boolean errorHop : new boolean[] {true, false}) {
      for (String table : new String[] {"target", "t"}) {
        for (int commitSize : new int[] {100, 2}) {
          for (String name : SynchronizeAfterMergeStatementOrderH2Test.CASES.keySet()) {
            arguments.add(Arguments.of(errorHop, table, commitSize, name));
          }
        }
      }
    }
    return arguments.stream();
  }

  /**
   * The statements of one key run in input order, see SynchronizeAfterMergeStatementOrderH2Test: a
   * row whose key waits in a batch of another statement makes the batches run first.
   */
  @ParameterizedTest(name = "error hop {0}, table {1}, commit size {2}: {3}")
  @MethodSource("orderCases")
  void operationsOnOneKeyRunInInputOrder(
      boolean errorHop, String table, int commitSize, String name) throws Exception {
    DatabaseMeta db = target("POSTGRESQL", table, 2);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .table(table)
            .batch(true)
            .errorHop(errorHop)
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
}

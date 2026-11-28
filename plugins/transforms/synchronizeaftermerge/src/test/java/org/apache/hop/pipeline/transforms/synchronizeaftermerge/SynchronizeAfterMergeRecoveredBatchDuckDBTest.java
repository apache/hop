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
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_WRITTEN;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.counters;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.mixedRows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.updates;
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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Synchronize after merge on DuckDB, whose contract LOSES_WHOLE_BATCH is validated: a failed batch
 * keeps none of its rows and the driver closes the statement; there are no savepoints. Failed
 * batches are recovered: every row reaches exactly one stream, in input order, after its commit.
 */
class SynchronizeAfterMergeRecoveredBatchDuckDBTest {

  @TempDir static Path folder;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  static DatabaseMeta database() {
    String file = folder.resolve(UUID.randomUUID() + ".duckdb").toString();
    return new DatabaseMeta("duckdb", "DuckDB", "Native", "", file, "", "", "");
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
    DatabaseMeta db = database();
    createCycleTarget(db, bad);

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
   * Fixed by #9768: before, the first failed row aborted the transaction for every row after it.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 100})
  void skippedRowsKeepTheirPlace(int commitSize) throws Exception {
    DatabaseMeta db = database();
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, range(1, 6));
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);

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
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** id, name, op, tbl (unused), key: the id once more, as text. */
  static IRowMeta keyTextRowMeta() {
    IRowMeta rowMeta = SynchronizeAfterMergeRun.idNameOpTableRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("key"));
    return rowMeta;
  }

  /** The row with its id as text in the key, or a text DuckDB can't make a number of. */
  static Object[] keyed(Object[] row, boolean badKey) {
    return new Object[] {row[0], row[1], row[2], null, badKey ? "no number" : row[0].toString()};
  }

  /**
   * DuckDB closes a statement that fails. A lookup that fails is rolled back and its statement is
   * prepared again, so only the row whose lookup failed goes to the error hop: the rows waiting in
   * the three batches, and the rows after it, are written as usual.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 4, 100})
  void aFailedLookupOnlyRejectsItsOwnRow(int commitSize) throws Exception {
    DatabaseMeta db = database();
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, range(1, 6));

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .lookup(true)
            .commitSize(commitSize)
            .watchCommits()
            .configure(SynchronizeAfterMergeRecoveredBatchDuckDBTest::lookUpByTheKeyAsWell)
            .run(
                keyTextRowMeta(),
                List.of(
                    keyed(SynchronizeAfterMergeRun.update(1), false),
                    keyed(SynchronizeAfterMergeRun.insert(7), false),
                    keyed(SynchronizeAfterMergeRun.delete(3), false),
                    keyed(SynchronizeAfterMergeRun.update(4), true),
                    keyed(SynchronizeAfterMergeRun.insert(8), false),
                    keyed(SynchronizeAfterMergeRun.delete(5), true),
                    keyed(SynchronizeAfterMergeRun.update(6), false),
                    keyed(SynchronizeAfterMergeRun.nothing(2), false)));

    assertEquals(labels("u1", "i7", "d3", "i8", "u6", "s2"), r.outputLabels());
    assertEquals(labels("u4", "d5"), r.errorLabels());
    assertEquals(List.of("SUYNC001", "SUYNC001"), r.errorCodes());
    assertTrue(
        r.errorField(SynchronizeAfterMergeRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("Conversion Error")),
        r.errorField(SynchronizeAfterMergeRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(
        List.of("1:r1", "2:x", "4:x", "5:x", "6:r6", "7:r7", "8:r8"),
        SynchronizeAfterMergeRun.table(db));
    // A lookup that fails is not counted as a line read
    assertEquals(counters(4, 2, 3, 1, 6, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** Looks the rows up by id, and once more by the key: {@code id = key}. */
  static void lookUpByTheKeyAsWell(SynchronizeAfterMergeMeta meta) {
    SynchronizeAfterMergeMeta.KeyCondition key = new SynchronizeAfterMergeMeta.KeyCondition();
    key.setColumnName("id");
    key.setCondition("=");
    key.setFieldName("key");
    key.setFieldName2("");
    meta.getLookup().getKeyConditions().add(key);
  }

  /** Without an error hop a failed batch stops the transform; the batches before it stay. */
  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = database();
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, range(1, 10));

    Result r =
        SynchronizeAfterMergeRun.on(db).batch(true).errorHop(false).commitSize(4).run(updates(9));

    assertEquals(labels("u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8"), r.outputLabels());
    assertEquals(
        List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "6:r6", "7:r7", "8:r8", "9:x", "10:x"),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(0, 0, 8, 0, 8, 0), r.counters());
    assertTrue(r.recoveredBatches);
  }

  /** Many batches, several of them failing: the statements are prepared again after each. */
  @Test
  void manyFailedBatches() throws Exception {
    DatabaseMeta db = database();
    long[] bad = {1, 2, 50, 99, 100, 101, 250, 377, 499, 500};
    createCycleTarget(db, 500, bad);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(50)
            .run(cycle(500, bad));

    assertEquals(
        SynchronizeAfterMergeRecoveredBatchH2Test.cycleLabels(500, false, bad), r.outputLabels());
    assertEquals(
        SynchronizeAfterMergeRecoveredBatchH2Test.cycleLabels(500, true, bad), r.errorLabels());
    assertEquals(
        SynchronizeAfterMergeRecoveredBatchH2Test.cycleTable(500, bad),
        SynchronizeAfterMergeRun.table(db));
    assertEquals(SynchronizeAfterMergeRecoveredBatchH2Test.cycleCounters(500, bad), r.counters());
    assertTrue(r.recoveredBatches);
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
    DatabaseMeta db = database();
    SynchronizeAfterMergeRun.createTarget(db, table, 2);

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

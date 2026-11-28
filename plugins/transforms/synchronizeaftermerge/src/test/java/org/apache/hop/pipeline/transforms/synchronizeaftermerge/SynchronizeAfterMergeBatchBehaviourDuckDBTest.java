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

import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MANY_DONE;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_UNTOUCHED;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.counters;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.manyLabels;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.manyRows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.mixedRows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.labels;
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
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins what Synchronize after merge does on DuckDB when a row fails. DuckDB's driver loses the
 * whole batch on a failure, together with everything else in the transaction since the last commit,
 * and closes the statement; it has no savepoints. A failed statement aborts the transaction until
 * it is rolled back. With an error hop DuckDB switches batching off, as H2 does: its {@code
 * IsSupportsErrorHandlingOnBatchUpdates()} is true.
 *
 * <p>Since #9768 DuckDB's validated contract recovers failed batches when batch updates are on, see
 * SynchronizeAfterMergeRecoveredBatchDuckDBTest; with batch updates on the tests here run with
 * HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection, which keep the handling as
 * it was. With batch updates off the rows run one by one exactly as before, on every path.
 */
class SynchronizeAfterMergeBatchBehaviourDuckDBTest {

  @TempDir static Path folder;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  /** A DuckDB database file with the given table, holding the given ids named x. */
  static DatabaseMeta target(String table, long... ids) throws Exception {
    String file = folder.resolve(UUID.randomUUID() + ".duckdb").toString();
    DatabaseMeta databaseMeta =
        new DatabaseMeta("duckdb", "DuckDB", "Native", "", file, "", "", "");
    SynchronizeAfterMergeRun.createTarget(databaseMeta, table, ids);
    return databaseMeta;
  }

  /** The table of SynchronizeAfterMergeBatchBehaviourH2Test.mixedRows(). */
  static DatabaseMeta mixedTarget() throws Exception {
    DatabaseMeta db = target(SynchronizeAfterMergeRun.TARGET, 1, 2, 3, 4, 5, 6);
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);
    return db;
  }

  /** Recovery variable and contract on the connection: default, LEGACY, and "As before". */
  static Stream<Arguments> paths() {
    return Stream.of(
        Arguments.of(null, null), Arguments.of("LEGACY", null), Arguments.of(null, "UNKNOWN"));
  }

  /** LEGACY, and "As before" on the connection: the paths that keep today's batches. */
  static Stream<Arguments> legacyPaths() {
    return Stream.of(Arguments.of("LEGACY", null), Arguments.of(null, "UNKNOWN"));
  }

  /**
   * Every path with batch updates off; with batch updates on only the paths that keep today's
   * batches: since #9768 the default path recovers them, see
   * SynchronizeAfterMergeRecoveredBatchDuckDBTest.
   */
  static Stream<Arguments> pathsAndBatch() {
    List<Arguments> arguments = new ArrayList<>();
    paths().forEach(path -> arguments.add(Arguments.of(path.get()[0], path.get()[1], false)));
    legacyPaths().forEach(path -> arguments.add(Arguments.of(path.get()[0], path.get()[1], true)));
    return arguments.stream();
  }

  /**
   * Buggy: row by row with an error hop (with batch updates on as well, which the error hop
   * switches off), the failed insert of i2 aborts the transaction. Every row after it goes to the
   * error hop as well, and u1 and i7, passed on right after their statement, are lost with the
   * transaction.
   */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}, batch {2}")
  @MethodSource("pathsAndBatch")
  void errorHop_Buggy_aFailedRowAbortsTheTransaction(
      String recovery, String contract, boolean batch) throws Exception {
    DatabaseMeta db = mixedTarget();
    if (contract != null) {
      db.setBatchFailureContractOverride(contract);
    }

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(batch)
            .errorHop(true)
            .commitSize(3)
            .run(mixedRows());

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(batch, r.logged("Batch mode is disabled!"));
    assertEquals(labels("u1", "i7", "s8"), r.outputLabels());
    assertEquals(labels("i2", "d3", "u4", "d5", "i9", "u6", "i10"), r.errorLabels());
    assertEquals(Collections.nCopies(7, "SUYNC001"), r.errorCodes());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(0, r.errorCount);
    assertEquals(counters(0, 1, 1, 1, 3, 7), r.counters());
  }

  /**
   * Buggy: without an error hop the failed i2 stops the transform and rolls the transaction back;
   * u1 and i7, already passed on, are not in the table.
   */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}")
  @MethodSource("paths")
  void batchOff_noErrorHop_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(
      String recovery, String contract) throws Exception {
    DatabaseMeta db = mixedTarget();
    if (contract != null) {
      db.setBatchFailureContractOverride(contract);
    }

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(false)
            .commitSize(3)
            .run(mixedRows());

    assertEquals(labels("u1", "i7"), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(0, 1, 1, 0, 2, 0), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * batched without an error hop, the database runs the insert batch at the third row added, i2,
   * which is a duplicate. The transform stops, nothing is passed on or written, but u1, whose batch
   * never ran, is counted as updated.
   */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}")
  @MethodSource("legacyPaths")
  void batchOn_noErrorHop_legacy_Buggy_aFailedBatchCountsAnUpdateNeverWritten(
      String recovery, String contract) throws Exception {
    DatabaseMeta db = mixedTarget();
    if (contract != null) {
      db.setBatchFailureContractOverride(contract);
    }

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(false)
            .commitSize(3)
            .run(mixedRows());

    assertTrue(r.batchMode);
    assertEquals(labels(), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(0, 0, 1, 0, 0, 0), r.counters());
  }

  static Stream<Arguments> commitSizes() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      for (int commitSize : new int[] {1, 3, 100}) {
        arguments.add(Arguments.of(recovery, commitSize));
      }
    }
    return arguments.stream();
  }

  /** Without failures every row is written and passed on. */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("commitSizes")
  void batchOn_noErrorHop_withoutFailuresEveryRowIsWritten(String recovery, int commitSize)
      throws Exception {
    DatabaseMeta db =
        target(SynchronizeAfterMergeRun.TARGET, SynchronizeAfterMergeRun.range(1, 10));

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

  static Stream<Arguments> orderCases() {
    List<Arguments> arguments = new ArrayList<>();
    // Since #9768 the default path runs the statements of one key in input order, see
    // SynchronizeAfterMergeRecoveredBatchDuckDBTest
    for (String recovery : new String[] {"LEGACY"}) {
      for (String table : new String[] {"target", "t"}) {
        for (String name : SynchronizeAfterMergeStatementOrderH2Test.CASES.keySet()) {
          arguments.add(
              Arguments.of(
                  recovery,
                  table,
                  name,
                  SynchronizeAfterMergeStatementOrderH2Test.batched(table, 100, name)));
        }
      }
    }
    return arguments.stream();
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: the batches run in the same
   * order as on H2, see SynchronizeAfterMergeStatementOrderH2Test: it depends on the table name,
   * not on the input.
   */
  @ParameterizedTest(name = "recovery {0}, table {1}: {2}")
  @MethodSource("orderCases")
  void batchOn_noErrorHop_legacy_Buggy_theBatchesRunInAnOrderOfTheirOwn(
      String recovery, String table, String name, List<String> expected) throws Exception {
    DatabaseMeta db = target(table, 2);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .table(table)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .run(SynchronizeAfterMergeStatementOrderH2Test.CASES.get(name));

    if (expected == SynchronizeAfterMergeStatementOrderH2Test.FAILS) {
      assertEquals(1, r.errorCount);
      assertEquals(List.of("2:x"), SynchronizeAfterMergeRun.table(db, table));
    } else {
      assertEquals(0, r.errorCount);
      assertEquals(expected, SynchronizeAfterMergeRun.table(db, table));
    }
  }
}

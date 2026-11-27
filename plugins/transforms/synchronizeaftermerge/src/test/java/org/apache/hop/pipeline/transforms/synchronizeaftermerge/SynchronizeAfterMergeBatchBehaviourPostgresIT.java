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
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_DONE;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_FAILED;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_UNTOUCHED;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_WRITTEN;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.counters;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.manyLabels;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.manyRows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.mixedRows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.ROW;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.labels;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Pins what Synchronize after merge does on PostgreSQL when rows fail, where a failed statement
 * aborts the transaction until it is rolled back. With an error hop every row runs behind a
 * savepoint, also with batch updates on, which PostgreSQL keeps on with an error hop: its {@code
 * IsSupportsErrorHandlingOnBatchUpdates()} is false. Without an error hop the rows are batched.
 *
 * <p>The tables are the ones of SynchronizeAfterMergeBatchBehaviourH2Test, and so are the inputs:
 * the same rows fail, on a duplicate key, the check constraint and the foreign key.
 *
 * <p>Since #9768 PostgreSQL's validated batch failure contract keeps batching on with an error hop,
 * without a savepoint per row, see SynchronizeAfterMergeRecoveredBatchPostgresIT. The tests here
 * whose name says legacy pin the handling as it was, which HOP_DATABASE_BATCH_RECOVERY=LEGACY still
 * gives, and which PostgreSQL's look-alikes still get: they declare no contract.
 */
class SynchronizeAfterMergeBatchBehaviourPostgresIT {

  /** A dialect that extends PostgreSQL, as Greenplum, Redshift and CockroachDB do. */
  @DatabaseMetaPlugin(type = "POSTGRESQL_SAM_LOOKALIKE", typeDescription = "PostgreSQL look-alike")
  public static class PostgreSqlLookAlikeDatabaseMeta extends PostgreSqlDatabaseMeta {}

  static PostgreSQLContainer<?> container;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlLookAlikeDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    container = new PostgreSQLContainer<>("postgres:16-alpine");
    container.start();
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  static DatabaseMeta databaseMeta(String type) {
    return new DatabaseMeta(
        "pg",
        type,
        "Native",
        container.getHost(),
        container.getDatabaseName(),
        Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
        container.getUsername(),
        container.getPassword());
  }

  /** The given table, created anew, holding the given ids named x. */
  static DatabaseMeta target(String type, String table, long... ids) throws Exception {
    DatabaseMeta databaseMeta = databaseMeta(type);
    SynchronizeAfterMergeRun.execute(
        databaseMeta, "DROP TABLE IF EXISTS " + table + "_child", "DROP TABLE IF EXISTS " + table);
    SynchronizeAfterMergeRun.createTarget(databaseMeta, table, ids);
    return databaseMeta;
  }

  /** The table of SynchronizeAfterMergeBatchBehaviourH2Test.mixedRows(). */
  static DatabaseMeta mixedTarget(String type) throws Exception {
    DatabaseMeta db = target(type, SynchronizeAfterMergeRun.TARGET, 1, 2, 3, 4, 5, 6);
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);
    return db;
  }

  static Stream<Arguments> legacyCommitSizes() {
    return Stream.of(Arguments.of("LEGACY", 3), Arguments.of("LEGACY", 100));
  }

  static Stream<Arguments> recoveriesAndCommitSizes() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      for (int commitSize : new int[] {3, 100}) {
        arguments.add(Arguments.of(recovery, commitSize));
      }
    }
    return arguments.stream();
  }

  /** The messages PostgreSQL gives for i2, u4 and d5. */
  static void assertPostgresMessages(Result r) {
    List<Object> descriptions = r.errorField(SynchronizeAfterMergeRun.ERROR_DESCRIPTIONS);
    assertTrue(((String) descriptions.get(0)).contains("duplicate key"), descriptions.toString());
    assertTrue(
        ((String) descriptions.get(1)).contains("violates check constraint"),
        descriptions.toString());
    assertTrue(
        ((String) descriptions.get(2)).contains("violates foreign key constraint"),
        descriptions.toString());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: with an error hop batching stays
   * on and every row runs behind a savepoint, at once. The rows wait in the batch buffer as if they
   * were batched, but a failed row and a skipped row are passed on at once, ahead of the waiting
   * rows, so the streams don't keep the input order. Only the failed rows go to the error hop, with
   * SUYNC002. See SynchronizeAfterMergeRecoveredBatchPostgresIT for what PostgreSQL does by
   * default.
   */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("legacyCommitSizes")
  void batchOn_errorHop_legacy_Buggy_aSavepointPerRowAndRowsOutOfOrder(
      String recovery, int commitSize) throws Exception {
    DatabaseMeta db = mixedTarget("POSTGRESQL");

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
    assertEquals(Collections.nCopies(3, "SUYNC002"), r.errorCodes());
    assertEquals(Collections.nCopies(3, ROW), r.errorKinds());
    assertPostgresMessages(r);
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1, 2, 3, 3, 3, 3), r.errorSizeAtRead);
    assertEquals(labels(), r.passedOnBeforeTheirCommit());
  }

  /**
   * Batch updates off: a savepoint per row, on the default path and with
   * HOP_DATABASE_BATCH_RECOVERY=LEGACY alike. Buggy: every row leaves right after its savepoint
   * section, before its commit, which only comes at the end here.
   */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("recoveriesAndCommitSizes")
  void batchOff_errorHop_Buggy_aSavepointPerRowRowsLeaveBeforeTheirCommit(
      String recovery, int commitSize) throws Exception {
    DatabaseMeta db = mixedTarget("POSTGRESQL");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(false)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(mixedRows());

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(Collections.nCopies(3, "SUYNC001"), r.errorCodes());
    assertPostgresMessages(r);
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 4, 5, 6, 7), r.outputSizeAtRead);
    assertEquals(labels("u1", "i7", "d3", "i9", "u6", "i10"), r.passedOnBeforeTheirCommit());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: without an error hop the rows
   * are batched. The update batch runs first on this table and fails on u4: PostgreSQL loses the
   * whole batch, the transform stops. Only the skipped row was passed on, but all five updates and
   * deletes are counted as updated.
   */
  @Test
  void batchOn_noErrorHop_legacy_Buggy_aFailedBatchCountsRowsThatWereNeverWritten()
      throws Exception {
    DatabaseMeta db = mixedTarget("POSTGRESQL");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .run(mixedRows());

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(labels("s8"), r.outputLabels());
    assertEquals(MIXED_UNTOUCHED, SynchronizeAfterMergeRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(0, 0, 5, 1, 1, 0), r.counters());
  }

  /** Without failures every row is written and passed on. */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 100})
  void batchOn_noErrorHop_withoutFailuresEveryRowIsWritten(int commitSize) throws Exception {
    DatabaseMeta db =
        target(
            "POSTGRESQL", SynchronizeAfterMergeRun.TARGET, SynchronizeAfterMergeRun.range(1, 10));

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(commitSize)
            .run(manyRows());

    assertEquals(manyLabels(), r.outputLabels().stream().sorted().toList());
    assertEquals(MANY_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(0, r.errorCount);
    assertEquals(counters(0, 5, 10, 5, 20, 0), r.counters());
  }

  /** A dialect that extends PostgreSQL behaves the same: a savepoint per row, rows out of order. */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {3, 100})
  void lookAlike_batchOn_errorHop_Buggy_aSavepointPerRow(int commitSize) throws Exception {
    DatabaseMeta db = mixedTarget("POSTGRESQL_SAM_LOOKALIKE");

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .run(mixedRows());

    assertTrue(r.batchMode);
    assertTrue(r.useSavepoints);
    assertEquals(labels("s8", "u1", "i7", "d3", "i9", "u6", "i10"), r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
  }

  static Stream<Arguments> orderCases() {
    List<Arguments> arguments = new ArrayList<>();
    for (String table : new String[] {"target", "t"}) {
      for (String name : SynchronizeAfterMergeStatementOrderH2Test.CASES.keySet()) {
        arguments.add(
            Arguments.of(
                table, name, SynchronizeAfterMergeStatementOrderH2Test.batched(table, 100, name)));
      }
    }
    return arguments.stream();
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: the batches run in the same
   * order as on H2, see SynchronizeAfterMergeStatementOrderH2Test: it depends on the table name,
   * not on the input. See SynchronizeAfterMergeRecoveredBatchPostgresIT for the default path.
   */
  @ParameterizedTest(name = "table {0}: {1}")
  @MethodSource("orderCases")
  void batchOn_noErrorHop_legacy_Buggy_theBatchesRunInAnOrderOfTheirOwn(
      String table, String name, List<String> expected) throws Exception {
    DatabaseMeta db = target("POSTGRESQL", table, 2);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
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

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

import static org.apache.hop.pipeline.transforms.update.UpdateRun.NOT_FOUND;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.ROW;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.ids;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.rows;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableUpdatedExcept;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.update.UpdateRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Pins what Update does on PostgreSQL when rows fail, where a failed statement aborts the
 * transaction until it is rolled back. With an error hop every row runs behind a savepoint, so "use
 * batch update" is switched off; without one the rows are batched.
 *
 * <p>Since #9722 PostgreSQL's validated batch failure contract keeps batching on with an error hop,
 * see UpdateRecoveredBatchPostgresIT. The tests here whose name says legacy pin the handling as it
 * was, which HOP_DATABASE_BATCH_RECOVERY=LEGACY still gives, and which PostgreSQL's look-alikes
 * still get: they declare no contract.
 */
class UpdateBatchBehaviourPostgresIT {

  /** A dialect that extends PostgreSQL, as Greenplum, Redshift and CockroachDB do. */
  @DatabaseMetaPlugin(
      type = "POSTGRESQL_UPDATE_LOOKALIKE",
      typeDescription = "PostgreSQL look-alike")
  public static class PostgreSqlLookAlikeDatabaseMeta extends PostgreSqlDatabaseMeta {}

  private static PostgreSQLContainer<?> container;

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

  /** The target table, holding ids 1..n named x, on a connection of the given type. */
  static DatabaseMeta target(String type, int n) throws Exception {
    DatabaseMeta databaseMeta = databaseMeta(type);
    UpdateRun.execute(databaseMeta, "DROP TABLE IF EXISTS target");
    UpdateRun.createTarget(databaseMeta, n);
    return databaseMeta;
  }

  static DatabaseMeta target(int n) throws Exception {
    return target("POSTGRESQL", n);
  }

  private static List<Long> counters(
      long input, long updated, long skipped, long written, long rejected) {
    return UpdateBatchBehaviourH2Test.counters(input, updated, skipped, written, rejected);
  }

  /** Which rows are passed on before their commit, by commit size, in the savepoint path. */
  private static List<Object> passedOnBeforeTheirCommit(int commitSize) {
    return switch (commitSize) {
      case 1 -> ids();
      case 3 -> ids(1, 2, 5, 6, 9, 10);
      default -> idsWithout(10, 3, 7);
    };
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: with an error hop, "use batch
   * update" is switched off and logged. Every row runs and leaves on its own, behind a savepoint;
   * only the failed rows go to the error hop. The transform commits every commit size rows, after
   * it passed them on. See UpdateRecoveredBatchPostgresIT for what PostgreSQL does by default.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 100})
  void batchOn_errorHop_legacy_Buggy_batchingIsSwitchedOffForASavepointPerRow(int commitSize)
      throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001"), r.errorCodes());
    assertEquals(List.of(ROW, ROW), r.errorKinds());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(counters(10, 8, 0, 8, 2), r.counters());
    assertEquals(passedOnBeforeTheirCommit(commitSize), r.passedOnBeforeTheirCommit());
  }

  /**
   * Batch updates off: the same savepoint per row, without the log line. A row leaves only after
   * the commit that includes its update: a commit every three rows that wait, after rows 3 and 6,
   * then row 7 fails with nothing in front of it, then a commit after row 10.
   */
  @Test
  void batchOff_errorHop_aSavepointPerRow() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(false)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertFalse(r.logged("batch updates are switched off"));
    // The savepoint undoes the failed row alone: nothing has to run again
    assertFalse(r.runAgainAfterFailure);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(counters(10, 8, 0, 8, 2), r.counters());
    assertEquals(List.of(0, 0, 0, 2, 2, 2, 5, 5, 5, 5, 8), r.outputSizeAtRead);
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
  }

  /**
   * Commit size 0 is auto-commit: no batches and no savepoints, every row is committed on its own,
   * and a failed row aborts nothing.
   */
  @Test
  void batchOn_errorHop_commitSizeZero_autoCommitsEveryRow() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(0)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertFalse(r.useSavepoints);
    assertFalse(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of(ROW, ROW), r.errorKinds());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(counters(10, 8, 0, 8, 2), r.counters());
  }

  /** A dialect that extends PostgreSQL behaves the same. */
  @Test
  void lookAlike_batchOn_errorHop_aSavepointPerRow() throws Exception {
    DatabaseMeta db = target("POSTGRESQL_UPDATE_LOOKALIKE", 10);

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
  }

  /**
   * A missing key and a row that needs no update, in their place between the updated rows, behind a
   * savepoint per row (HOP_DATABASE_BATCH_RECOVERY=LEGACY).
   */
  @Test
  void batchOn_errorHop_legacy_rowsThatAreNotUpdatedKeepTheirPlace() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .run(UpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(ids(1, 2, 3, 4, 6), r.outputKeys());
    assertEquals(ids(11, 12, 5), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001", "UPD001"), r.errorCodes());
    assertEquals(List.of(NOT_FOUND, NOT_FOUND, ROW), r.errorKinds());
    assertEquals(List.of("[11]", "[12]"), r.keysNotFound());
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(counters(8, 3, 2, 5, 3), r.counters());
  }

  /** Without an error hop the rows are batched; a failed batch is rolled back and stops. */
  @Test
  void batchOn_noErrorHop_aFailedBatchIsRolledBackAndStopsTheTransform() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db).batch(true).errorHop(false).commitSize(4).watchCommits().run(rows(10, 7));

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableWith(10, id -> id <= 4), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(8, 4, 0, 4, 0), r.counters());
  }

  /** id (Integer), pattern (String), name (String). */
  static IRowMeta patternRowMeta() {
    IRowMeta rowMeta = UpdateRun.idNameRowMeta();
    rowMeta.addValueMeta(1, new ValueMetaString("pattern"));
    return rowMeta;
  }

  /**
   * Rows 1..n with a LIKE pattern on the current name, which PostgreSQL refuses for the bad ids: a
   * pattern must not end with the escape character. So the lookup of those rows fails.
   */
  static List<Object[]> patternRows(int n, long... bad) {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      rows.add(new Object[] {id, UpdateRun.contains(bad, id) ? "\\" : "x%", "r" + id});
    }
    return rows;
  }

  /**
   * A lookup that fails aborts the transaction; behind a savepoint (HOP_DATABASE_BATCH_RECOVERY=
   * LEGACY) only that row goes to the error hop and the rows after it are updated.
   */
  @Test
  void batchOn_errorHop_legacy_aFailedLookupOnlyRejectsItsOwnRow() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .configure(
                meta ->
                    meta.getLookupField()
                        .getLookupKeys()
                        .add(new UpdateKeyField("pattern", "name", "LIKE")))
            .run(patternRowMeta(), patternRows(10, 3, 7));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001"), r.errorCodes());
    assertTrue(
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("LIKE pattern must not end with escape")),
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(counters(8, 8, 0, 8, 2), r.counters());
  }
}

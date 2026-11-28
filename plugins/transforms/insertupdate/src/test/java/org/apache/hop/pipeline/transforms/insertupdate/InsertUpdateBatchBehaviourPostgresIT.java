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

package org.apache.hop.pipeline.transforms.insertupdate;

import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.LOOKUP;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ROW;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ids;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.rows;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWith;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWrittenExcept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Pins what Insert/Update does on PostgreSQL today, where a failed statement aborts the transaction
 * until it is rolled back. With an error hop every row runs behind a savepoint, so only a failed
 * row goes to the error hop; without one the rows run as they are. Every test runs on the default
 * path and with HOP_DATABASE_BATCH_RECOVERY=LEGACY, and both must give the same. The table holds
 * the odd ids, so the odd rows are updated and the even ones inserted.
 */
class InsertUpdateBatchBehaviourPostgresIT {

  /** A dialect that extends PostgreSQL, as Greenplum, Redshift and CockroachDB do. */
  @DatabaseMetaPlugin(
      type = "POSTGRESQL_INSUPD_LOOKALIKE",
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

  static DatabaseMeta databaseMeta(PostgreSQLContainer<?> container, String type) {
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

  /** The target table, holding the odd ids up to n, on a connection of the given type. */
  static DatabaseMeta target(PostgreSQLContainer<?> container, String type, int n)
      throws Exception {
    DatabaseMeta databaseMeta = databaseMeta(container, type);
    InsertUpdateRun.execute(databaseMeta, "DROP TABLE IF EXISTS target");
    InsertUpdateRun.createTarget(databaseMeta, n);
    return databaseMeta;
  }

  static DatabaseMeta target(int n) throws Exception {
    return target(container, "POSTGRESQL", n);
  }

  private static List<Long> counters(
      long input, long inserted, long updated, long skipped, long written, long rejected) {
    return InsertUpdateBatchBehaviourH2Test.counters(
        input, inserted, updated, skipped, written, rejected);
  }

  static Stream<Arguments> savepointCommits() {
    return InsertUpdateBatchBehaviourH2Test.savepointCommits();
  }

  /**
   * Buggy: with an error hop every row runs and leaves on its own, behind a savepoint; only the
   * failed rows go to the error hop. The transform commits every commit size rows, after it passed
   * them on.
   */
  @ParameterizedTest(name = "recovery {0}, commit size {1}")
  @MethodSource("savepointCommits")
  void errorHop_Buggy_aSavepointPerRowAndRowsLeaveBeforeTheirCommit(
      String recovery, int commitSize, List<Object> passedOnBeforeTheirCommit) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, 3, 6));

    assertTrue(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(List.of("ISU001", "ISU001"), r.errorCodes());
    assertEquals(List.of(ROW, ROW), r.errorKinds());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertEquals(counters(10, 4, 4, 0, 8, 2), r.counters());
    assertEquals(passedOnBeforeTheirCommit, r.passedOnBeforeTheirCommit());
  }

  /**
   * Rows that need no change and failed rows keep their place between the written rows; the rows
   * that need no change count towards the commit size behind a savepoint. Buggy, as above.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource(
      "org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateBatchBehaviourH2Test#recoveries")
  void errorHop_Buggy_rowsThatNeedNoChangeKeepTheirPlace(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(InsertUpdateBatchBehaviourH2Test.mixedRows());

    assertTrue(r.useSavepoints);
    assertEquals(ids(1, 2, 3, 5, 6, 9), r.outputKeys());
    assertEquals(ids(4, 7), r.errorKeys());
    assertEquals(List.of("ISU001", "ISU001"), r.errorCodes());
    assertEquals(InsertUpdateBatchBehaviourH2Test.mixedTable(), InsertUpdateRun.table(db));
    assertEquals(counters(8, 2, 2, 2, 6, 2), r.counters());
    assertEquals(List.of(0, 1, 2, 3, 3, 4, 5, 5, 6), r.outputSizeAtRead);
    assertEquals(ids(1, 2, 5, 6), r.passedOnBeforeTheirCommit());
  }

  /**
   * "Don't perform any updates": the found rows are skipped, behind a savepoint like the others.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource(
      "org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateBatchBehaviourH2Test#recoveries")
  void errorHop_updatesBypassed(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(3)
            .configure(meta -> meta.setUpdateBypassed(true))
            .run(rows(10, 6));

    assertTrue(r.useSavepoints);
    assertEquals(idsWithout(10, 6), r.outputKeys());
    assertEquals(ids(6), r.errorKeys());
    assertEquals(tableWith(10, id -> id % 2 == 0 && id != 6), InsertUpdateRun.table(db));
    assertEquals(counters(10, 4, 0, 5, 9, 1), r.counters());
  }

  /**
   * Commit size 0 is auto-commit: no savepoints, every row is committed on its own, and a failed
   * row aborts nothing.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource(
      "org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateBatchBehaviourH2Test#recoveries")
  void errorHop_commitSizeZero_autoCommitsEveryRow(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(0)
            .watchCommits()
            .run(rows(10, 3, 6));

    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(List.of(ROW, ROW), r.errorKinds());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertEquals(counters(10, 4, 4, 0, 8, 2), r.counters());
  }

  /** A dialect that extends PostgreSQL behaves the same. */
  @Test
  void lookAlike_errorHop_aSavepointPerRow() throws Exception {
    DatabaseMeta db = target(container, "POSTGRESQL_INSUPD_LOOKALIKE", 10);

    Result r = InsertUpdateRun.on(db).errorHop(true).commitSize(4).run(rows(10, 3, 6));

    assertTrue(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
  }

  /**
   * Buggy: without an error hop a failed row stops the transform and the transaction is rolled
   * back; rows 5 and 6, already passed on, are not in the table.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource(
      "org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateBatchBehaviourH2Test#recoveries")
  void noErrorHop_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(String recovery)
      throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(false)
            .commitSize(4)
            .watchCommits()
            .run(rows(10, 7));

    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(6), r.outputKeys());
    assertEquals(ids(1, 2, 3, 5, 6), r.passedOnBeforeTheirCommit());
    assertEquals(tableWith(10, id -> id <= 4), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(7, 3, 3, 0, 6, 0), r.counters());
  }

  /** id (Integer), pattern (String), name (String). */
  static IRowMeta patternRowMeta() {
    IRowMeta rowMeta = InsertUpdateRun.idNameRowMeta();
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
      rows.add(new Object[] {id, InsertUpdateRun.contains(bad, id) ? "\\" : "x%", "r" + id});
    }
    return rows;
  }

  /** Looks the rows up by id, and by a LIKE pattern on the name as well. */
  static void lookUpByAPatternAsWell(InsertUpdateMeta meta) {
    meta.getInsertUpdateLookupField()
        .getLookupKeys()
        .add(new InsertUpdateKeyField("pattern", "name", "LIKE"));
  }

  /**
   * A lookup that fails aborts the transaction; behind a savepoint only that row goes to the error
   * hop and the rows after it are written. PostgreSQL only checks the pattern against a row the
   * lookup finds by its id, so the bad patterns are on ids in the table, 3 and 7.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource(
      "org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateBatchBehaviourH2Test#recoveries")
  void errorHop_aFailedLookupOnlyRejectsItsOwnRow(String recovery) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery(recovery)
            .errorHop(true)
            .commitSize(4)
            .configure(InsertUpdateBatchBehaviourPostgresIT::lookUpByAPatternAsWell)
            .run(patternRowMeta(), patternRows(10, 3, 7));

    assertTrue(r.useSavepoints);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("ISU001", "ISU001"), r.errorCodes());
    assertEquals(List.of(LOOKUP, LOOKUP), r.errorKinds());
    assertTrue(
        r.errorField(InsertUpdateRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("LIKE pattern must not end with escape")),
        r.errorField(InsertUpdateRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(tableWrittenExcept(10, 3, 7), InsertUpdateRun.table(db));
    assertEquals(counters(8, 5, 3, 0, 8, 2), r.counters());
  }
}

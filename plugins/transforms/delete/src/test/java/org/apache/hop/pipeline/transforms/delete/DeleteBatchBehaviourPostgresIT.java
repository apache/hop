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

import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ROW;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.counters;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ids;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.idsWithout;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.remaining;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.rows;
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
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test.Legacy;
import org.apache.hop.pipeline.transforms.delete.DeleteRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Pins what Delete does on PostgreSQL when rows fail, where a failed statement aborts the
 * transaction until it is rolled back. With an error hop every row runs behind a savepoint, so "use
 * batch deletes" is switched off; without one the rows are batched. A row fails when a child row
 * still references its id.
 *
 * <p>Since #9768 PostgreSQL's validated batch failure contract keeps batching on with an error hop,
 * see DeleteRecoveredBatchPostgresIT. The tests here whose name says legacy pin the handling as it
 * was, which HOP_DATABASE_BATCH_RECOVERY=LEGACY and the contract "As before" (UNKNOWN) on the
 * connection still give, and which PostgreSQL's look-alikes still get: they declare no contract.
 * The other tests run on the default path as well.
 */
class DeleteBatchBehaviourPostgresIT {

  /** A dialect that extends PostgreSQL, as Greenplum, Redshift and CockroachDB do. */
  @DatabaseMetaPlugin(
      type = "POSTGRESQL_DELETE_LOOKALIKE",
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

  /** The target table holding ids 1..n, the given ids referenced, on a connection of a type. */
  static DatabaseMeta target(String type, int n, long... referenced) throws Exception {
    DatabaseMeta databaseMeta = databaseMeta(type);
    DeleteRun.execute(
        databaseMeta, "DROP TABLE IF EXISTS target_child", "DROP TABLE IF EXISTS target");
    DeleteRun.createTarget(databaseMeta, n, referenced);
    return databaseMeta;
  }

  static DatabaseMeta target(int n, long... referenced) throws Exception {
    return target("POSTGRESQL", n, referenced);
  }

  /** Which rows are passed on before their commit, by commit size, in the savepoint path. */
  private static List<Object> passedOnBeforeTheirCommit(int commitSize) {
    return switch (commitSize) {
      case 1 -> ids();
      case 3 -> ids(1, 2, 5, 6, 9, 10);
      default -> idsWithout(10, 3, 7);
    };
  }

  /** The two paths that handle a failed batch as main does, each with commit sizes 1, 3, 100. */
  static Stream<Arguments> savepointCases() {
    List<Arguments> arguments = new ArrayList<>();
    DeleteBatchBehaviourH2Test.legacyPaths()
        .forEach(
            path -> {
              for (int commitSize : new int[] {1, 3, 100}) {
                arguments.add(Arguments.of(path, commitSize));
              }
            });
    return arguments.stream();
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * with an error hop, "use batch deletes" is switched off and logged. Every row runs and leaves on
   * its own, behind a savepoint; only the failed rows go to the error hop. The transform commits
   * every commit size rows, after it passed them on. See DeleteRecoveredBatchPostgresIT for what
   * PostgreSQL does by default.
   */
  @ParameterizedTest(name = "{0}, commit size {1}")
  @MethodSource("savepointCases")
  void batchOn_errorHop_legacy_Buggy_batchingIsSwitchedOffForASavepointPerRow(
      Legacy path, int commitSize) throws Exception {
    DatabaseMeta db = target(10, 3, 7);

    Result r =
        DeleteBatchBehaviourH2Test.on(path, db)
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
    assertEquals(List.of("DEL001", "DEL001"), r.errorCodes());
    assertEquals(List.of(ROW, ROW), r.errorKinds());
    assertTrue(
        r.errorField(DeleteRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("violates foreign key constraint")),
        r.errorField(DeleteRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(ids(3, 7), remaining(db));
    assertEquals(counters(10, 8, 8, 2), r.counters());
    assertEquals(passedOnBeforeTheirCommit(commitSize), r.passedOnBeforeTheirCommit());
  }

  /**
   * Batch deletes off: the same savepoint per row, without the log line. Buggy: every row leaves
   * right after its savepoint section, before the commit that includes its delete.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#paths")
  void batchOff_errorHop_Buggy_aSavepointPerRow(Legacy path) throws Exception {
    DatabaseMeta db = target(10, 3, 7);

    Result r =
        DeleteBatchBehaviourH2Test.on(path, db)
            .batch(false)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(rows(10));

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertFalse(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(ids(3, 7), remaining(db));
    assertEquals(counters(10, 8, 8, 2), r.counters());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(passedOnBeforeTheirCommit(3), r.passedOnBeforeTheirCommit());
  }

  /**
   * Commit size 0 is auto-commit: no batches and no savepoints, every row is committed on its own,
   * and a failed row aborts nothing.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#paths")
  void batchOn_errorHop_commitSizeZero_autoCommitsEveryRow(Legacy path) throws Exception {
    DatabaseMeta db = target(10, 3, 7);

    Result r =
        DeleteBatchBehaviourH2Test.on(path, db)
            .batch(true)
            .errorHop(true)
            .commitSize(0)
            .watchCommits()
            .run(rows(10));

    assertFalse(r.useSavepoints);
    assertFalse(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of(ROW, ROW), r.errorKinds());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(ids(3, 7), remaining(db));
    assertEquals(counters(10, 8, 8, 2), r.counters());
  }

  /** A dialect that extends PostgreSQL behaves the same, on every path. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#paths")
  void lookAlike_batchOn_errorHop_aSavepointPerRow(Legacy path) throws Exception {
    DatabaseMeta db = target("POSTGRESQL_DELETE_LOOKALIKE", 10, 3, 7);

    Result r =
        DeleteBatchBehaviourH2Test.on(path, db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(rows(10));

    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(ids(3, 7), remaining(db));
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * without an error hop the rows are batched and passed on as they are added. A failed batch is
   * rolled back and stops the transform: rows 5-7 were passed on and are still in the table.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#legacyPaths")
  void batchOn_noErrorHop_legacy_Buggy_theRowsOfTheFailedBatchWerePassedOn(Legacy path)
      throws Exception {
    DatabaseMeta db = target(10, 7);

    Result r =
        DeleteBatchBehaviourH2Test.on(path, db)
            .batch(true)
            .errorHop(false)
            .commitSize(4)
            .run(rows(10));

    assertTrue(r.batchMode);
    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(ids(5, 6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(8, 7, 7, 0), r.counters());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY or "As before" on the connection:
   * the partial batch at the end of the input fails when the transform is disposed. Its rows were
   * passed on; PostgreSQL lost the whole batch, so neither row is deleted.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test#legacyPaths")
  void batchOn_noErrorHop_legacy_Buggy_theLastPartialBatchFails(Legacy path) throws Exception {
    DatabaseMeta db = target(10, 9);

    Result r =
        DeleteBatchBehaviourH2Test.on(path, db)
            .batch(true)
            .errorHop(false)
            .commitSize(4)
            .run(rows(10));

    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(ids(9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(10, 10, 10, 0), r.counters());
  }
}

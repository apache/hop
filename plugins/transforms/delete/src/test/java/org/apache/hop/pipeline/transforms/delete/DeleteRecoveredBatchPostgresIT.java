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
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourPostgresIT.PostgreSqlLookAlikeDatabaseMeta;
import org.apache.hop.pipeline.transforms.delete.DeleteRun.Result;
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
 * Delete on PostgreSQL, whose contract LOSES_WHOLE_BATCH is validated: a failed batch keeps none of
 * its rows and aborts the transaction. With an error hop the rows are still batched, without a
 * savepoint per row, and a failed batch is recovered: every row reaches exactly one stream, in
 * input order, after its commit. A row fails when a child row still references its id.
 */
class DeleteRecoveredBatchPostgresIT {

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

  /** The target table holding ids 1..n, the given ids referenced, on a connection of a type. */
  static DatabaseMeta target(
      PostgreSQLContainer<?> container, String type, String contract, int n, long... referenced)
      throws Exception {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            "pg",
            type,
            "Native",
            container.getHost(),
            container.getDatabaseName(),
            Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
            container.getUsername(),
            container.getPassword());
    if (contract != null) {
      databaseMeta.setBatchFailureContractOverride(contract);
    }
    DeleteRun.execute(
        databaseMeta, "DROP TABLE IF EXISTS target_child", "DROP TABLE IF EXISTS target");
    DeleteRun.createTarget(databaseMeta, n, referenced);
    return databaseMeta;
  }

  private static DatabaseMeta target(int n, long... referenced) throws Exception {
    return target(container, "POSTGRESQL", null, n, referenced);
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {
      {}, {1}, {3}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}
    };
    for (int commitSize : new int[] {1, 3, 5, 100}) {
      for (long[] bad : badSets) {
        arguments.add(Arguments.of(commitSize, bad));
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "commit size {0}, bad rows {1}")
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(int commitSize, long[] bad) throws Exception {
    DatabaseMeta db = target(10, bad);

    Result r =
        DeleteRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(Collections.nCopies(bad.length, "DEL002"), r.errorCodes());
    assertTrue(
        r.errorField(DeleteRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("violates foreign key constraint")),
        r.errorField(DeleteRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(ids(bad), remaining(db));
    assertEquals(counters(10, 10 - bad.length, 10 - bad.length, bad.length), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** The rows of a batch leave together, once it has run and is committed. */
  @Test
  void theRowsOfABatchLeaveTogether() throws Exception {
    DatabaseMeta db = target(10, 3, 7);

    Result r = DeleteRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10));

    assertEquals(List.of(0, 0, 0, 0, 3, 3, 3, 3, 6, 6, 6), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2), r.errorSizeAtRead);
    assertRecovered(r);
  }

  /** A key Hop can't bind goes to the error hop in its place, between the rows of the batch. */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 100})
  void aKeyThatCannotBeBoundKeepsItsPlace(int commitSize) throws Exception {
    DatabaseMeta db = target(10, 2, 7);

    Result r =
        DeleteRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(DeleteRun.rowsWithUnboundKeys(10, 4));

    assertEquals(idsWithout(10, 2, 4, 7), r.outputKeys());
    assertEquals(List.of(2L, "no number 4", 7L), r.errorKeys());
    assertEquals(List.of("DEL002", "DEL001", "DEL002"), r.errorCodes());
    assertEquals(ids(2, 4, 7), remaining(db));
    assertEquals(counters(10, 7, 7, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * Without an error hop the failed batch is rolled back and the transform stops; none of its rows
   * is passed on.
   */
  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = target(10, 7);

    Result r =
        DeleteRun.on(db).batch(true).errorHop(false).commitSize(4).watchCommits().run(rows(10));

    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(ids(5, 6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(8, 4, 4, 0), r.counters());
    assertTrue(r.recoveredBatches);
  }

  /** The partial batch at the end of the input fails: only the full batches before it stay. */
  @Test
  void withoutAnErrorHopTheLastPartialBatchFails() throws Exception {
    DatabaseMeta db = target(10, 9);

    Result r = DeleteRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10));

    assertEquals(idsWithout(8), r.outputKeys());
    assertEquals(ids(9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(10, 8, 8, 0), r.counters());
  }

  /** At the end of a single-threaded iteration a failed batch is recovered like any other. */
  @Test
  void batchCompleteRecoversTheBatch() throws Exception {
    DatabaseMeta db = target(5, 2, 4);
    List<List<Object>> remainingAtBatchComplete = new ArrayList<>();

    Result r =
        DeleteRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .watchCommits()
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    remainingAtBatchComplete.add(remaining(db));
                  }
                })
            .run(rows(5));

    assertEquals(List.of(ids(2, 4, 5)), remainingAtBatchComplete);
    assertEquals(ids(1, 3, 5), r.outputKeys());
    assertEquals(ids(2, 4), r.errorKeys());
    assertEquals(ids(2, 4), remaining(db));
    assertEquals(counters(5, 3, 3, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
  }

  /** Larger batches with a few bad rows, as in a real load. */
  @Test
  void largerBatches() throws Exception {
    long[] bad = {1, 97, 194, 500, 501, 1_000, 1_999, 2_000};
    DatabaseMeta db = target(2_000, bad);

    Result r = DeleteRun.on(db).batch(true).errorHop(true).commitSize(500).run(rows(2_000));

    assertEquals(idsWithout(2_000, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(ids(bad), remaining(db));
    assertEquals(counters(2_000, 1_992, 1_992, 8), r.counters());
    assertRecovered(r);
  }

  /** HOP_DATABASE_BATCH_RECOVERY=LEGACY goes back to a savepoint per row, batching off. */
  @Test
  void theLegacyVariableGoesBackToASavepointPerRow() throws Exception {
    DatabaseMeta db = target(10, 3, 7);

    Result r =
        DeleteRun.on(db).recovery("LEGACY").batch(true).errorHop(true).commitSize(4).run(rows(10));

    assertFalse(r.recoveredBatches);
    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("DEL001", "DEL001"), r.errorCodes());
  }

  /** "As before" on the connection does the same, for that connection. */
  @Test
  void unknownOnTheConnectionGoesBackToASavepointPerRow() throws Exception {
    DatabaseMeta db = target(container, "POSTGRESQL", "UNKNOWN", 10, 3, 7);

    Result r = DeleteRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10));

    assertFalse(r.recoveredBatches);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged("batch updates are switched off"));
  }

  /**
   * A dialect that extends PostgreSQL declares no contract and keeps a savepoint per row, unless
   * the contract is set on the connection.
   */
  @Test
  void aLookAlikeRecoversBatchesWithTheContractOnTheConnection() throws Exception {
    DatabaseMeta plain = target(container, "POSTGRESQL_DELETE_LOOKALIKE", null, 10, 3);
    Result before = DeleteRun.on(plain).batch(true).errorHop(true).commitSize(4).run(rows(10));
    assertFalse(before.recoveredBatches);
    assertTrue(before.useSavepoints);

    DatabaseMeta db =
        target(container, "POSTGRESQL_DELETE_LOOKALIKE", "LOSES_WHOLE_BATCH", 10, 3, 7);
    Result r =
        DeleteRun.on(db).batch(true).errorHop(true).commitSize(4).watchCommits().run(rows(10));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(ids(3, 7), remaining(db));
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }
}

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

import static org.apache.hop.pipeline.transforms.update.UpdateRecoveredBatchH2Test.assertPassedOnAfterTheCommit;
import static org.apache.hop.pipeline.transforms.update.UpdateRecoveredBatchH2Test.assertRecovered;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.update.UpdateBatchBehaviourPostgresIT.PostgreSqlLookAlikeDatabaseMeta;
import org.apache.hop.pipeline.transforms.update.UpdateRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Update on PostgreSQL, whose contract LOSES_WHOLE_BATCH is validated: a failed batch keeps none of
 * its rows and aborts the transaction. With an error hop the rows are still batched, without a
 * savepoint per row, and a failed batch is recovered: every row reaches exactly one stream, in
 * input order, after its commit.
 */
class UpdateRecoveredBatchPostgresIT {

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

  private static DatabaseMeta target(String type, String contract, int n) throws Exception {
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
    UpdateRun.execute(databaseMeta, "DROP TABLE IF EXISTS target");
    UpdateRun.createTarget(databaseMeta, n);
    return databaseMeta;
  }

  private static DatabaseMeta target(int n) throws Exception {
    return target("POSTGRESQL", null, n);
  }

  private static List<Long> counters(
      long input, long updated, long skipped, long written, long rejected) {
    return UpdateBatchBehaviourH2Test.counters(input, updated, skipped, written, rejected);
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
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, bad));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(Collections.nCopies(bad.length, "UPD002"), r.errorCodes());
    assertTrue(
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("violates check constraint")),
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(tableUpdatedExcept(10, bad), UpdateRun.table(db));
    assertEquals(counters(10, 10 - bad.length, 0, 10 - bad.length, bad.length), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** The rows of a batch leave together, once it has run and is committed. */
  @Test
  void theRowsOfABatchLeaveTogether() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 3, 7));

    assertEquals(List.of(0, 0, 0, 0, 3, 3, 3, 3, 6, 6, 6), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2), r.errorSizeAtRead);
    assertRecovered(r);
  }

  @Test
  void rowsOutsideTheBatchKeepTheirPlace() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(UpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(ids(1, 2, 3, 4, 6), r.outputKeys());
    assertEquals(ids(11, 12, 5), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001", "UPD002"), r.errorCodes());
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(counters(8, 3, 2, 5, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * A lookup that fails aborts the transaction. It holds no updates between two batches, so it is
   * rolled back, and only that row goes to the error hop: the rows after it, and the batch they are
   * in, run as usual.
   */
  @Test
  void aFailedLookupOnlyRejectsItsOwnRow() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .configure(
                meta ->
                    meta.getLookupField()
                        .getLookupKeys()
                        .add(new UpdateKeyField("pattern", "name", "LIKE")))
            .run(
                UpdateBatchBehaviourPostgresIT.patternRowMeta(),
                UpdateBatchBehaviourPostgresIT.patternRows(10, 3, 7));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001"), r.errorCodes());
    assertTrue(
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("LIKE pattern must not end with escape")),
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(counters(8, 8, 0, 8, 2), r.counters());
    assertRecovered(r);
  }

  /** Without an error hop: unchanged, the failed batch is rolled back and the transform stops. */
  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db).batch(true).errorHop(false).commitSize(4).watchCommits().run(rows(10, 7));

    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableWith(10, id -> id <= 4), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(8, 4, 0, 4, 0), r.counters());
    assertTrue(r.recoveredBatches);
  }

  /** Larger batches with a few bad rows, as in a real load. */
  @Test
  void largerBatches() throws Exception {
    DatabaseMeta db = target(2_000);
    long[] bad = {1, 97, 194, 500, 501, 1_000, 1_999, 2_000};

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(500).run(rows(2_000, bad));

    assertEquals(idsWithout(2_000, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(tableUpdatedExcept(2_000, bad), UpdateRun.table(db));
    assertEquals(counters(2_000, 1_992, 0, 1_992, 8), r.counters());
    assertRecovered(r);
  }

  /** HOP_DATABASE_BATCH_RECOVERY=LEGACY goes back to a savepoint per row, batching off. */
  @Test
  void theLegacyVariableGoesBackToASavepointPerRow() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(rows(10, 3, 7));

    assertFalse(r.recoveredBatches);
    assertFalse(r.batchMode);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged("batch updates are switched off"));
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
  }

  /** "As before" on the connection does the same, for that connection. */
  @Test
  void unknownOnTheConnectionGoesBackToASavepointPerRow() throws Exception {
    DatabaseMeta db = target("POSTGRESQL", "UNKNOWN", 10);

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 3, 7));

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
    DatabaseMeta plain = target("POSTGRESQL_UPDATE_LOOKALIKE", null, 10);
    Result before = UpdateRun.on(plain).batch(true).errorHop(true).commitSize(4).run(rows(10, 3));
    assertFalse(before.recoveredBatches);
    assertTrue(before.useSavepoints);

    DatabaseMeta db = target("POSTGRESQL_UPDATE_LOOKALIKE", "LOSES_WHOLE_BATCH", 10);
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
}

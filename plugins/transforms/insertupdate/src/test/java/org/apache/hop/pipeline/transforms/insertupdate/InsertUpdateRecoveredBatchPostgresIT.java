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

import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.ONE_BY_ONE;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.assertPassedOnAfterTheCommit;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.assertRecovered;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.counters;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.countersFor;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.LOOKUP;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ids;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.row;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.rows;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWith;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWrittenExcept;
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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateBatchBehaviourPostgresIT.PostgreSqlLookAlikeDatabaseMeta;
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
 * Insert/Update with "Use batch updates" on PostgreSQL, whose contract LOSES_WHOLE_BATCH is
 * validated: a failed batch keeps none of its rows and aborts the transaction. With an error hop
 * the inserts and updates are still batched, without a savepoint per row, and a failed batch is
 * recovered: every row reaches exactly one stream, in input order, after its commit.
 */
class InsertUpdateRecoveredBatchPostgresIT {

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
    DatabaseMeta databaseMeta = InsertUpdateBatchBehaviourPostgresIT.target(container, type, n);
    if (contract != null) {
      databaseMeta.setBatchFailureContractOverride(contract);
    }
    return databaseMeta;
  }

  private static DatabaseMeta target(int n) throws Exception {
    return target("POSTGRESQL", null, n);
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {
      {}, {1}, {2}, {3}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}
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
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, bad));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(Collections.nCopies(bad.length, "ISU002"), r.errorCodes());
    assertTrue(
        r.errorField(InsertUpdateRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("violates check constraint")),
        r.errorField(InsertUpdateRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(tableWrittenExcept(10, bad), InsertUpdateRun.table(db));
    assertEquals(countersFor(10, bad), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** The rows of a window leave together, once both its batches have run and are committed. */
  @Test
  void theRowsOfAWindowLeaveTogether() throws Exception {
    DatabaseMeta db = target(10);

    Result r = InsertUpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 3, 6));

    assertEquals(List.of(0, 0, 0, 0, 3, 3, 3, 3, 6, 6, 6), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2), r.errorSizeAtRead);
    assertRecovered(r);
  }

  @Test
  void rowsThatNeedNoChangeKeepTheirPlace() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(InsertUpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(ids(1, 2, 3, 5, 6, 9), r.outputKeys());
    assertEquals(ids(4, 7), r.errorKeys());
    assertEquals(List.of("ISU002", "ISU002"), r.errorCodes());
    assertEquals(InsertUpdateBatchBehaviourH2Test.mixedTable(), InsertUpdateRun.table(db));
    assertEquals(counters(8, 2, 2, 2, 6, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** A key that waits in a batch is written before it is looked up again. */
  @Test
  void aKeyThatComesTwiceIsInsertedOnceThenUpdated() throws Exception {
    DatabaseMeta db = target(1);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .run(
                List.of(
                    row(2, "first"),
                    row(2, "second"),
                    row(1, "r1"),
                    row(2, "second"),
                    row(1, "r1")));

    assertEquals(ids(2, 2, 1, 2, 1), r.outputKeys());
    assertEquals(List.of("1:r1", "2:second"), InsertUpdateRun.table(db));
    assertEquals(counters(5, 1, 2, 2, 5, 0), r.counters());
    assertRecovered(r);
  }

  /** id (Integer), name (String), code (String). */
  static IRowMeta idNameCodeRowMeta() {
    IRowMeta rowMeta = InsertUpdateRun.idNameRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("code"));
    return rowMeta;
  }

  /**
   * A lookup that fails aborts the transaction. It holds no inserts or updates between two flushes,
   * so it is rolled back, and only that row goes to the error hop: the rows after it, and the
   * batches they are in, run as usual. The rows are looked up by id and by a code, and PostgreSQL
   * refuses the code of the bad rows, 3 (an update) and 6 (an insert): text can't hold a zero byte.
   * Every key condition is an equality, so the rows are batched.
   */
  @Test
  void aFailedLookupOnlyRejectsItsOwnRow() throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourPostgresIT.databaseMeta(container, "POSTGRESQL");
    InsertUpdateRun.execute(
        db,
        "DROP TABLE IF EXISTS target",
        "CREATE TABLE target (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(20) NOT NULL"
            + " CHECK (name <> 'bad'), code VARCHAR(10))",
        "INSERT INTO target (id, name, code) VALUES (1, 'x', 'c'), (3, 'x', 'c'), (5, 'x', 'c'),"
            + " (7, 'x', 'c'), (9, 'x', 'c')");
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 10; id++) {
      rows.add(new Object[] {id, "r" + id, id == 3 || id == 6 ? "c" + (char) 0 : "c"});
    }

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .watchCommits()
            .configure(
                meta -> {
                  InsertUpdateLookupField lookup = meta.getInsertUpdateLookupField();
                  lookup.getLookupKeys().add(new InsertUpdateKeyField("code", "code", "="));
                  lookup.getValueFields().add(new InsertUpdateValue("code", "code", false));
                })
            .run(idNameCodeRowMeta(), rows);

    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(List.of("ISU001", "ISU001"), r.errorCodes());
    assertEquals(List.of(LOOKUP, LOOKUP), r.errorKinds());
    assertTrue(
        r.errorField(InsertUpdateRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("0x00")),
        r.errorField(InsertUpdateRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertEquals(counters(8, 4, 4, 0, 8, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** A unique column outside the key, a NOT NULL column and a value that is too long. */
  @Test
  void rowsRefusedForOtherReasons() throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourPostgresIT.databaseMeta(container, "POSTGRESQL");
    InsertUpdateRun.execute(
        db,
        "DROP TABLE IF EXISTS target",
        "CREATE TABLE target (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(5) NOT NULL UNIQUE)",
        "INSERT INTO target VALUES (1, 's1'), (3, 's3'), (5, 's5')");

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .watchCommits()
            .run(
                List.of(
                    row(1, "a"),
                    row(2, "b"),
                    row(3, "a"),
                    row(4, null),
                    row(5, "c"),
                    row(6, "toolong"),
                    row(7, "b"),
                    row(8, "d")));

    assertEquals(ids(1, 2, 5, 8), r.outputKeys());
    assertEquals(ids(3, 4, 6, 7), r.errorKeys());
    assertEquals(Collections.nCopies(4, "ISU002"), r.errorCodes());
    assertEquals(List.of("1:a", "2:b", "3:s3", "5:c", "8:d"), InsertUpdateRun.table(db));
    assertEquals(counters(8, 2, 2, 0, 4, 4), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** Without an error hop a failed INSERT batch is rolled back and stops the transform. */
  @Test
  void withoutAnErrorHopAFailedInsertBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(4)
            .watchCommits()
            .run(rows(10, 6));

    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableWith(10, id -> id <= 4), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(8, 2, 2, 0, 4, 0), r.counters());
    assertTrue(r.recoveredBatches);
  }

  /**
   * Without an error hop a failed UPDATE batch is rolled back and stops the transform; the inserts
   * of its window, committed before it ran, are passed on.
   */
  @Test
  void withoutAnErrorHopAFailedUpdateBatchPassesOnTheCommittedInserts() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(4)
            .watchCommits()
            .run(rows(10, 7));

    assertEquals(ids(1, 2, 3, 4, 6, 8), r.outputKeys());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableWith(10, id -> id <= 4 || id == 6 || id == 8), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertEquals(counters(8, 4, 2, 0, 6, 0), r.counters());
  }

  /** Larger windows with a few bad rows, as in a real load. */
  @Test
  void largerBatches() throws Exception {
    DatabaseMeta db = target(2_000);
    long[] bad = {1, 2, 97, 194, 500, 501, 1_000, 1_999, 2_000};

    Result r =
        InsertUpdateRun.on(db).batch(true).errorHop(true).commitSize(500).run(rows(2_000, bad));

    assertEquals(idsWithout(2_000, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(tableWrittenExcept(2_000, bad), InsertUpdateRun.table(db));
    assertEquals(countersFor(2_000, bad), r.counters());
    assertRecovered(r);
  }

  /** HOP_DATABASE_BATCH_RECOVERY=LEGACY goes back to a savepoint per row, row by row. */
  @Test
  void theLegacyVariableGoesBackToASavepointPerRow() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(rows(10, 3, 6));

    assertFalse(r.recoveredBatches);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged(ONE_BY_ONE));
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(List.of("ISU001", "ISU001"), r.errorCodes());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 5, 6, 7, 8), r.outputSizeAtRead);
  }

  /** "As before" on the connection does the same, for that connection. */
  @Test
  void unknownOnTheConnectionGoesBackToASavepointPerRow() throws Exception {
    DatabaseMeta db = target("POSTGRESQL", "UNKNOWN", 10);

    Result r = InsertUpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 3, 6));

    assertFalse(r.recoveredBatches);
    assertTrue(r.useSavepoints);
    assertTrue(r.logged(ONE_BY_ONE));
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
  }

  /**
   * A dialect that extends PostgreSQL declares no contract and keeps a savepoint per row, unless
   * the contract is set on the connection.
   */
  @Test
  void aLookAlikeRecoversBatchesWithTheContractOnTheConnection() throws Exception {
    DatabaseMeta plain = target("POSTGRESQL_INSUPD_LOOKALIKE", null, 10);
    Result before =
        InsertUpdateRun.on(plain).batch(true).errorHop(true).commitSize(4).run(rows(10, 3));
    assertFalse(before.recoveredBatches);
    assertTrue(before.useSavepoints);

    DatabaseMeta db = target("POSTGRESQL_INSUPD_LOOKALIKE", "LOSES_WHOLE_BATCH", 10);
    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .watchCommits()
            .run(rows(10, 3, 6));

    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }
}

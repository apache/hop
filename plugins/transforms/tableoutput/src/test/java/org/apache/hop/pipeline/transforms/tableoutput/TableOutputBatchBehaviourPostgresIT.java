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

package org.apache.hop.pipeline.transforms.tableoutput;

import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.ROW;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idNameRowMeta;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idNameRows;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.ids;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idsWithout;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.tableWithSeeds;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.Result;
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
 * Pins what Table output does today on PostgreSQL when rows fail, with the same observations as
 * {@link TableOutputBatchBehaviourH2Test}: main output, error hop, table contents and counters.
 *
 * <p>PostgreSQL uses savepoints: with an error hop, "use batch update" is switched off and every
 * row runs behind a savepoint, so the failed rows go to the error hop one by one with TOP001.
 * Without an error hop it batches, and a failed statement aborts the whole transaction.
 */
class TableOutputBatchBehaviourPostgresIT {

  private static final String SELECT_TARGET = "SELECT id, name FROM target ORDER BY id";
  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
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

  private static DatabaseMeta databaseMeta() {
    return new DatabaseMeta(
        "pg",
        "POSTGRESQL",
        "Native",
        container.getHost(),
        container.getDatabaseName(),
        Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
        container.getUsername(),
        container.getPassword());
  }

  /** A fresh target table holding the seeded ids with the name "seed". */
  private static DatabaseMeta target(long... seeded) throws Exception {
    DatabaseMeta db = databaseMeta();
    TableOutputRun.execute(
        db,
        "DROP TABLE IF EXISTS target",
        "CREATE TABLE target (id BIGINT PRIMARY KEY, name VARCHAR(20))");
    for (long id : seeded) {
      TableOutputRun.execute(db, "INSERT INTO target VALUES (" + id + ", 'seed')");
    }
    return db;
  }

  private static List<String> table(DatabaseMeta db) throws Exception {
    return TableOutputRun.query(db, SELECT_TARGET);
  }

  private static List<Integer> list(Integer... values) {
    return Arrays.asList(values);
  }

  private static void assertCounters(
      Result r, long linesOutput, long linesWritten, long linesRejected, long errors) {
    assertEquals(
        List.of(linesOutput, linesWritten, linesRejected, errors),
        List.of(r.linesOutput, r.linesWritten, r.linesRejected, r.errorCount),
        "lines output, lines written, lines rejected, errors");
  }

  private static List<Long> keys(long... ids) {
    List<Long> keys = new ArrayList<>();
    for (long id : ids) {
      keys.add(id);
    }
    return keys;
  }

  // ----------------------------------------------------------------------------------------------
  // With an error hop: savepoints, one row at a time
  // ----------------------------------------------------------------------------------------------

  static Stream<Arguments> errorHopCases() {
    return Stream.of(
        Arguments.of(false, 100, 10, new long[] {3, 7}),
        Arguments.of(false, 3, 10, new long[] {3, 7}),
        Arguments.of(true, 5, 10, new long[] {2, 4}),
        Arguments.of(true, 5, 10, new long[] {1, 2, 3, 4, 5}),
        Arguments.of(true, 4, 10, new long[] {10}),
        Arguments.of(true, 4, 10, new long[] {}));
  }

  /**
   * With or without "use batch update", every row runs behind a savepoint and is passed on, or sent
   * to the error hop with TOP001, as soon as it is written.
   */
  @ParameterizedTest(name = "batch {0}, commit size {1}, seeded {3}")
  @MethodSource("errorHopCases")
  void errorHop_failedRowsGoToTheErrorHopOneByOne(
      boolean batch, int commitSize, int rows, long[] seeded) throws Exception {
    DatabaseMeta db = target(seeded);

    Result r =
        TableOutputRun.on(db)
            .batch(batch)
            .errorHop(true)
            .commitSize(commitSize)
            .run(idNameRowMeta(), idNameRows(rows));

    assertFalse(r.batchMode);
    assertTrue(r.useSafePoints);
    assertFalse(r.stopped);
    assertEquals(idsWithout(rows, seeded), r.outputKeys());
    assertEquals(ids(seeded), r.errorKeys());
    assertEquals(Collections.nCopies(seeded.length, "TOP001"), r.errorCodes());
    if (seeded.length > 0) {
      assertEquals(Collections.nCopies(seeded.length, ROW), r.errorKinds());
      assertEquals(keys(seeded), r.errorMessageKeys());
    }
    assertEquals(tableWithSeeds(rows, seeded), table(db));
    assertCounters(r, rows - seeded.length, rows - seeded.length, seeded.length, 0);

    // One by one: when row n is read, the n-1 rows before it have left on one stream or the other
    for (int read = 0; read < r.outputSizeAtRead.size(); read++) {
      assertEquals(
          Math.min(read, rows),
          r.outputSizeAtRead.get(read) + r.errorSizeAtRead.get(read),
          "rows passed on before read " + read);
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Without an error hop
  // ----------------------------------------------------------------------------------------------

  /** The rows passed on before the failure in the same commit window are rolled back. */
  @Test
  void batchOff_noErrorHop_rowsAlreadyPassedOnAreRolledBack() throws Exception {
    DatabaseMeta db = target(3);

    Result r =
        TableOutputRun.on(db).batch(false).commitSize(100).run(idNameRowMeta(), idNameRows(10));

    assertFalse(r.useSafePoints);
    assertTrue(r.stopped);
    assertEquals(ids(1, 2), r.outputKeys());
    assertEquals(List.of("3:seed"), table(db));
    assertCounters(r, 2, 2, 0, 1);
  }

  @Test
  void batchOff_noErrorHop_earlierCommitsStay() throws Exception {
    DatabaseMeta db = target(5);

    Result r =
        TableOutputRun.on(db).batch(false).commitSize(2).run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.stopped);
    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(tableWithSeeds(5, 5), table(db));
    assertCounters(r, 4, 4, 0, 1);
  }

  /** Lines output counts the rows of the failed batch: 9 for 5 rows written. */
  @Test
  void batchOn_noErrorHop_failedBatchIsRolledBack() throws Exception {
    DatabaseMeta db = target(7);

    Result r = TableOutputRun.on(db).batch(true).commitSize(5).run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.batchMode);
    assertFalse(r.useSafePoints);
    assertTrue(r.stopped);
    assertEquals(ids(1, 2, 3, 4, 5), r.outputKeys());
    assertEquals(List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "7:seed"), table(db));
    assertCounters(r, 9, 5, 0, 1);
    assertEquals(list(0, 0, 0, 0, 0, 5, 5, 5, 5, 5), r.outputSizeAtRead);
  }

  @Test
  void batchOn_noErrorHop_firstBatchFails() throws Exception {
    DatabaseMeta db = target(3);

    Result r = TableOutputRun.on(db).batch(true).commitSize(5).run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.stopped);
    assertTrue(r.output.isEmpty());
    assertEquals(List.of("3:seed"), table(db));
    assertCounters(r, 4, 0, 0, 1);
  }

  @Test
  void batchOn_noErrorHop_lastPartialBatchFails() throws Exception {
    DatabaseMeta db = target(10);

    Result r = TableOutputRun.on(db).batch(true).commitSize(4).run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.stopped);
    assertEquals(idsWithout(8), r.outputKeys());
    List<String> expected = tableWithSeeds(8);
    expected.add("10:seed");
    assertEquals(expected, table(db));
    assertCounters(r, 10, 8, 0, 1);
  }

  // ----------------------------------------------------------------------------------------------
  // Ignore insert errors
  // ----------------------------------------------------------------------------------------------

  /**
   * Buggy: without savepoints the failed row aborts the transaction, so every later insert fails
   * too and is ignored as well, and the commits do nothing. All ten rows are passed on and counted,
   * and none of them is in the table, rows 1 and 2 before the failure included.
   */
  @Test
  void ignoreErrors_batchOff_everyRowIsPassedOnAndNoneIsWritten() throws Exception {
    DatabaseMeta db = target(3);

    Result r =
        TableOutputRun.on(db)
            .batch(false)
            .commitSize(5)
            .configure(meta -> meta.setIgnoreErrors(true))
            .run(idNameRowMeta(), idNameRows(10));

    assertFalse(r.useSafePoints);
    assertFalse(r.stopped);
    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(List.of("3:seed"), table(db));
    assertCounters(r, 10, 10, 0, 0);
  }

  /** With batching the option has no effect: the transform fails as without it. */
  @Test
  void ignoreErrors_batchOn_isNotApplied() throws Exception {
    DatabaseMeta db = target(3);

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .commitSize(5)
            .configure(meta -> meta.setIgnoreErrors(true))
            .run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.batchMode);
    assertTrue(r.stopped);
    assertTrue(r.output.isEmpty());
    assertEquals(List.of("3:seed"), table(db));
    assertCounters(r, 4, 0, 0, 1);
  }

  // ----------------------------------------------------------------------------------------------
  // Table name in a field, generated keys
  // ----------------------------------------------------------------------------------------------

  /**
   * With an error hop the savepoints switch batching off, so the table name in a field does not mix
   * tables in a batch buffer: only the failed row is rejected.
   */
  @ParameterizedTest(name = "seeded {0}")
  @ValueSource(longs = {3, 4})
  void tableNameInField_errorHop_onlyTheFailedRowIsRejected(long seeded) throws Exception {
    DatabaseMeta db = databaseMeta();
    TableOutputRun.execute(
        db,
        "DROP TABLE IF EXISTS ta",
        "DROP TABLE IF EXISTS tb",
        "CREATE TABLE ta (id BIGINT PRIMARY KEY, name VARCHAR(20))",
        "CREATE TABLE tb (id BIGINT PRIMARY KEY, name VARCHAR(20))",
        "INSERT INTO " + (seeded % 2 == 1 ? "ta" : "tb") + " VALUES (" + seeded + ", 'seed')");
    IRowMeta rowMeta = idNameRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("tbl"));
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 10; id++) {
      rows.add(new Object[] {id, "r" + id, id % 2 == 1 ? "ta" : "tb"});
    }

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .configure(
                meta -> {
                  meta.setTableNameInField(true);
                  meta.setTableNameField("tbl");
                  meta.setTableNameInTable(false);
                })
            .run(rowMeta, rows);

    assertFalse(r.batchMode);
    assertEquals(idsWithout(10, seeded), r.outputKeys());
    assertEquals(ids(seeded), r.errorKeys());
    assertEquals(List.of("TOP001"), r.errorCodes());
    assertEquals(List.of(seeded), r.errorMessageKeys());
    List<String> expected = new ArrayList<>();
    for (String t : new String[] {"ta", "tb"}) {
      for (long id = t.equals("ta") ? 1 : 2; id <= 10; id += 2) {
        expected.add(t + ":" + id + ":" + (id == seeded ? "seed" : "r" + id));
      }
    }
    assertEquals(
        expected,
        TableOutputRun.query(
            db,
            "SELECT 'ta', id, name FROM ta UNION ALL SELECT 'tb', id, name FROM tb ORDER BY 1, 2"));
    assertCounters(r, 9, 9, 1, 0);
  }

  @Test
  void returningGeneratedKeys_errorHop() throws Exception {
    DatabaseMeta db = databaseMeta();
    TableOutputRun.execute(
        db,
        "DROP TABLE IF EXISTS gen",
        "CREATE TABLE gen (gid BIGSERIAL PRIMARY KEY, id BIGINT UNIQUE, name VARCHAR(20))",
        "INSERT INTO gen (id, name) VALUES (3, 'seed')");

    Result r =
        TableOutputRun.on(db)
            .table("gen")
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .configure(
                meta -> {
                  meta.setReturningGeneratedKeys(true);
                  meta.setGeneratedKeyField("gk");
                  meta.setSpecifyFields(true);
                  meta.getFields().add(new TableOutputField("id", "id"));
                  meta.getFields().add(new TableOutputField("name", "name"));
                })
            .run(idNameRowMeta(), idNameRows(6));

    assertFalse(r.batchMode);
    assertTrue(r.useSafePoints);
    assertArrayEquals(new String[] {"id", "name", "gk"}, r.outputRowMeta.getFieldNames());
    List<String> output = new ArrayList<>();
    for (Object[] row : r.output) {
      output.add(row[0] + ":" + row[1] + ":" + row[2]);
    }
    // The failed insert of row 3 uses up sequence value 4
    assertEquals(List.of("1:r1:2", "2:r2:3", "4:r4:5", "5:r5:6", "6:r6:7"), output);
    assertEquals(ids(3), r.errorKeys());
    assertEquals(List.of("TOP001"), r.errorCodes());
    assertEquals(List.of(3L), r.errorMessageKeys());
    assertEquals(
        List.of("1:3:seed", "2:1:r1", "3:2:r2", "5:4:r4", "6:5:r5", "7:6:r6"),
        TableOutputRun.query(db, "SELECT gid, id, name FROM gen ORDER BY gid"));
    assertCounters(r, 5, 5, 1, 0);
  }
}

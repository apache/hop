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

import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idNameRowMeta;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idNameRows;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.ids;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idsWithout;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.tableWithSeeds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
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
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Table output on PostgreSQL, whose driver loses the whole batch when a row fails: with an error
 * hop the transform keeps batching, rolls a failed batch back and finds the failing rows. Every row
 * must reach exactly one stream in input order, and the table must hold exactly the rows on the
 * main output, with and without pgjdbc's reWriteBatchedInserts.
 */
class TableOutputRecoveredBatchPostgresIT {

  private static final String SELECT_TARGET = "SELECT id, name FROM target ORDER BY id";
  private static final String SELECT_TA_TB =
      "SELECT 'ta', id, name FROM ta UNION ALL SELECT 'tb', id, name FROM tb ORDER BY 1, 2";

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

  private static DatabaseMeta databaseMeta(boolean rewrite) {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            "pg",
            "POSTGRESQL",
            "Native",
            container.getHost(),
            container.getDatabaseName(),
            Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
            container.getUsername(),
            container.getPassword());
    databaseMeta.addExtraOption("POSTGRESQL", "reWriteBatchedInserts", Boolean.toString(rewrite));
    return databaseMeta;
  }

  private static DatabaseMeta target(boolean rewrite, long... seeded) throws Exception {
    DatabaseMeta db = databaseMeta(rewrite);
    TableOutputRun.execute(
        db,
        "DROP TABLE IF EXISTS target",
        "CREATE TABLE target (id BIGINT PRIMARY KEY, name VARCHAR(20))");
    for (long id : seeded) {
      TableOutputRun.execute(db, "INSERT INTO target VALUES (" + id + ", 'seed')");
    }
    return db;
  }

  private static void assertCounters(
      Result r, long linesOutput, long linesWritten, long linesRejected, long errors) {
    assertEquals(
        List.of(linesOutput, linesWritten, linesRejected, errors),
        List.of(r.linesOutput, r.linesWritten, r.linesRejected, r.errorCount),
        "lines output, lines written, lines rejected, errors");
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] seededSets = {{}, {1}, {3}, {5}, {6}, {10}, {3, 4}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}};
    for (boolean rewrite : new boolean[] {false, true}) {
      for (long[] seeded : seededSets) {
        arguments.add(Arguments.of(rewrite, seeded));
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(boolean rewrite, long[] seeded) throws Exception {
    DatabaseMeta db = target(rewrite, seeded);

    Result r =
        TableOutputRun.on(db)
            .recovery(null)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.recoveredBatches);
    assertTrue(r.batchMode);
    assertFalse(r.useSafePoints);
    assertFalse(r.stopped);
    assertEquals(idsWithout(10, seeded), r.outputKeys());
    assertEquals(ids(seeded), r.errorKeys());
    assertEquals(tableWithSeeds(10, seeded), TableOutputRun.query(db, SELECT_TARGET));
    assertCounters(r, 10 - seeded.length, 10 - seeded.length, seeded.length, 0);
  }

  @Test
  void tableNameInFieldRoutesEveryRowToItsOwnStream() throws Exception {
    DatabaseMeta db = databaseMeta(false);
    TableOutputRun.execute(
        db,
        "DROP TABLE IF EXISTS ta",
        "DROP TABLE IF EXISTS tb",
        "CREATE TABLE ta (id BIGINT PRIMARY KEY, name VARCHAR(20))",
        "CREATE TABLE tb (id BIGINT PRIMARY KEY, name VARCHAR(20))",
        "INSERT INTO ta VALUES (3, 'seed')",
        "INSERT INTO tb VALUES (6, 'seed')");
    IRowMeta rowMeta = idNameRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("tbl"));
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 10; id++) {
      rows.add(new Object[] {id, "r" + id, id % 2 == 1 ? "ta" : "tb"});
    }

    Result r =
        TableOutputRun.on(db)
            .recovery(null)
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

    assertTrue(r.recoveredBatches);
    assertEquals(ids(1, 2, 4, 5, 7, 8, 9, 10), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(
        List.of(
            "ta:1:r1",
            "ta:3:seed",
            "ta:5:r5",
            "ta:7:r7",
            "ta:9:r9",
            "tb:2:r2",
            "tb:4:r4",
            "tb:6:seed",
            "tb:8:r8",
            "tb:10:r10"),
        TableOutputRun.query(db, SELECT_TA_TB));
    assertCounters(r, 8, 8, 2, 0);
  }

  @Test
  void withoutAnErrorHopAFailedBatchStopsAndOnlyCommittedRowsAreCounted() throws Exception {
    DatabaseMeta db = target(false, 7);

    Result r =
        TableOutputRun.on(db)
            .recovery(null)
            .batch(true)
            .errorHop(false)
            .commitSize(5)
            .run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.recoveredBatches);
    assertTrue(r.errorCount > 0);
    assertEquals(ids(1, 2, 3, 4, 5), r.outputKeys());
    assertEquals(
        List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "7:seed"),
        TableOutputRun.query(db, SELECT_TARGET));
    assertEquals(5, r.linesOutput);
  }

  @Test
  void theLegacyVariableKeepsTheSavepointPerRow() throws Exception {
    DatabaseMeta db = target(false, 3);

    Result r =
        TableOutputRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(idNameRowMeta(), idNameRows(10));

    assertFalse(r.recoveredBatches);
    assertFalse(r.batchMode);
    assertTrue(r.useSafePoints);
    assertEquals(idsWithout(10, 3), r.outputKeys());
  }
}

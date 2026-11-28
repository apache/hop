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

import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ids;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.rows;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWith;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWrittenExcept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins what Insert/Update does on DuckDB today, row by row. A failed statement aborts the
 * transaction until it is rolled back, and the driver closes the statement that failed; DuckDB has
 * no savepoints, and its dialect asks for none. Every test runs on the default path, with
 * HOP_DATABASE_BATCH_RECOVERY=LEGACY and with "As before" (UNKNOWN) as the connection's batch
 * failure contract, and all three must give the same.
 */
class InsertUpdateBatchBehaviourDuckDBTest {

  @TempDir static Path folder;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  /** An empty DuckDB database file. */
  static DatabaseMeta database() {
    String file = folder.resolve(UUID.randomUUID() + ".duckdb").toString();
    return new DatabaseMeta("duckdb", "DuckDB", "Native", "", file, "", "", "");
  }

  /** A DuckDB database file with the target table, holding the odd ids up to n. */
  static DatabaseMeta target(int n) throws Exception {
    DatabaseMeta databaseMeta = database();
    InsertUpdateRun.createTarget(databaseMeta, n);
    return databaseMeta;
  }

  private static List<Long> counters(
      long input, long inserted, long updated, long skipped, long written, long rejected) {
    return InsertUpdateBatchBehaviourH2Test.counters(
        input, inserted, updated, skipped, written, rejected);
  }

  /** Recovery variable and contract on the connection: default, LEGACY, and "As before". */
  static Stream<Arguments> rowByRowPaths() {
    return Stream.of(
        Arguments.of(null, null), Arguments.of("LEGACY", null), Arguments.of(null, "UNKNOWN"));
  }

  static DatabaseMeta target(int n, String contract) throws Exception {
    DatabaseMeta db = target(n);
    if (contract != null) {
      db.setBatchFailureContractOverride(contract);
    }
    return db;
  }

  /** Without a failure every row is inserted or updated, and passed on right after it. */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}")
  @MethodSource("rowByRowPaths")
  void errorHop_withoutFailuresEveryRowIsWritten(String recovery, String contract)
      throws Exception {
    DatabaseMeta db = target(10, contract);

    Result r = InsertUpdateRun.on(db).recovery(recovery).errorHop(true).commitSize(4).run(rows(10));

    assertFalse(r.useSavepoints);
    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(tableWrittenExcept(10), InsertUpdateRun.table(db));
    assertEquals(counters(10, 5, 5, 0, 10, 0), r.counters());
    assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), r.outputSizeAtRead);
  }

  /**
   * Buggy: the failed update of row 3 aborts the transaction. Every row after it goes to the error
   * hop as well, and rows 1 and 2, passed on right after their insert or update, are lost with the
   * transaction.
   */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}")
  @MethodSource("rowByRowPaths")
  void errorHop_Buggy_aFailedRowAbortsTheTransaction(String recovery, String contract)
      throws Exception {
    DatabaseMeta db = target(10, contract);

    Result r =
        InsertUpdateRun.on(db).recovery(recovery).errorHop(true).commitSize(5).run(rows(10, 3, 6));

    assertFalse(r.useSavepoints);
    assertEquals(ids(1, 2), r.outputKeys());
    assertEquals(ids(3, 4, 5, 6, 7, 8, 9, 10), r.errorKeys());
    assertEquals(Collections.nCopies(8, "ISU001"), r.errorCodes());
    assertEquals(tableWith(10, id -> false), InsertUpdateRun.table(db));
    assertEquals(0, r.errorCount);
    assertEquals(counters(3, 1, 1, 0, 2, 8), r.counters());
  }

  /**
   * Buggy: without an error hop a failed row stops the transform and the transaction is rolled
   * back; rows 5 and 6, already passed on, are not in the table.
   */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}")
  @MethodSource("rowByRowPaths")
  void noErrorHop_Buggy_rowsPassedOnSinceTheLastCommitAreRolledBack(
      String recovery, String contract) throws Exception {
    DatabaseMeta db = target(10, contract);

    Result r =
        InsertUpdateRun.on(db).recovery(recovery).errorHop(false).commitSize(4).run(rows(10, 7));

    assertEquals(idsWithout(6), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(7, 3, 3, 0, 6, 0), r.counters());
  }
}

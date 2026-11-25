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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.pipeline.transforms.update.UpdateRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Update on DuckDB, whose contract LOSES_WHOLE_BATCH is validated: a failed batch keeps none of its
 * rows and the driver closes the statement; there are no savepoints. Failed batches are recovered:
 * every row reaches exactly one stream, in input order, after its commit.
 */
class UpdateRecoveredBatchDuckDBTest {

  @TempDir static Path folder;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  static DatabaseMeta target(int n) throws Exception {
    String file = folder.resolve(UUID.randomUUID() + ".duckdb").toString();
    DatabaseMeta databaseMeta =
        new DatabaseMeta("duckdb", "DuckDB", "Native", "", file, "", "", "");
    UpdateRun.createTarget(databaseMeta, n);
    return databaseMeta;
  }

  private static List<Long> counters(
      long input, long updated, long skipped, long written, long rejected) {
    return UpdateBatchBehaviourH2Test.counters(input, updated, skipped, written, rejected);
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {{}, {1}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}};
    for (int commitSize : new int[] {1, 3, 5}) {
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
    assertEquals(tableUpdatedExcept(10, bad), UpdateRun.table(db));
    assertEquals(counters(10, 10 - bad.length, 0, 10 - bad.length, bad.length), r.counters());
    assertPassedOnAfterTheCommit(r);
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
    assertEquals(List.of("[11]", "[12]"), r.keysNotFound());
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(counters(8, 3, 2, 5, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** Without an error hop: unchanged, the failed batch is rolled back and the transform stops. */
  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(false).commitSize(5).run(rows(10, 7));

    assertEquals(ids(1, 2, 3, 4, 5), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 5), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertTrue(r.recoveredBatches);
  }

  /** id (Integer), name (String), key (String): the id once more, as text. */
  static IRowMeta keyTextRowMeta() {
    IRowMeta rowMeta = UpdateRun.idNameRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("key"));
    return rowMeta;
  }

  /**
   * Rows 1..n named rN with their id as text in the key, except for the bad ids: DuckDB can't make
   * a number of their key, so their lookup fails.
   */
  static List<Object[]> keyTextRows(int n, long... bad) {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      rows.add(
          new Object[] {
            id, "r" + id, UpdateRun.contains(bad, id) ? "no number" : Long.toString(id)
          });
    }
    return rows;
  }

  /** Looks the rows up by id, and once more by the key: {@code id = key}. */
  static void lookUpByTheKeyAsWell(UpdateMeta meta) {
    meta.getLookupField().getLookupKeys().add(new UpdateKeyField("key", "id", "="));
  }

  /**
   * DuckDB closes a statement that fails. A lookup that fails is rolled back and its statement is
   * prepared again, so only the row whose lookup failed goes to the error hop; the rows after it
   * are looked up and updated as usual. Before, every row after it went to the error hop with
   * "Statement was closed".
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 4, 100})
  void aFailedLookupOnlyRejectsItsOwnRow(int commitSize) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .configure(UpdateRecoveredBatchDuckDBTest::lookUpByTheKeyAsWell)
            .run(keyTextRowMeta(), keyTextRows(10, 3, 7));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("UPD001", "UPD001"), r.errorCodes());
    assertTrue(
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("Conversion Error")),
        r.errorField(UpdateRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(counters(8, 8, 0, 8, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** Many batches, several of them failing: the statement is prepared again after each. */
  @Test
  void manyFailedBatches() throws Exception {
    DatabaseMeta db = target(500);
    long[] bad = {1, 2, 50, 99, 100, 101, 250, 377, 499, 500};

    Result r = UpdateRun.on(db).batch(true).errorHop(true).commitSize(50).run(rows(500, bad));

    assertEquals(idsWithout(500, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(tableUpdatedExcept(500, bad), UpdateRun.table(db));
    assertEquals(counters(500, 490, 0, 490, 10), r.counters());
    assertTrue(r.recoveredBatches);
  }
}

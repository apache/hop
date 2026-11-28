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

import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.assertPassedOnAfterTheCommit;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.assertRecovered;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.counters;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.countersFor;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ids;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.row;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.rows;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWith;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWrittenExcept;
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
import org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Insert/Update with "Use batch updates" on DuckDB, whose contract LOSES_WHOLE_BATCH is validated:
 * a failed batch keeps none of its rows and the driver closes the statement; there are no
 * savepoints. Failed batches are recovered, the INSERT and the UPDATE statement prepared again when
 * the driver closed them: every row reaches exactly one stream, in input order, after its commit.
 */
class InsertUpdateRecoveredBatchDuckDBTest {

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

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {
      {}, {1}, {2}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}
    };
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
    assertEquals(tableWrittenExcept(10, bad), InsertUpdateRun.table(db));
    assertEquals(countersFor(10, bad), r.counters());
    assertPassedOnAfterTheCommit(r);
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
    DatabaseMeta db = database();
    InsertUpdateRun.createTarget(db, 1L);

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

  /**
   * Without an error hop a failed UPDATE batch is rolled back and stops the transform; the inserts
   * of its window, committed before it, are passed on.
   */
  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = target(10);

    Result r = InsertUpdateRun.on(db).batch(true).errorHop(false).commitSize(5).run(rows(10, 7));

    assertEquals(ids(1, 2, 3, 4, 5, 6, 8, 10), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 5 || id % 2 == 0), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertTrue(r.recoveredBatches);
  }

  /** id (Integer), name (String), key (String): the id once more, as text. */
  static IRowMeta keyTextRowMeta() {
    IRowMeta rowMeta = InsertUpdateRun.idNameRowMeta();
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
            id, "r" + id, InsertUpdateRun.contains(bad, id) ? "no number" : Long.toString(id)
          });
    }
    return rows;
  }

  /** Looks the rows up by id, and once more by the key: {@code id = key}. */
  static void lookUpByTheKeyAsWell(InsertUpdateMeta meta) {
    meta.getInsertUpdateLookupField()
        .getLookupKeys()
        .add(new InsertUpdateKeyField("key", "id", "="));
  }

  /**
   * DuckDB closes a statement that fails. A lookup that fails is rolled back and its statement is
   * prepared again, so only the row whose lookup failed goes to the error hop; the rows after it,
   * and the batches they are in, are looked up and written as usual. DuckDB only converts the key
   * of a row it finds by its id, so the bad keys are on ids in the table, 3 and 7.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 4, 100})
  void aFailedLookupOnlyRejectsItsOwnRow(int commitSize) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .configure(InsertUpdateRecoveredBatchDuckDBTest::lookUpByTheKeyAsWell)
            .run(keyTextRowMeta(), keyTextRows(10, 3, 7));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("ISU001", "ISU001"), r.errorCodes());
    assertTrue(
        r.errorField(InsertUpdateRun.ERROR_DESCRIPTIONS).stream()
            .allMatch(d -> ((String) d).contains("Conversion Error")),
        r.errorField(InsertUpdateRun.ERROR_DESCRIPTIONS).toString());
    assertEquals(tableWrittenExcept(10, 3, 7), InsertUpdateRun.table(db));
    assertEquals(counters(8, 5, 3, 0, 8, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** Many windows, several of them failing: the statements are prepared again after each. */
  @Test
  void manyFailedBatches() throws Exception {
    DatabaseMeta db = target(500);
    long[] bad = {1, 2, 50, 99, 100, 101, 250, 377, 499, 500};

    Result r = InsertUpdateRun.on(db).batch(true).errorHop(true).commitSize(50).run(rows(500, bad));

    assertEquals(idsWithout(500, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(tableWrittenExcept(500, bad), InsertUpdateRun.table(db));
    assertEquals(countersFor(500, bad), r.counters());
    assertTrue(r.recoveredBatches);
  }

  /** HOP_DATABASE_BATCH_RECOVERY=LEGACY keeps the rows one by one, as without the option. */
  @Test
  void theLegacyVariableKeepsTheRowsOneByOne() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(rows(10, 3, 6));

    assertFalse(r.recoveredBatches);
    assertTrue(r.logged(InsertUpdateRecoveredBatchH2Test.ONE_BY_ONE));
    // As pinned by InsertUpdateBatchBehaviourDuckDBTest: the failed row aborts the transaction
    assertEquals(ids(1, 2), r.outputKeys());
    assertEquals(ids(3, 4, 5, 6, 7, 8, 9, 10), r.errorKeys());
  }
}

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
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Table output on a connection whose batch failure contract is known: failed batches are recovered
 * with RecoverableBatch. Every row must reach exactly one stream, in input order, the table must
 * hold exactly the rows on the main output, and the counters must be exact. H2 declares
 * MARKS_FAILED_ROWS; the other contracts are forced through the connection option, because each of
 * them has to route the rows correctly on any driver whose behaviour it covers.
 */
class TableOutputRecoveredBatchH2Test {

  private static final String CREATE_TARGET =
      "CREATE TABLE target (id BIGINT PRIMARY KEY, name VARCHAR(20))";
  private static final String SELECT_TARGET = "SELECT id, name FROM target ORDER BY id";
  private static final String SELECT_TA_TB =
      "SELECT 'ta', id, name FROM ta UNION ALL SELECT 'tb', id, name FROM tb ORDER BY 1, 2";

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  private static DatabaseMeta database(String contract, String... statements) throws Exception {
    String name =
        "mem:recovered" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta = new DatabaseMeta("h2", "H2", "Native", "", name, "", "sa", "");
    if (contract != null) {
      databaseMeta.setBatchFailureContractOverride(contract);
    }
    TableOutputRun.execute(databaseMeta, statements);
    return databaseMeta;
  }

  private static DatabaseMeta target(String contract, long... seeded) throws Exception {
    DatabaseMeta databaseMeta = database(contract, CREATE_TARGET);
    for (long id : seeded) {
      TableOutputRun.execute(databaseMeta, "INSERT INTO target VALUES (" + id + ", 'seed')");
    }
    return databaseMeta;
  }

  private static void assertCounters(
      Result r, long linesOutput, long linesWritten, long linesRejected, long errors) {
    assertEquals(
        List.of(linesOutput, linesWritten, linesRejected, errors),
        List.of(r.linesOutput, r.linesWritten, r.linesRejected, r.errorCount),
        "lines output, lines written, lines rejected, errors");
  }

  /** H2's own contract, and the others forced on the connection. */
  static Stream<String> contracts() {
    return Stream.of(null, "KEEPS_PREFIX", "LOSES_WHOLE_BATCH");
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] seededSets = {
      {}, {1}, {3}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}
    };
    contracts()
        .forEach(
            contract -> {
              for (long[] seeded : seededSets) {
                arguments.add(Arguments.of(contract, seeded));
              }
            });
    return arguments.stream();
  }

  @ParameterizedTest
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(String contract, long[] seeded) throws Exception {
    DatabaseMeta db = target(contract, seeded);

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
    assertEquals(Collections.nCopies(seeded.length, "TOP0002"), r.errorCodes());
    assertEquals(tableWithSeeds(10, seeded), TableOutputRun.query(db, SELECT_TARGET));
    assertCounters(r, 10 - seeded.length, 10 - seeded.length, seeded.length, 0);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 7, 100})
  void anyCommitSizeRoutesTheRowsTheSame(int commitSize) throws Exception {
    long[] seeded = {2, 5, 9};
    DatabaseMeta db = target(null, seeded);

    Result r =
        TableOutputRun.on(db)
            .recovery(null)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.recoveredBatches);
    assertEquals(idsWithout(10, seeded), r.outputKeys());
    assertEquals(ids(seeded), r.errorKeys());
    assertEquals(tableWithSeeds(10, seeded), TableOutputRun.query(db, SELECT_TARGET));
    assertCounters(r, 7, 7, 3, 0);
  }

  @Test
  void aKeyRepeatedWithinOneBatchRejectsTheSecondRow() throws Exception {
    DatabaseMeta db = target(null);
    List<Object[]> rows = new ArrayList<>(idNameRows(5));
    rows.add(3, new Object[] {3L, "again"});

    Result r =
        TableOutputRun.on(db)
            .recovery(null)
            .batch(true)
            .errorHop(true)
            .commitSize(10)
            .run(idNameRowMeta(), rows);

    assertEquals(ids(1, 2, 3, 4, 5), r.outputKeys());
    assertEquals(ids(3), r.errorKeys());
    assertEquals("again", r.errors.get(0)[1]);
    assertEquals(tableWithSeeds(5), TableOutputRun.query(db, SELECT_TARGET));
  }

  /** Odd ids go to table ta, even ids to tb, named in field "tbl" that is not inserted. */
  private static Result runTableNameInField(DatabaseMeta db) throws Exception {
    IRowMeta rowMeta = idNameRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("tbl"));
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 10; id++) {
      rows.add(new Object[] {id, "r" + id, id % 2 == 1 ? "ta" : "tb"});
    }
    return TableOutputRun.on(db)
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
  }

  private static List<String> twoTablesWithSeeds(long... seeded) {
    List<String> lines = new ArrayList<>();
    for (String t : new String[] {"ta", "tb"}) {
      for (long id = t.equals("ta") ? 1 : 2; id <= 10; id += 2) {
        boolean seed = false;
        for (long s : seeded) {
          seed |= s == id;
        }
        lines.add(t + ":" + id + ":" + (seed ? "seed" : "r" + id));
      }
    }
    return lines;
  }

  static Stream<long[]> tableNameInFieldFailures() {
    return Stream.of(new long[] {}, new long[] {3}, new long[] {4}, new long[] {3, 6, 9});
  }

  /**
   * Each table has its own batch, so a failure is only matched against the rows of that table, and
   * the rows still leave in input order although the two tables commit at different times.
   */
  @ParameterizedTest
  @MethodSource("tableNameInFieldFailures")
  void tableNameInFieldRoutesEveryRowToItsOwnStream(long[] seeded) throws Exception {
    DatabaseMeta db =
        database(
            null,
            "CREATE TABLE ta (id BIGINT PRIMARY KEY, name VARCHAR(20))",
            "CREATE TABLE tb (id BIGINT PRIMARY KEY, name VARCHAR(20))");
    for (long id : seeded) {
      TableOutputRun.execute(
          db, "INSERT INTO " + (id % 2 == 1 ? "ta" : "tb") + " VALUES (" + id + ", 'seed')");
    }

    Result r = runTableNameInField(db);

    assertTrue(r.recoveredBatches);
    assertFalse(r.stopped);
    assertEquals(idsWithout(10, seeded), r.outputKeys());
    assertEquals(ids(seeded), r.errorKeys());
    assertEquals(twoTablesWithSeeds(seeded), TableOutputRun.query(db, SELECT_TA_TB));
    assertCounters(r, 10 - seeded.length, 10 - seeded.length, seeded.length, 0);
  }

  /**
   * Without an error hop a failed batch rolls back and stops the transform. Only the rows of the
   * batches committed before it were passed on, and only they are counted.
   */
  @Test
  void withoutAnErrorHopAFailedBatchStopsAndOnlyCommittedRowsAreCounted() throws Exception {
    DatabaseMeta db = target(null, 7);

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
    assertTrue(r.errors.isEmpty());
    List<String> table = TableOutputRun.query(db, SELECT_TARGET);
    assertEquals(List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "7:seed"), table);
    assertEquals(5, r.linesOutput);
  }

  @Test
  void theLastPartialBatchIsRecoveredToo() throws Exception {
    DatabaseMeta db = target(null, 10);

    Result r =
        TableOutputRun.on(db)
            .recovery(null)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(idNameRowMeta(), idNameRows(10));

    assertEquals(idsWithout(10, 10), r.outputKeys());
    assertEquals(ids(10), r.errorKeys());
    assertEquals(tableWithSeeds(10, 10), TableOutputRun.query(db, SELECT_TARGET));
    assertCounters(r, 9, 9, 1, 0);
  }

  @Test
  void unknownOnTheConnectionKeepsTheHandlingAsBefore() throws Exception {
    DatabaseMeta db = target("UNKNOWN", 3);

    Result r =
        TableOutputRun.on(db)
            .recovery(null)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(idNameRowMeta(), idNameRows(10));

    assertFalse(r.recoveredBatches);
    assertTrue(r.batchMode);
  }

  @Test
  void theLegacyVariableKeepsTheHandlingAsBefore() throws Exception {
    DatabaseMeta db = target(null, 3);

    Result r =
        TableOutputRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(idNameRowMeta(), idNameRows(10));

    assertFalse(r.recoveredBatches);
  }

  @Test
  void batchUpdatesOffOrGeneratedKeysKeepTheRowByRowPath() throws Exception {
    Result off =
        TableOutputRun.on(target(null))
            .recovery(null)
            .batch(false)
            .errorHop(true)
            .commitSize(5)
            .run(idNameRowMeta(), idNameRows(3));
    assertFalse(off.recoveredBatches);
    assertFalse(off.batchMode);

    Result unlimited =
        TableOutputRun.on(target(null))
            .recovery(null)
            .batch(true)
            .errorHop(true)
            .commitSize(0)
            .run(idNameRowMeta(), idNameRows(3));
    assertFalse(unlimited.recoveredBatches);
    assertFalse(unlimited.batchMode);
  }
}

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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.pipeline.transforms.delete.DeleteRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Delete on DuckDB, whose contract LOSES_WHOLE_BATCH is validated: a failed batch keeps none of its
 * rows and the driver closes the statement; there are no savepoints. Failed batches are recovered:
 * every row reaches exactly one stream, in input order, after its commit.
 */
class DeleteRecoveredBatchDuckDBTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  @TempDir static Path folder;

  /** A DuckDB database file with the target table holding ids 1..n, the given ids referenced. */
  static DatabaseMeta target(int n, long... referenced) throws Exception {
    String file = folder.resolve(UUID.randomUUID() + ".duckdb").toString();
    DatabaseMeta databaseMeta =
        new DatabaseMeta("duckdb", "DuckDB", "Native", "", file, "", "", "");
    DeleteRun.createTarget(databaseMeta, n, referenced);
    return databaseMeta;
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
    for (int i = 0; i < bad.length; i++) {
      assertTrue(DeleteBatchBehaviourDuckDBTest.foreignKeyViolation(r, i));
    }
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

  /**
   * A key Hop can't bind goes to the error hop in its place, between the rows of a batch that
   * fails; the closed statement is prepared again for the rows after it.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 5, 100})
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

  /** Without an error hop: the failed batch is rolled back and the transform stops. */
  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = target(10, 7);

    Result r =
        DeleteRun.on(db).batch(true).errorHop(false).commitSize(5).watchCommits().run(rows(10));

    assertEquals(ids(1, 2, 3, 4, 5), r.outputKeys());
    assertEquals(ids(6, 7, 8, 9, 10), remaining(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(10, 5, 5, 0), r.counters());
    assertTrue(r.recoveredBatches);
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

  /** Many batches, several of them failing: the statement is prepared again after each. */
  @Test
  void manyFailedBatches() throws Exception {
    long[] bad = {1, 2, 50, 99, 100, 101, 250, 377, 499, 500};
    DatabaseMeta db = target(500, bad);

    Result r = DeleteRun.on(db).batch(true).errorHop(true).commitSize(50).run(rows(500));

    assertEquals(idsWithout(500, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(ids(bad), remaining(db));
    assertEquals(counters(500, 490, 490, 10), r.counters());
    assertTrue(r.recoveredBatches);
  }

  /** HOP_DATABASE_BATCH_RECOVERY=LEGACY goes back to the handling as before. */
  @Test
  void theLegacyVariableKeepsTheHandlingAsBefore() throws Exception {
    DatabaseMeta db = target(10, 8);

    Result r =
        DeleteRun.on(db).recovery("LEGACY").batch(true).errorHop(true).commitSize(5).run(rows(10));

    assertFalse(r.recoveredBatches);
    assertTrue(r.batchMode);
    assertEquals(ids(10), r.errorKeys());
    assertEquals(1, r.errorCount);
  }
}

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

import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.DRIVER;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.contains;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ids;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.row;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.rows;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWith;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.tableWrittenExcept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateBatchBehaviourH2Test.H2SavepointsDatabaseMeta;
import org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.Result;
import org.apache.hop.pipeline.transforms.insertupdate.SimulatedBatchDriver.Shape;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Insert/Update with "Use batch updates" on a connection whose batch failure contract is known: the
 * inserts and the updates go out in recovered batches, also with an error hop. Every row must reach
 * exactly one stream, in input order, once it is committed or refused; the table must hold exactly
 * the rows on the main output, and the counters must be exact.
 *
 * <p>H2 declares MARKS_FAILED_ROWS. The other contracts are set on the connection, each with H2 and
 * with a simulated driver that behaves the way the contract describes, because each of them has to
 * route the rows correctly on any driver whose behaviour it covers.
 *
 * <p>The table holds the odd ids, so in rows 1..n the odd ids are updated and the even ones
 * inserted, one after the other: every window of rows has an insert batch and an update batch.
 */
class InsertUpdateRecoveredBatchH2Test {

  /** A dialect that extends H2 and so declares no contract: it has not been measured. */
  @DatabaseMetaPlugin(type = "H2_INSUPD_UNVALIDATED", typeDescription = "H2, not validated")
  public static class UnvalidatedH2DatabaseMeta extends H2DatabaseMeta {}

  static final String ONE_BY_ONE = "the rows are inserted and updated one by one";

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2SavepointsDatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(UnvalidatedH2DatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  /** A contract on the connection (null: H2's own) and a simulated driver (null: H2 itself). */
  record Setup(String contract, Shape driver) {
    @Override
    public String toString() {
      return (contract == null ? "MARKS_FAILED_ROWS (H2)" : contract)
          + " on "
          + (driver == null ? "H2" : driver.name());
    }
  }

  static Stream<Setup> setups() {
    return Stream.of(
        new Setup(null, null),
        new Setup(null, Shape.PREFIX_WITHOUT_COUNTS),
        new Setup("KEEPS_PREFIX", Shape.KEEPS_PREFIX),
        new Setup("KEEPS_PREFIX", null),
        new Setup("LOSES_WHOLE_BATCH", null),
        new Setup("LOSES_WHOLE_BATCH", Shape.PREFIX_WITHOUT_COUNTS));
  }

  static DatabaseMeta target(String type, String contract, int n) throws Exception {
    DatabaseMeta databaseMeta = InsertUpdateBatchBehaviourH2Test.target(type, n);
    if (contract != null) {
      databaseMeta.setBatchFailureContractOverride(contract);
    }
    return databaseMeta;
  }

  static DatabaseMeta target(Setup setup, int n) throws Exception {
    return target("H2", setup.contract(), n);
  }

  static InsertUpdateRun recovered(DatabaseMeta db, Setup setup) {
    return InsertUpdateRun.on(db).driver(setup.driver()).batch(true).errorHop(true).watchCommits();
  }

  static List<Long> counters(
      long input, long inserted, long updated, long skipped, long written, long rejected) {
    return InsertUpdateBatchBehaviourH2Test.counters(
        input, inserted, updated, skipped, written, rejected);
  }

  /** The counters of rows 1..n, odd ids in the table, with the given rows refused. */
  static List<Long> countersFor(int n, long... bad) {
    long inserted = 0;
    long updated = 0;
    for (long id = 1; id <= n; id++) {
      if (!contains(bad, id)) {
        if (id % 2 == 0) {
          inserted++;
        } else {
          updated++;
        }
      }
    }
    return counters(n, inserted, updated, 0, n - bad.length, bad.length);
  }

  /**
   * Every row is committed when it goes to the output, and not written when it goes to the error
   * hop.
   */
  static void assertPassedOnAfterTheCommit(Result r) {
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    for (int i = 0; i < r.errors.size(); i++) {
      String committed = r.committedAtError.get(i);
      Object name = r.errors.get(i)[1];
      assertFalse(
          committed != null && committed.equals(name),
          "row " + r.errors.get(i)[0] + " went to the error hop, but its write is committed");
    }
  }

  /** Recovered batches: also with an error hop, and no savepoint per row. */
  static void assertRecovered(Result r) {
    assertTrue(r.recoveredBatches);
    assertFalse(r.useSavepoints);
    assertFalse(r.logged(ONE_BY_ONE));
    assertTrue(r.loggedDetailed("Batch support: batch failure contract"));
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {
      {}, {1}, {2}, {3}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}
    };
    setups()
        .forEach(
            setup -> {
              for (long[] bad : badSets) {
                arguments.add(Arguments.of(setup, bad));
              }
            });
    return arguments.stream();
  }

  /**
   * Inserts and updates interleaved, refused rows among both: only the refused rows go to the error
   * hop, each once, the others are committed and passed on, all in input order.
   */
  @ParameterizedTest(name = "{0}, bad rows {1}")
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(Setup setup, long[] bad) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r = recovered(db, setup).commitSize(5).run(rows(10, bad));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(Collections.nCopies(bad.length, "ISU002"), r.errorCodes());
    assertEquals(Collections.nCopies(bad.length, DRIVER), r.errorKinds());
    InsertUpdateBatchBehaviourH2Test.assertNoErrorFields(r);
    assertEquals(tableWrittenExcept(10, bad), InsertUpdateRun.table(db));
    assertEquals(countersFor(10, bad), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  static Stream<Arguments> commitSizes() {
    List<Arguments> arguments = new ArrayList<>();
    setups()
        .forEach(
            setup -> {
              for (int commitSize : new int[] {1, 2, 3, 4, 7, 100}) {
                arguments.add(Arguments.of(setup, commitSize));
              }
            });
    return arguments.stream();
  }

  /**
   * Any commit size routes the rows the same, and a window holds exactly the commit size: its
   * batches run, are committed and pass their rows on once it is full, and the last, partial one at
   * the end of the input.
   */
  @ParameterizedTest(name = "{0}, commit size {1}")
  @MethodSource("commitSizes")
  void everyWindowHoldsTheCommitSize(Setup setup, int commitSize) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r = recovered(db, setup).commitSize(commitSize).run(rows(10, 2, 5, 9));

    assertEquals(idsWithout(10, 2, 5, 9), r.outputKeys());
    assertEquals(ids(2, 5, 9), r.errorKeys());
    assertEquals(tableWrittenExcept(10, 2, 5, 9), InsertUpdateRun.table(db));
    assertEquals(countersFor(10, 2, 5, 9), r.counters());
    // Before read k (0-based) the rows of every full window before it have left, no more
    List<Integer> settledAtRead = new ArrayList<>();
    for (int read = 0; read < r.outputSizeAtRead.size(); read++) {
      settledAtRead.add(r.outputSizeAtRead.get(read) + r.errorSizeAtRead.get(read));
    }
    List<Integer> expected = new ArrayList<>();
    for (int read = 0; read <= 10; read++) {
      expected.add(read / commitSize * commitSize);
    }
    assertEquals(expected, settledAtRead);
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** The rows of a window leave together, once both its batches have run and are committed. */
  @Test
  void theRowsOfAWindowLeaveTogether() throws Exception {
    DatabaseMeta db = target("H2", null, 10);

    Result r = InsertUpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 3, 6));

    assertEquals(List.of(0, 0, 0, 0, 3, 3, 3, 3, 6, 6, 6), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2), r.errorSizeAtRead);
    assertRecovered(r);
  }

  static Stream<Arguments> mixedRowCases() {
    List<Arguments> arguments = new ArrayList<>();
    setups()
        .forEach(
            setup -> {
              for (int commitSize : new int[] {1, 3, 5, 100}) {
                arguments.add(Arguments.of(setup, commitSize));
              }
            });
    return arguments.stream();
  }

  /**
   * A row that needs no change goes to the output in its place between the rows of the batches,
   * also when a batch fails; it waits for the rows before it.
   */
  @ParameterizedTest(name = "{0}, commit size {1}")
  @MethodSource("mixedRowCases")
  void rowsThatNeedNoChangeKeepTheirPlace(Setup setup, int commitSize) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        recovered(db, setup)
            .commitSize(commitSize)
            .run(InsertUpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(ids(1, 2, 3, 5, 6, 9), r.outputKeys());
    assertEquals(ids(4, 7), r.errorKeys());
    assertEquals(List.of("ISU002", "ISU002"), r.errorCodes());
    assertEquals(InsertUpdateBatchBehaviourH2Test.mixedTable(), InsertUpdateRun.table(db));
    assertEquals(counters(8, 2, 2, 2, 6, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /**
   * The rows that wait behind the batches count towards the commit size: with commit size 3, rows
   * 1, 2 and 3 make the first batches run although row 3 is in none of them.
   */
  @Test
  void rowsWaitingBehindTheBatchesCountTowardsTheCommitSize() throws Exception {
    DatabaseMeta db = target("H2", null, 10);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .run(InsertUpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(List.of(0, 0, 0, 3, 3, 3, 5, 5, 5), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 0, 0, 1, 1, 1), r.errorSizeAtRead);
    assertEquals(ids(1, 2, 3, 5, 6, 9), r.outputKeys());
    assertRecovered(r);
  }

  // ----------------------------------------------------------------------------------------------
  // A key that waits in a batch: the batches are written before it is looked up
  // ----------------------------------------------------------------------------------------------

  /**
   * A key that is still waiting in the INSERT batch writes both batches before its lookup, so its
   * second occurrence finds the row and becomes an update instead of a second insert; the third one
   * finds nothing to change. Exactly as row by row.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void aKeyThatComesTwiceIsInsertedOnceThenUpdated(Setup setup) throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourH2Test.database("H2");
    InsertUpdateRun.createTarget(db, 1L);
    if (setup.contract() != null) {
      db.setBatchFailureContractOverride(setup.contract());
    }

    Result r =
        recovered(db, setup)
            .commitSize(100)
            .run(
                List.of(
                    row(2, "first"),
                    row(2, "second"),
                    row(1, "r1"),
                    row(2, "second"),
                    row(1, "r1")));

    assertEquals(ids(2, 2, 1, 2, 1), r.outputKeys());
    assertEquals(ids(), r.errorKeys());
    assertEquals(List.of("1:r1", "2:second"), InsertUpdateRun.table(db));
    assertEquals(counters(5, 1, 2, 2, 5, 0), r.counters());
    // Row 2 waits for row 1 to be written, row 4 for rows 2 and 3; row 5 waits for nothing
    assertEquals(List.of(0, 0, 1, 1, 4, 5), r.outputSizeAtRead);
    assertRecovered(r);
  }

  /** A key that waits in the UPDATE batch is written first too: the next row compares with it. */
  @Test
  void aKeyUpdatedTwiceInAWindowIsWrittenBetweenTheUpdates() throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourH2Test.database("H2");
    InsertUpdateRun.createTarget(db, 1L);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .run(List.of(row(1, "a"), row(1, "b"), row(1, "b")));

    assertEquals(ids(1, 1, 1), r.outputKeys());
    assertEquals(List.of("1:b"), InsertUpdateRun.table(db));
    assertEquals(counters(3, 0, 2, 1, 3, 0), r.counters());
    assertRecovered(r);
  }

  /**
   * On a key column without case, "a" finds the "A" inserted just before it: the keys of waiting
   * rows compare without case, so "A" is written before "a" is looked up.
   */
  @Test
  void aKeyThatComesTwiceInAnotherCaseIsUpdatedOnACaseInsensitiveColumn() throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourH2Test.caseInsensitiveTarget();

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .configure(InsertUpdateBatchBehaviourH2Test::byCode)
            .run(
                InsertUpdateBatchBehaviourH2Test.codeNameRowMeta(),
                List.of(
                    new Object[] {"A", "first"},
                    new Object[] {"a", "second"},
                    new Object[] {"B", "b"}));

    assertEquals(List.of("A", "a", "B"), r.outputKeys());
    assertEquals(
        List.of("A:second", "B:b"),
        InsertUpdateRun.query(db, "SELECT code, name FROM ci_target ORDER BY code"));
    assertEquals(counters(3, 2, 1, 0, 3, 0), r.counters());
    assertRecovered(r);
  }

  /**
   * A key held as text finds the number that waits in a batch: the database casts '07' to the
   * number 7 to compare it with the numeric column.
   */
  @Test
  void aKeyAsTextIsTheSameKeyAsTheNumberItHolds() throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourH2Test.database("H2");
    InsertUpdateRun.createTarget(db);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("other"));

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .configure(
                meta -> {
                  // Looked up by "other" as well, a text that holds the id
                  meta.getInsertUpdateLookupField()
                      .getLookupKeys()
                      .add(new InsertUpdateKeyField("other", "id", "="));
                })
            .run(rowMeta, List.of(new Object[] {7L, "a", "7"}, new Object[] {7L, "b", "07"}));

    assertEquals(List.of("7:b"), InsertUpdateRun.table(db));
    assertEquals(counters(2, 1, 1, 0, 2, 0), r.counters());
    assertRecovered(r);
  }

  /** "Don't perform any updates": a key that waits in the INSERT batch is skipped the next time. */
  @Test
  void updatesBypassed_aKeyThatComesTwiceIsSkipped() throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourH2Test.database("H2");
    InsertUpdateRun.createTarget(db, 1L);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .configure(meta -> meta.setUpdateBypassed(true))
            .run(List.of(row(2, "first"), row(1, "r1"), row(2, "second")));

    assertEquals(ids(2, 1, 2), r.outputKeys());
    assertEquals(List.of("1:x", "2:first"), InsertUpdateRun.table(db));
    assertEquals(counters(3, 1, 0, 2, 3, 0), r.counters());
    assertRecovered(r);
  }

  /**
   * "Don't perform any updates": there is no UPDATE batch, the found rows are skipped and wait in
   * their place behind the inserts before them; with commit size 3 rows 2-4 and 6-8 make a window.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void updatesBypassed_skippedRowsWaitInTheirPlace(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        InsertUpdateRun.on(db)
            .driver(setup.driver())
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .configure(meta -> meta.setUpdateBypassed(true))
            .run(rows(10, 6));

    assertEquals(idsWithout(10, 6), r.outputKeys());
    assertEquals(ids(6), r.errorKeys());
    assertEquals(List.of("ISU002"), r.errorCodes());
    assertEquals(tableWith(10, id -> id % 2 == 0 && id != 6), InsertUpdateRun.table(db));
    assertEquals(counters(10, 4, 0, 5, 9, 1), r.counters());
    assertEquals(List.of(0, 1, 1, 1, 4, 5, 5, 5, 7, 8, 8), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1), r.errorSizeAtRead);
    assertRecovered(r);
  }

  /** id (Integer), code (String), name (String). */
  static IRowMeta idCodeNameRowMeta() {
    IRowMeta rowMeta = InsertUpdateRun.idNameRowMeta();
    rowMeta.addValueMeta(1, new ValueMetaString("code"));
    return rowMeta;
  }

  /**
   * The INSERT leaves key column code to its default, 'k', so the transform can't tell the key of
   * an inserted row: every later row waits for the batches to be written. Here row 2 is looked up
   * by (1, 'k') and finds the row that row 1, looked up by (1, 'zz'), inserted.
   */
  @Test
  void anInsertThatLeavesAKeyColumnToTheDatabaseWritesTheBatchesBeforeEveryLookup()
      throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourH2Test.database("H2");
    InsertUpdateRun.execute(
        db,
        "CREATE TABLE target (id BIGINT NOT NULL PRIMARY KEY, code VARCHAR(5) DEFAULT 'k',"
            + " name VARCHAR(20))");

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .configure(
                meta ->
                    meta.getInsertUpdateLookupField()
                        .getLookupKeys()
                        .add(new InsertUpdateKeyField("code", "code", "=")))
            .run(
                idCodeNameRowMeta(),
                List.of(new Object[] {1L, "zz", "a"}, new Object[] {1L, "k", "b"}));

    assertEquals(ids(1, 1), r.outputKeys());
    assertEquals(ids(), r.errorKeys());
    assertEquals(List.of("1:k:b"), InsertUpdateRun.query(db, "SELECT id, code, name FROM target"));
    assertEquals(counters(2, 1, 1, 0, 2, 0), r.counters());
    assertEquals(List.of(0, 0, 1), r.outputSizeAtRead);
    assertRecovered(r);
  }

  /** old_id (Integer), new_id (Integer), name (String). */
  static IRowMeta renameRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("old_id"));
    rowMeta.addValueMeta(new ValueMetaInteger("new_id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  /**
   * An update that changes the key column registers the key the row gets, too: row 2 looks up id
   * 11, which row 1 is about to give the row it updates, so the batches are written first and row 2
   * updates that row instead of inserting a second id 11.
   */
  @Test
  void anUpdateThatChangesTheKeyWritesTheBatchesForTheNewKey() throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourH2Test.database("H2");
    InsertUpdateRun.createTarget(db, 1L);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .configure(
                meta -> {
                  InsertUpdateLookupField lookup = meta.getInsertUpdateLookupField();
                  lookup.getLookupKeys().clear();
                  lookup.getLookupKeys().add(new InsertUpdateKeyField("old_id", "id", "="));
                  lookup.getValueFields().clear();
                  lookup.getValueFields().add(new InsertUpdateValue("id", "new_id", true));
                  lookup.getValueFields().add(new InsertUpdateValue("name", "name", true));
                })
            .run(
                renameRowMeta(),
                List.of(new Object[] {1L, 11L, "a"}, new Object[] {11L, 11L, "b"}));

    assertEquals(ids(), r.errorKeys());
    assertEquals(List.of("11:b"), InsertUpdateRun.table(db));
    assertEquals(counters(2, 0, 2, 0, 2, 0), r.counters());
    assertRecovered(r);
  }

  // ----------------------------------------------------------------------------------------------
  // Key conditions
  // ----------------------------------------------------------------------------------------------

  /**
   * A key condition that is not an equality could match a row waiting in a batch for any later
   * lookup, so the transform writes row by row, as without the option, and says why.
   */
  @ParameterizedTest(name = "extra key condition {0}")
  @ValueSource(strings = {">=", "<=", "BETWEEN", "<>"})
  void aKeyConditionThatIsNotAnEqualityKeepsTheRowsOneByOne(String condition) throws Exception {
    DatabaseMeta db = target("H2", null, 10);
    boolean onName = "<>".equals(condition);
    String stream2 = "BETWEEN".equals(condition) ? "id" : null;

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .configure(
                meta ->
                    meta.getInsertUpdateLookupField()
                        .getLookupKeys()
                        .add(
                            new InsertUpdateKeyField(
                                onName ? "name" : "id",
                                onName ? "name" : "id",
                                condition,
                                stream2)))
            .run(rows(10, 3, 6));

    assertFalse(r.recoveredBatches);
    assertTrue(r.logged(ONE_BY_ONE));
    assertTrue(r.logged("not every key condition is an equality"));
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(List.of("ISU001", "ISU001"), r.errorCodes());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
  }

  /** = ~NULL, IS NULL and IS NOT NULL compare equal or no value: the rows are batched. */
  @ParameterizedTest(name = "key condition {0}")
  @ValueSource(strings = {"= ~NULL", "IS NOT NULL"})
  void equalityKeyConditionsKeepBatching(String condition) throws Exception {
    DatabaseMeta db = target("H2", null, 10);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .watchCommits()
            .configure(
                meta -> {
                  List<InsertUpdateKeyField> keys =
                      meta.getInsertUpdateLookupField().getLookupKeys();
                  if ("= ~NULL".equals(condition)) {
                    keys.set(0, new InsertUpdateKeyField("id", "id", condition));
                  } else {
                    keys.add(new InsertUpdateKeyField("", "name", condition));
                  }
                })
            .run(rows(10, 3, 6));

    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(List.of("ISU002", "ISU002"), r.errorCodes());
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertEquals(countersFor(10, 3, 6), r.counters());
    assertEquals(List.of(0, 0, 0, 0, 3, 3, 3, 3, 6, 6, 6), r.outputSizeAtRead);
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  // ----------------------------------------------------------------------------------------------
  // Other ways the database refuses a row
  // ----------------------------------------------------------------------------------------------

  /**
   * A unique column outside the key, a NOT NULL column and a value that is too long: the database
   * refuses those inserts and updates, and only they go to the error hop.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void rowsRefusedForOtherReasons(Setup setup) throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourH2Test.database("H2");
    if (setup.contract() != null) {
      db.setBatchFailureContractOverride(setup.contract());
    }
    InsertUpdateRun.execute(
        db,
        "CREATE TABLE target (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(5) NOT NULL UNIQUE)",
        "INSERT INTO target VALUES (1, 's1'), (3, 's3'), (5, 's5')");

    Result r =
        recovered(db, setup)
            .commitSize(100)
            .run(
                List.of(
                    row(1, "a"), // update
                    row(2, "b"), // insert
                    row(3, "a"), // update refused: name a is row 1's
                    row(4, null), // insert refused: name can't be null
                    row(5, "c"), // update
                    row(6, "toolong"), // insert refused: too long
                    row(7, "b"), // insert refused: name b is row 2's
                    row(8, "d"))); // insert

    assertEquals(ids(1, 2, 5, 8), r.outputKeys());
    assertEquals(ids(3, 4, 6, 7), r.errorKeys());
    assertEquals(Collections.nCopies(4, "ISU002"), r.errorCodes());
    assertEquals(List.of("1:a", "2:b", "3:s3", "5:c", "8:d"), InsertUpdateRun.table(db));
    assertEquals(counters(8, 2, 2, 0, 4, 4), r.counters());
    assertRecovered(r);
  }

  /**
   * The inserts of a window are written before its updates. So when an update and a later insert of
   * the same window want the same value of a unique column outside the key, the insert gets it and
   * the update is refused, where row by row the update, which came first, gets it.
   */
  @Test
  void aUniqueColumnOutsideTheKey_theInsertsOfAWindowAreWrittenFirst() throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourH2Test.database("H2");
    InsertUpdateRun.execute(
        db,
        "CREATE TABLE target (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(5) NOT NULL UNIQUE)",
        "INSERT INTO target VALUES (1, 's1')");

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .run(List.of(row(1, "z"), row(2, "z")));

    assertEquals(ids(2), r.outputKeys());
    assertEquals(ids(1), r.errorKeys());
    assertEquals(List.of("1:s1", "2:z"), InsertUpdateRun.table(db));
    assertRecovered(r);
  }

  // ----------------------------------------------------------------------------------------------
  // Without an error hop
  // ----------------------------------------------------------------------------------------------

  /**
   * A failed INSERT batch is rolled back and stops the transform; the UPDATE batch of the window is
   * dropped without running. None of the window's rows is passed on.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void noErrorHop_aFailedInsertBatchIsRolledBackAndStops(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        InsertUpdateRun.on(db)
            .driver(setup.driver())
            .batch(true)
            .errorHop(false)
            .commitSize(5)
            .watchCommits()
            .run(rows(10, 4));

    assertEquals(ids(), r.outputKeys());
    assertEquals(tableWith(10, id -> false), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(5, 0, 0, 0, 0, 0), r.counters());
    assertTrue(
        r.errorLog.stream()
            .anyMatch(line -> line.contains("Error inserting a batch of rows into table")));
    assertTrue(r.recoveredBatches);
  }

  /**
   * A failed UPDATE batch is rolled back and stops the transform. The inserts of the window were
   * committed before it ran: they, and only they, are passed on.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void noErrorHop_aFailedUpdateBatchPassesOnTheCommittedInserts(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        InsertUpdateRun.on(db)
            .driver(setup.driver())
            .batch(true)
            .errorHop(false)
            .commitSize(5)
            .watchCommits()
            .run(rows(10, 3));

    assertEquals(ids(2, 4), r.outputKeys());
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(tableWith(10, id -> id == 2 || id == 4), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(counters(5, 2, 0, 0, 2, 0), r.counters());
    assertTrue(
        r.errorLog.stream()
            .anyMatch(line -> line.contains("Error updating a batch of rows in table")));
  }

  /** The windows committed before a failed one were passed on and stay. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void noErrorHop_theWindowsBeforeAFailedOneStay(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        InsertUpdateRun.on(db)
            .driver(setup.driver())
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

  // ----------------------------------------------------------------------------------------------
  // batchComplete(), dispose(), a stopped or failed transform
  // ----------------------------------------------------------------------------------------------

  /** At the end of a single-threaded iteration the batches run, are committed and passed on. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void batchCompleteWritesThePendingBatches(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 5);
    List<List<String>> tableAtBatchComplete = new ArrayList<>();

    Result r =
        recovered(db, setup)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    tableAtBatchComplete.add(InsertUpdateRun.table(db));
                  }
                })
            .run(rows(5, 2, 5));

    assertEquals(List.of(tableWith(5, id -> id == 1 || id == 3)), tableAtBatchComplete);
    assertEquals(List.of(0, 0, 0, 2, 2, 2), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 1, 1, 1), r.errorSizeAtRead);
    assertEquals(ids(1, 3, 4), r.outputKeys());
    assertEquals(ids(2, 5), r.errorKeys());
    assertEquals(counters(5, 1, 2, 0, 3, 2), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  /** Without an error hop a failed batch at the end of an iteration is rolled back and thrown. */
  @Test
  void batchComplete_noErrorHop_aFailedBatchIsRolledBackAndThrown() throws Exception {
    DatabaseMeta db = target("H2", null, 5);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                  }
                })
            .run(rows(5, 2));

    assertInstanceOf(HopDatabaseBatchException.class, r.thrown);
    assertEquals(ids(), r.outputKeys());
    assertEquals(tableWith(5, id -> false), InsertUpdateRun.table(db));
    assertEquals(1, r.errorCount);
  }

  /**
   * A transform that is stopped drops the rows still waiting in its batches, so the table holds
   * exactly the rows that were passed on.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void aStoppedTransformDropsItsBatches(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        recovered(db, setup)
            .commitSize(5)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 7) {
                    transform.stopAll();
                  }
                })
            .run(rows(10));

    assertTrue(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(5), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 5), InsertUpdateRun.table(db));
    assertEquals(counters(7, 2, 3, 0, 5, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.startsWith("2 rows of the batches")));
    assertRecovered(r);
  }

  /** A transform that fails drops the rows of its batches: none of them is committed. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("setups")
  void aFailedTransformDropsItsBatches(Setup setup) throws Exception {
    DatabaseMeta db = target(setup, 10);

    Result r =
        recovered(db, setup)
            .commitSize(5)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 7) {
                    transform.setErrors(1);
                    transform.stopAll();
                  }
                })
            .run(rows(10));

    assertEquals(idsWithout(5), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 5), InsertUpdateRun.table(db));
    assertEquals(counters(7, 2, 3, 0, 5, 0), r.counters());
    assertTrue(r.errorLog.stream().anyMatch(line -> line.startsWith("2 rows of the batches")));
    assertRecovered(r);
  }

  // ----------------------------------------------------------------------------------------------
  // Where the rows are still written one by one
  // ----------------------------------------------------------------------------------------------

  /**
   * A dialect that runs every row behind a savepoint (as PostgreSQL's look-alikes do) batches with
   * an error hop once its contract is set on the connection.
   */
  @Test
  void theConnectionOptionRecoversBatchesOnAnyDialect() throws Exception {
    DatabaseMeta db = target("H2_INSUPD_SAVEPOINTS", "LOSES_WHOLE_BATCH", 10);

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

  /**
   * Row by row, exactly as without the option, and logged: every row is passed on right after its
   * statement, and a refused row goes to the error hop with ISU001.
   */
  private static void assertRowByRow(Result r, DatabaseMeta db, boolean savepoints)
      throws Exception {
    assertFalse(r.recoveredBatches);
    assertEquals(savepoints, r.useSavepoints);
    assertEquals(idsWithout(10, 3, 6), r.outputKeys());
    assertEquals(ids(3, 6), r.errorKeys());
    assertEquals(List.of("ISU001", "ISU001"), r.errorCodes());
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 4, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(tableWrittenExcept(10, 3, 6), InsertUpdateRun.table(db));
    assertEquals(countersFor(10, 3, 6), r.counters());
  }

  @Test
  void theLegacyVariableKeepsTheRowsOneByOne() throws Exception {
    DatabaseMeta db = target("H2", null, 10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(rows(10, 3, 6));

    assertRowByRow(r, db, false);
    assertTrue(r.logged(ONE_BY_ONE));
    assertTrue(r.logged("UNKNOWN (from VARIABLE)"));
  }

  /** The legacy variable on a savepoint dialect: a savepoint per row, as before. */
  @Test
  void theLegacyVariableKeepsASavepointPerRow() throws Exception {
    DatabaseMeta db = target("H2_INSUPD_SAVEPOINTS", "LOSES_WHOLE_BATCH", 10);

    Result r =
        InsertUpdateRun.on(db)
            .recovery("LEGACY")
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(rows(10, 3, 6));

    assertRowByRow(r, db, true);
    assertTrue(r.logged(ONE_BY_ONE));
  }

  @Test
  void unknownOnTheConnectionKeepsTheRowsOneByOne() throws Exception {
    DatabaseMeta db = target("H2", "UNKNOWN", 10);

    Result r = InsertUpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 3, 6));

    assertRowByRow(r, db, false);
    assertTrue(r.logged("UNKNOWN (from CONNECTION)"));
  }

  /** A dialect that extends H2 declares no contract: it has not been measured. */
  @Test
  void anUnvalidatedDialectKeepsTheRowsOneByOne() throws Exception {
    DatabaseMeta db = target("H2_INSUPD_UNVALIDATED", null, 10);

    Result r = InsertUpdateRun.on(db).batch(true).errorHop(true).commitSize(4).run(rows(10, 3, 6));

    assertRowByRow(r, db, false);
    assertTrue(r.logged("UNKNOWN (from NOT_DECLARED)"));
  }

  /** Commit size 0 is auto-commit: the rows are written one by one, each committed on its own. */
  @Test
  void commitSizeZeroKeepsTheRowsOneByOne() throws Exception {
    DatabaseMeta db = target("H2", null, 10);

    Result r =
        InsertUpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(0)
            .watchCommits()
            .run(rows(10, 3, 6));

    assertRowByRow(r, db, false);
    assertTrue(r.logged("the commit size is 0"));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
  }

  /** With the option off nothing changes, and nothing is logged about batches. */
  @Test
  void theOptionOffKeepsTheRowsOneByOne() throws Exception {
    DatabaseMeta db = target("H2", null, 10);

    Result r = InsertUpdateRun.on(db).batch(false).errorHop(true).commitSize(4).run(rows(10, 3, 6));

    assertRowByRow(r, db, false);
    assertFalse(r.logged(ONE_BY_ONE));
  }
}

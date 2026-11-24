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

import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.BATCH;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.DRIVER;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.ERROR_CODES;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.ERROR_DESCRIPTIONS;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.ERROR_FIELDS;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.NOT_SENT;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.NR_ERRORS;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.ROW;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idNameRowMeta;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idNameRows;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.ids;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idsWithout;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.tableWithSeeds;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
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
 * Pins what Table output does today, end to end against H2, when rows fail: with and without batch
 * updates, with and without an error hop, for several commit sizes, with the table name in a field
 * or date partitioning, with generated keys, and on a dialect that uses savepoints.
 *
 * <p>Each test checks the rows on the main output (values and order), the rows on the error hop
 * (values, error codes, error fields, the kind of message and the key it names), the rows in the
 * table after the run, the counters, and for the important cases when the rows left the transform.
 *
 * <p>These are characterization tests: they describe current behaviour, including behaviour that is
 * arguably wrong. Those cases say so in their comments. A failed row is a primary key that is
 * already in the table ("seeded").
 *
 * <p>H2, like SQL Server and MySQL, runs the whole batch and marks the failed rows with {@code
 * EXECUTE_FAILED} in the update counts, chaining one exception per failed row.
 */
class TableOutputBatchBehaviourH2Test {

  private static final String CREATE_TARGET =
      "CREATE TABLE target (id BIGINT PRIMARY KEY, name VARCHAR(20))";
  private static final String SELECT_TARGET = "SELECT id, name FROM target ORDER BY id";

  /** H2 that asks for savepoints, the way PostgreSQL does. */
  @DatabaseMetaPlugin(type = "H2SAVEPOINTS", typeDescription = "H2 with savepoints")
  public static class H2SavepointsDatabaseMeta extends H2DatabaseMeta {
    @Override
    public boolean isUseSafePoints() {
      return true;
    }
  }

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2SavepointsDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  private static DatabaseMeta database(String type, String... statements) throws Exception {
    String name =
        "mem:tableoutput" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta = new DatabaseMeta("h2", type, "Native", "", name, "", "sa", "");
    TableOutputRun.execute(databaseMeta, statements);
    return databaseMeta;
  }

  /** An H2 database with the target table, holding the seeded ids with the name "seed". */
  private static DatabaseMeta target(String type, long... seeded) throws Exception {
    DatabaseMeta databaseMeta = database(type, CREATE_TARGET);
    for (long id : seeded) {
      TableOutputRun.execute(databaseMeta, "INSERT INTO target VALUES (" + id + ", 'seed')");
    }
    return databaseMeta;
  }

  private static DatabaseMeta target(long... seeded) throws Exception {
    return target("H2", seeded);
  }

  private static List<String> table(DatabaseMeta databaseMeta) throws Exception {
    return TableOutputRun.query(databaseMeta, SELECT_TARGET);
  }

  private static List<Integer> list(Integer... values) {
    return Arrays.asList(values);
  }

  private static List<String> repeat(String value, int times) {
    return Collections.nCopies(times, value);
  }

  private static void assertCounters(
      Result r, long linesOutput, long linesWritten, long linesRejected, long errors) {
    assertEquals(
        List.of(linesOutput, linesWritten, linesRejected, errors),
        List.of(r.linesOutput, r.linesWritten, r.linesRejected, r.errorCount),
        "lines output, lines written, lines rejected, errors");
  }

  /** The error row is the input row followed by the four error fields of the error hop. */
  private static void assertErrorRowLayout(Result r, String... inputFields) {
    List<String> expected = new ArrayList<>(Arrays.asList(inputFields));
    expected.addAll(List.of(NR_ERRORS, ERROR_DESCRIPTIONS, ERROR_FIELDS, ERROR_CODES));
    assertArrayEquals(expected.toArray(new String[0]), r.errorRowMeta.getFieldNames());
    for (Object nrErrors : r.errorField(NR_ERRORS)) {
      assertEquals(1L, nrErrors);
    }
    for (Object fields : r.errorField(ERROR_FIELDS)) {
      assertNull(fields, "Table output never names the failing field");
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Batch updates off
  // ----------------------------------------------------------------------------------------------

  /** Without batching every row is written and passed on, or sent to the error hop, on its own. */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {100, 3, 1})
  void batchOff_errorHop_eachFailedRowGoesToTheErrorHopAtOnce(int commitSize) throws Exception {
    DatabaseMeta db = target(3, 7);

    Result r =
        TableOutputRun.on(db)
            .batch(false)
            .errorHop(true)
            .commitSize(commitSize)
            .run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.initialized);
    assertFalse(r.batchMode);
    assertFalse(r.useSafePoints);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals("r1", r.output.get(0)[1]);
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("TOP001", "TOP001"), r.errorCodes());
    assertEquals(List.of(ROW, ROW), r.errorKinds());
    assertEquals(List.of(3L, 7L), r.errorMessageKeys());
    assertErrorRowLayout(r, "id", "name");
    assertEquals("r3", r.errors.get(0)[1], "the error row carries the input values");
    assertEquals(tableWithSeeds(10, 3, 7), table(db));
    assertCounters(r, 8, 8, 2, 0);
    assertEquals(list(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(list(0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2), r.errorSizeAtRead);
  }

  /**
   * Without an error hop the failed row stops the transform and rolls back what was not committed.
   * The rows before it in the same commit window were already passed on: they are on the main
   * output and counted, but not in the table.
   */
  @Test
  void batchOff_noErrorHop_rowsAlreadyPassedOnAreRolledBack() throws Exception {
    DatabaseMeta db = target(3);

    Result r =
        TableOutputRun.on(db).batch(false).commitSize(100).run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.stopped);
    assertEquals(ids(1, 2), r.outputKeys());
    assertEquals(List.of("3:seed"), table(db), "rows 1 and 2 were rolled back");
    assertCounters(r, 2, 2, 0, 1);
    assertEquals(list(0, 1, 2), r.outputSizeAtRead, "rows 4..10 are never read");
  }

  /** Commits that happened before the failure stay. */
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

  // ----------------------------------------------------------------------------------------------
  // Batch updates on, with an error hop
  // ----------------------------------------------------------------------------------------------

  static Stream<Arguments> failuresInABatchOfFive() {
    return Stream.of(
        Arguments.of(
            "first row of a batch",
            new long[] {1},
            list(0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 9),
            list(0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1)),
        Arguments.of(
            "middle row of a batch",
            new long[] {3},
            list(0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 9),
            list(0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1)),
        Arguments.of(
            "last row of a batch",
            new long[] {5},
            list(0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 9),
            list(0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1)),
        Arguments.of(
            "several rows of one batch",
            new long[] {2, 4},
            list(0, 0, 0, 0, 0, 3, 3, 3, 3, 3, 8),
            list(0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2)),
        Arguments.of(
            "a whole batch",
            new long[] {1, 2, 3, 4, 5},
            list(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5),
            list(0, 0, 0, 0, 0, 5, 5, 5, 5, 5, 5)),
        Arguments.of(
            "rows in two batches",
            new long[] {3, 8},
            list(0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 8),
            list(0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2)));
  }

  /**
   * Ten rows, commit size 5. A failed batch is committed as far as it got, its good rows go to the
   * main output and its failed rows to the error hop with TOP0002 and the driver's own message for
   * that row. The rows leave a batch at a time: the first batch when row 6 is read, the second at
   * the end of the input.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("failuresInABatchOfFive")
  void batchOn_errorHop_onlyTheFailedRowsAreRejected(
      String description,
      long[] seeded,
      List<Integer> outputSizeAtRead,
      List<Integer> errorSizeAtRead)
      throws Exception {
    DatabaseMeta db = target(seeded);

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.batchMode);
    assertFalse(r.useSafePoints);
    assertEquals(idsWithout(10, seeded), r.outputKeys());
    assertEquals(ids(seeded), r.errorKeys());
    assertEquals(repeat("TOP0002", seeded.length), r.errorCodes());
    assertEquals(repeat(DRIVER, seeded.length), r.errorKinds());
    List<Long> keys = new ArrayList<>();
    for (long id : seeded) {
      keys.add(id);
    }
    assertEquals(keys, r.errorMessageKeys(), "each rejected row gets its own message");
    assertErrorRowLayout(r, "id", "name");
    assertEquals(tableWithSeeds(10, seeded), table(db));
    assertCounters(r, 10 - seeded.length, 10 - seeded.length, seeded.length, 0);
    assertEquals(outputSizeAtRead, r.outputSizeAtRead);
    assertEquals(errorSizeAtRead, r.errorSizeAtRead);
  }

  /** A key that is repeated within one batch: the second occurrence is the one rejected. */
  @Test
  void batchOn_errorHop_duplicateWithinTheBatch() throws Exception {
    DatabaseMeta db = target();
    List<Object[]> rows = idNameRows(5);
    rows.set(3, new Object[] {2L, "dup"});

    Result r =
        TableOutputRun.on(db).batch(true).errorHop(true).commitSize(5).run(idNameRowMeta(), rows);

    assertEquals(ids(1, 2, 3, 5), r.outputKeys());
    assertEquals(ids(2), r.errorKeys());
    assertEquals("dup", r.errors.get(0)[1]);
    assertEquals(List.of("TOP0002"), r.errorCodes());
    assertEquals(List.of(DRIVER), r.errorKinds());
    assertEquals(List.of("1:r1", "2:r2", "3:r3", "5:r5"), table(db));
    assertCounters(r, 4, 4, 1, 0);
  }

  // ----------------------------------------------------------------------------------------------
  // Commit sizes
  // ----------------------------------------------------------------------------------------------

  @Test
  void batchOn_commitSizeOne_everyRowIsItsOwnBatch() throws Exception {
    DatabaseMeta db = target(3);

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(1)
            .run(idNameRowMeta(), idNameRows(5));

    assertTrue(r.batchMode);
    assertEquals(ids(1, 2, 4, 5), r.outputKeys());
    assertEquals(ids(3), r.errorKeys());
    assertEquals(List.of("TOP0002"), r.errorCodes());
    assertEquals(List.of(DRIVER), r.errorKinds());
    assertEquals(List.of(3L), r.errorMessageKeys());
    assertEquals(tableWithSeeds(5, 3), table(db));
    assertCounters(r, 4, 4, 1, 0);
    assertEquals(list(0, 1, 2, 2, 3, 4), r.outputSizeAtRead);
    assertEquals(list(0, 0, 0, 1, 1, 1), r.errorSizeAtRead);
  }

  @Test
  void batchOn_commitSizeTwo() throws Exception {
    DatabaseMeta db = target(3);

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(2)
            .run(idNameRowMeta(), idNameRows(5));

    assertEquals(ids(1, 2, 4, 5), r.outputKeys());
    assertEquals(ids(3), r.errorKeys());
    assertEquals(List.of("TOP0002"), r.errorCodes());
    assertEquals(tableWithSeeds(5, 3), table(db));
    assertCounters(r, 4, 4, 1, 0);
    // [1,2] leave at the commit after row 2, [3,4] when that batch fails at row 4, 5 at the end
    assertEquals(list(0, 0, 2, 2, 3, 3), r.outputSizeAtRead);
    assertEquals(list(0, 0, 0, 0, 1, 1), r.errorSizeAtRead);
  }

  /** A commit size larger than the input: everything is written and passed on at the end. */
  @Test
  void batchOn_commitSizeLargerThanTheInput_allRowsLeaveAtTheEnd() throws Exception {
    DatabaseMeta db = target(3, 7);

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.batchMode);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("TOP0002", "TOP0002"), r.errorCodes());
    assertEquals(List.of(DRIVER, DRIVER), r.errorKinds());
    assertEquals(List.of(3L, 7L), r.errorMessageKeys());
    assertEquals(tableWithSeeds(10, 3, 7), table(db));
    assertCounters(r, 8, 8, 2, 0);
    assertEquals(Collections.nCopies(11, 0), r.outputSizeAtRead);
    assertEquals(Collections.nCopies(11, 0), r.errorSizeAtRead);
  }

  /** 10 rows with commit size 4: the last two are a partial batch, flushed at the end of input. */
  @Test
  void batchOn_lastPartialBatchIsFlushedAtTheEnd() throws Exception {
    DatabaseMeta db = target();

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(idNameRowMeta(), idNameRows(10));

    assertEquals(idsWithout(10), r.outputKeys());
    assertTrue(r.errors.isEmpty());
    assertEquals(tableWithSeeds(10), table(db));
    assertCounters(r, 10, 10, 0, 0);
    assertEquals(list(0, 0, 0, 0, 4, 4, 4, 4, 8, 8, 8), r.outputSizeAtRead);
  }

  /** A failure in the last partial batch gets the same recovery as any other batch. */
  @Test
  void batchOn_failureInTheLastPartialBatch() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .run(idNameRowMeta(), idNameRows(10));

    assertEquals(idsWithout(10, 10), r.outputKeys());
    assertEquals(ids(10), r.errorKeys());
    assertEquals(List.of("TOP0002"), r.errorCodes());
    assertEquals(List.of(DRIVER), r.errorKinds());
    assertEquals(List.of(10L), r.errorMessageKeys());
    assertEquals(tableWithSeeds(10, 10), table(db), "row 9 of the failed partial batch is kept");
    assertCounters(r, 9, 9, 1, 0);
    assertEquals(list(0, 0, 0, 0, 4, 4, 4, 4, 8, 8, 8), r.outputSizeAtRead);
    assertEquals(Collections.nCopies(11, 0), r.errorSizeAtRead);
  }

  /** Commit size 0 means one commit at the end, and turns batch mode off. */
  @Test
  void batchOn_commitSizeZero_turnsBatchModeOff() throws Exception {
    DatabaseMeta db = target(3);

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(0)
            .run(idNameRowMeta(), idNameRows(5));

    assertFalse(r.batchMode);
    assertEquals(ids(1, 2, 4, 5), r.outputKeys());
    assertEquals(ids(3), r.errorKeys());
    assertEquals(List.of("TOP001"), r.errorCodes());
    assertEquals(List.of(ROW), r.errorKinds());
    assertEquals(tableWithSeeds(5, 3), table(db));
    assertCounters(r, 4, 4, 1, 0);
    assertEquals(list(0, 1, 2, 2, 3, 4), r.outputSizeAtRead);
  }

  // ----------------------------------------------------------------------------------------------
  // Batch updates on, without an error hop
  // ----------------------------------------------------------------------------------------------

  /**
   * A failed batch rolls back the whole batch and stops the transform; earlier batches stay
   * committed.
   *
   * <p>Surprising: on dispose the rows of the failed batch still in the buffer (6, 7, 8, 9: the row
   * that ran the batch, 10, never got there) are "passed on" to a stopped transform. putRow drops
   * them, but lines output still counts them, failed row 7 included: 9 lines output for 5 rows in
   * the table.
   */
  @Test
  void batchOn_noErrorHop_failedBatchIsRolledBack() throws Exception {
    DatabaseMeta db = target(7);

    Result r = TableOutputRun.on(db).batch(true).commitSize(5).run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.batchMode);
    assertTrue(r.stopped);
    assertEquals(ids(1, 2, 3, 4, 5), r.outputKeys());
    assertEquals(List.of("1:r1", "2:r2", "3:r3", "4:r4", "5:r5", "7:seed"), table(db));
    assertCounters(r, 9, 5, 0, 1);
    assertEquals(list(0, 0, 0, 0, 0, 5, 5, 5, 5, 5), r.outputSizeAtRead);
  }

  /** A failure in the very first batch: nothing is written, yet 4 lines output are counted. */
  @Test
  void batchOn_noErrorHop_firstBatchFails() throws Exception {
    DatabaseMeta db = target(3);

    Result r = TableOutputRun.on(db).batch(true).commitSize(5).run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.stopped);
    assertTrue(r.output.isEmpty());
    assertEquals(List.of("3:seed"), table(db));
    assertCounters(r, 4, 0, 0, 1);
  }

  /**
   * The last partial batch fails at the end of the input: it is rolled back and the transform
   * fails. Its two rows are counted as lines output (10) although only 8 rows are in the table.
   */
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
    assertEquals(list(0, 0, 0, 0, 4, 4, 4, 4, 8, 8, 8), r.outputSizeAtRead);
  }

  // ----------------------------------------------------------------------------------------------
  // Ignore insert errors
  // ----------------------------------------------------------------------------------------------

  /**
   * "Ignore insert errors" without batching: the failed row is logged and then passed on to the
   * main output as if it had been written. Lines output counts it.
   */
  @Test
  void ignoreErrors_batchOff_failedRowIsPassedOnAsIfWritten() throws Exception {
    DatabaseMeta db = target(3);

    Result r =
        TableOutputRun.on(db)
            .batch(false)
            .commitSize(5)
            .configure(meta -> meta.setIgnoreErrors(true))
            .run(idNameRowMeta(), idNameRows(10));

    assertFalse(r.stopped);
    assertEquals(idsWithout(10), r.outputKeys(), "row 3 is on the main output");
    assertEquals(tableWithSeeds(10, 3), table(db));
    assertCounters(r, 10, 10, 0, 0);
  }

  /**
   * "Ignore insert errors" with batching has no effect: a failed batch stops the transform exactly
   * as it does without the option.
   */
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

  /** With an error hop as well, the error hop wins. */
  @Test
  void ignoreErrors_withErrorHop_theErrorHopWins() throws Exception {
    DatabaseMeta db = target(3);

    Result r =
        TableOutputRun.on(db)
            .batch(false)
            .errorHop(true)
            .commitSize(5)
            .configure(meta -> meta.setIgnoreErrors(true))
            .run(idNameRowMeta(), idNameRows(10));

    assertEquals(idsWithout(10, 3), r.outputKeys());
    assertEquals(ids(3), r.errorKeys());
    assertEquals(List.of("TOP001"), r.errorCodes());
    assertEquals(tableWithSeeds(10, 3), table(db));
    assertCounters(r, 9, 9, 1, 0);
  }

  // ----------------------------------------------------------------------------------------------
  // Several tables in one buffer: table name in a field, date partitioning
  // ----------------------------------------------------------------------------------------------

  private static final String SELECT_TA_TB =
      "SELECT 'ta', id, name FROM ta UNION ALL SELECT 'tb', id, name FROM tb ORDER BY 1, 2";

  /** Odd ids go to table ta, even ids to tb, named in field "tbl" that is not inserted. */
  private static Result runTableNameInField(DatabaseMeta db) throws Exception {
    IRowMeta rowMeta = idNameRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("tbl"));
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 10; id++) {
      rows.add(new Object[] {id, "r" + id, id % 2 == 1 ? "ta" : "tb"});
    }
    return TableOutputRun.on(db)
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

  private static DatabaseMeta twoTables(long... seeded) throws Exception {
    DatabaseMeta db =
        database(
            "H2",
            "CREATE TABLE ta (id BIGINT PRIMARY KEY, name VARCHAR(20))",
            "CREATE TABLE tb (id BIGINT PRIMARY KEY, name VARCHAR(20))");
    for (long id : seeded) {
      TableOutputRun.execute(
          db, "INSERT INTO " + (id % 2 == 1 ? "ta" : "tb") + " VALUES (" + id + ", 'seed')");
    }
    return db;
  }

  /** ta:1,3,5,7,9 and tb:2,4,...,10, with the seeded ones reading "seed". */
  private static List<String> twoTablesWithSeeds(String a, String b, long... seeded) {
    List<String> lines = new ArrayList<>();
    for (String t : new String[] {a, b}) {
      for (long id = t.equals(a) ? 1 : 2; id <= 10; id += 2) {
        boolean seed = false;
        for (long s : seeded) {
          seed |= s == id;
        }
        lines.add(t + ":" + id + ":" + (seed ? "seed" : "r" + id));
      }
    }
    return lines;
  }

  /**
   * Each table has its own commit counter, but the buffer is shared: when one table commits, the
   * whole buffer is passed on, including rows for the other table that are not even sent yet.
   */
  @Test
  void tableNameInField_noFailures() throws Exception {
    DatabaseMeta db = twoTables();

    Result r = runTableNameInField(db);

    assertEquals(idsWithout(10), r.outputKeys());
    assertTrue(r.errors.isEmpty());
    assertEquals(twoTablesWithSeeds("ta", "tb"), TableOutputRun.query(db, SELECT_TA_TB));
    assertCounters(r, 10, 10, 0, 0);
    // ta commits 1,3,5 on row 5 and the buffer 1..5 leaves; tb commits 2,4,6 on row 6
    assertEquals(list(0, 0, 0, 0, 0, 5, 6, 6, 6, 6, 6), r.outputSizeAtRead);
  }

  /**
   * Buggy: the batch of ta (1, 3, 5) fails at 3, but the update counts are matched against the
   * shared buffer 1..5. Row 2 (tb, fine) is rejected with row 3's message, row 3 (the one that
   * failed) goes to the main output, row 4 (tb, fine) is rejected as the row the driver "stopped
   * at", and row 5 (written) is rejected as "not sent". Rows 2 and 4 are still in tb's batch and
   * get written later: three rows on the error hop are in the table, and the failed one is on the
   * main output.
   */
  @Test
  void tableNameInField_failureIsAttributedToTheWrongRows() throws Exception {
    DatabaseMeta db = twoTables(3);

    Result r = runTableNameInField(db);

    assertFalse(r.stopped);
    assertEquals(ids(1, 3, 6, 7, 8, 9, 10), r.outputKeys());
    assertEquals(ids(2, 4, 5), r.errorKeys());
    assertEquals(List.of("TOP0002", "TOP0002", "TOP0003"), r.errorCodes());
    assertEquals(List.of(DRIVER, BATCH, NOT_SENT), r.errorKinds());
    assertEquals(Arrays.asList(3L, 3L, 3L), r.errorMessageKeys(), "all three name key 3");
    assertErrorRowLayout(r, "id", "name", "tbl");
    assertEquals(twoTablesWithSeeds("ta", "tb", 3), TableOutputRun.query(db, SELECT_TA_TB));
    assertCounters(r, 7, 7, 3, 0);
    assertEquals(list(0, 0, 0, 0, 0, 2, 3, 3, 3, 3, 3), r.outputSizeAtRead);
    assertEquals(list(0, 0, 0, 0, 0, 3, 3, 3, 3, 3, 3), r.errorSizeAtRead);
  }

  /**
   * Buggy: tb's batch (2, 4, 6) fails at 4 after ta's commit already passed rows 1..5 on (failed
   * row 4 included). The buffer then holds only row 6, fewer rows than update counts, so the
   * transform fails ("Unable to attribute batch errors to rows") although it has an error hop. Rows
   * 2 and 6 were committed before that; rows 7..10 are never read.
   */
  @Test
  void tableNameInField_failureInTheOtherTableStopsTheTransformDespiteTheErrorHop()
      throws Exception {
    DatabaseMeta db = twoTables(4);

    Result r = runTableNameInField(db);

    assertTrue(r.stopped);
    assertEquals(ids(1, 2, 3, 4, 5), r.outputKeys());
    assertTrue(r.errors.isEmpty());
    assertEquals(
        List.of("ta:1:r1", "ta:3:r3", "ta:5:r5", "tb:2:r2", "tb:4:seed", "tb:6:r6"),
        TableOutputRun.query(db, SELECT_TA_TB));
    assertCounters(r, 6, 5, 0, 1);
    assertEquals(list(0, 0, 0, 0, 0, 5), r.outputSizeAtRead);
  }

  /**
   * Buggy, at the end of the input: ta's last batch (7, 9) fails at 9 while the buffer holds 7..10.
   * Row 7 is passed on, row 8 (tb, written) is rejected with row 9's message, row 9 is rejected,
   * and row 10 (tb, written) is rejected as "not sent".
   */
  @Test
  void tableNameInField_failureInTheFinalFlush() throws Exception {
    DatabaseMeta db = twoTables(9);

    Result r = runTableNameInField(db);

    assertFalse(r.stopped);
    assertEquals(idsWithout(7), r.outputKeys());
    assertEquals(ids(8, 9, 10), r.errorKeys());
    assertEquals(List.of("TOP0002", "TOP0002", "TOP0003"), r.errorCodes());
    assertEquals(List.of(DRIVER, BATCH, NOT_SENT), r.errorKinds());
    assertEquals(Arrays.asList(9L, 9L, 9L), r.errorMessageKeys());
    assertEquals(twoTablesWithSeeds("ta", "tb", 9), TableOutputRun.query(db, SELECT_TA_TB));
    assertCounters(r, 7, 7, 3, 0);
    assertEquals(list(0, 0, 0, 0, 0, 5, 6, 6, 6, 6, 6), r.outputSizeAtRead);
    assertEquals(Collections.nCopies(11, 0), r.errorSizeAtRead);
  }

  private static final String SELECT_PARTITIONS =
      "SELECT 'ta', id, name FROM target_202601 UNION ALL "
          + "SELECT 'tb', id, name FROM target_202602 ORDER BY 1, 2";

  private static DatabaseMeta partitionTables(long... seeded) throws Exception {
    DatabaseMeta db =
        database(
            "H2",
            "CREATE TABLE target_202601 (id BIGINT PRIMARY KEY, name VARCHAR(20), dt TIMESTAMP)",
            "CREATE TABLE target_202602 (id BIGINT PRIMARY KEY, name VARCHAR(20), dt TIMESTAMP)");
    for (long id : seeded) {
      TableOutputRun.execute(
          db,
          "INSERT INTO "
              + (id % 2 == 1 ? "target_202601" : "target_202602")
              + " VALUES ("
              + id
              + ", 'seed', NULL)");
    }
    return db;
  }

  /** Odd ids in January 2026 (target_202601), even ids in February (target_202602). */
  private static Result runPartitioned(DatabaseMeta db) throws Exception {
    IRowMeta rowMeta = idNameRowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("dt"));
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 10; id++) {
      Calendar calendar = Calendar.getInstance();
      calendar.clear();
      calendar.set(2026, id % 2 == 1 ? Calendar.JANUARY : Calendar.FEBRUARY, 15);
      rows.add(new Object[] {id, "r" + id, calendar.getTime()});
    }
    return TableOutputRun.on(db)
        .batch(true)
        .errorHop(true)
        .commitSize(3)
        .configure(
            meta -> {
              meta.setPartitioningEnabled(true);
              meta.setPartitioningField("dt");
              meta.setPartitioningMonthly(true);
              meta.setPartitioningDaily(false);
            })
        .run(rowMeta, rows);
  }

  @Test
  void partitionedByMonth_noFailures() throws Exception {
    DatabaseMeta db = partitionTables();

    Result r = runPartitioned(db);

    assertEquals(idsWithout(10), r.outputKeys());
    assertTrue(r.errors.isEmpty());
    assertEquals(twoTablesWithSeeds("ta", "tb"), TableOutputRun.query(db, SELECT_PARTITIONS));
    assertCounters(r, 10, 10, 0, 0);
  }

  /** The same misattribution as with the table name in a field. */
  @Test
  void partitionedByMonth_failureIsAttributedToTheWrongRows() throws Exception {
    DatabaseMeta db = partitionTables(3);

    Result r = runPartitioned(db);

    assertEquals(ids(1, 3, 6, 7, 8, 9, 10), r.outputKeys());
    assertEquals(ids(2, 4, 5), r.errorKeys());
    assertEquals(List.of("TOP0002", "TOP0002", "TOP0003"), r.errorCodes());
    assertEquals(List.of(DRIVER, BATCH, NOT_SENT), r.errorKinds());
    assertEquals(Arrays.asList(3L, 3L, 3L), r.errorMessageKeys());
    assertEquals(twoTablesWithSeeds("ta", "tb", 3), TableOutputRun.query(db, SELECT_PARTITIONS));
    assertCounters(r, 7, 7, 3, 0);
  }

  // ----------------------------------------------------------------------------------------------
  // Generated keys
  // ----------------------------------------------------------------------------------------------

  /**
   * Returning generated keys turns batch mode off: rows go one by one, the key is appended to the
   * output row, and a failed row goes to the error hop with TOP001. On H2 the failed insert uses up
   * a key value (4).
   */
  @Test
  void returningGeneratedKeys_turnsBatchModeOff() throws Exception {
    DatabaseMeta db =
        database(
            "H2",
            "CREATE TABLE gen (gid BIGINT AUTO_INCREMENT PRIMARY KEY, id BIGINT UNIQUE, "
                + "name VARCHAR(20))",
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
    assertArrayEquals(new String[] {"id", "name", "gk"}, r.outputRowMeta.getFieldNames());
    List<String> output = new ArrayList<>();
    for (Object[] row : r.output) {
      output.add(row[0] + ":" + row[1] + ":" + row[2]);
    }
    assertEquals(List.of("1:r1:2", "2:r2:3", "4:r4:5", "5:r5:6", "6:r6:7"), output);
    assertEquals(ids(3), r.errorKeys());
    assertEquals(List.of("TOP001"), r.errorCodes());
    assertEquals(List.of(ROW), r.errorKinds());
    assertEquals(List.of(3L), r.errorMessageKeys());
    assertErrorRowLayout(r, "id", "name");
    assertEquals(
        List.of("1:3:seed", "2:1:r1", "3:2:r2", "5:4:r4", "6:5:r5", "7:6:r6"),
        TableOutputRun.query(db, "SELECT gid, id, name FROM gen ORDER BY gid"));
    assertCounters(r, 5, 5, 1, 0);
    assertEquals(list(0, 1, 2, 2, 3, 4, 5), r.outputSizeAtRead);
  }

  // ----------------------------------------------------------------------------------------------
  // A dialect that uses savepoints (PostgreSQL and look-alikes)
  // ----------------------------------------------------------------------------------------------

  /**
   * With an error hop, "use batch update" is switched off and every row runs behind a savepoint:
   * the rows are written and passed on one by one, and the failed ones go to the error hop with
   * TOP001.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {5, 1})
  void savepointDialect_errorHop_rowsAreWrittenOneByOne(int commitSize) throws Exception {
    DatabaseMeta db = target("H2SAVEPOINTS", 3, 7);

    Result r =
        TableOutputRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .run(idNameRowMeta(), idNameRows(10));

    assertFalse(r.batchMode);
    assertTrue(r.useSafePoints);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("TOP001", "TOP001"), r.errorCodes());
    assertEquals(List.of(ROW, ROW), r.errorKinds());
    assertEquals(List.of(3L, 7L), r.errorMessageKeys());
    assertErrorRowLayout(r, "id", "name");
    assertEquals(tableWithSeeds(10, 3, 7), table(db));
    assertCounters(r, 8, 8, 2, 0);
    assertEquals(list(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(list(0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2), r.errorSizeAtRead);
  }

  /** Without an error hop no savepoints are used and the dialect batches as usual. */
  @Test
  void savepointDialect_noErrorHop_keepsBatching() throws Exception {
    DatabaseMeta db = target("H2SAVEPOINTS", 3, 7);

    Result r = TableOutputRun.on(db).batch(true).commitSize(5).run(idNameRowMeta(), idNameRows(10));

    assertTrue(r.batchMode);
    assertFalse(r.useSafePoints);
    assertTrue(r.stopped);
    assertTrue(r.output.isEmpty());
    assertEquals(List.of("3:seed", "7:seed"), table(db));
    assertCounters(r, 4, 0, 0, 1);
  }
}

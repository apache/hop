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

package org.apache.hop.pipeline.transforms.mergeintotable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Merge into table end to end against H2: inserts, updates, insert only, update only changed rows,
 * NULLs, the same key twice in a batch, an error hop, a failure without one, batchComplete and a
 * stopped transform. Most tests run both in batch mode and row by row.
 */
class MergeIntoTableH2Test {

  private static final String CREATE_TARGET =
      "CREATE TABLE TARGET (ID BIGINT PRIMARY KEY, NAME VARCHAR(20) NOT NULL, AMOUNT DECIMAL(10,2))";
  private static final String COUNT_UPDATES =
      "CREATE TRIGGER TARGET_UPDATES AFTER UPDATE ON TARGET FOR EACH ROW CALL '"
          + UpdateCountingTrigger.class.getName()
          + "'";

  private MergeIntoTableH2Harness h2;

  /**
   * How the rows are written: in a recovered batch under a validated contract, or row by row (batch
   * updates off, a contract that is not validated, a savepoint per row).
   */
  enum Mode {
    BATCH_MARKS_FAILED_ROWS(true, BatchFailureContract.MARKS_FAILED_ROWS, false),
    BATCH_KEEPS_PREFIX(true, BatchFailureContract.KEEPS_PREFIX, false),
    BATCH_LOSES_WHOLE_BATCH(true, BatchFailureContract.LOSES_WHOLE_BATCH, false),
    /** Batch updates asked for, but the contract is not validated: one statement per row. */
    BATCH_NOT_VALIDATED(true, null, false),
    ROW_BY_ROW(false, BatchFailureContract.MARKS_FAILED_ROWS, false),
    /** A database whose failed statement aborts the transaction: a savepoint per row. */
    ROW_SAVEPOINTS(false, BatchFailureContract.MARKS_FAILED_ROWS, true);

    final boolean batch;
    final BatchFailureContract contract;
    final boolean savepoints;

    Mode(boolean batch, BatchFailureContract contract, boolean savepoints) {
      this.batch = batch;
      this.contract = contract;
      this.savepoints = savepoints;
    }

    /** True when rows wait in a batch until it is committed. */
    boolean batched() {
      return batch && contract != null;
    }
  }

  /** Gives the connection the contract and the savepoints of the mode. */
  private void use(Mode mode) {
    MergeTestH2Database dialect = (MergeTestH2Database) h2.databaseMeta.getIDatabase();
    dialect.setContract(mode.contract);
    dialect.setUseSafePoints(mode.savepoints);
  }

  private MergeIntoTableMeta meta(Mode mode, int commitSize) {
    use(mode);
    return meta(mode.batch, commitSize);
  }

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    MergeIntoTableH2Harness.registerH2();
  }

  @BeforeEach
  void setUp() throws Exception {
    h2 = new MergeIntoTableH2Harness(DmlSyntax.STANDARD_MERGE);
    h2.execute(CREATE_TARGET, COUNT_UPDATES);
    UpdateCountingTrigger.UPDATES.set(0);
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("amount"));
    return rowMeta;
  }

  private static Object[] row(long id, String name, String amount) {
    return new Object[] {id, name, amount == null ? null : new BigDecimal(amount)};
  }

  private static MergeIntoTableMeta meta(boolean batch, int commitSize) {
    MergeIntoTableMeta meta = new MergeIntoTableMeta();
    meta.setDefault();
    meta.setTableName("TARGET");
    meta.setCommitSize(Integer.toString(commitSize));
    meta.setUseBatchUpdate(batch);
    meta.getKeyFields().add(new MergeKeyField("id", "ID"));
    meta.getValueFields().add(new MergeValueField("name", "NAME", true));
    meta.getValueFields().add(new MergeValueField("amount", "AMOUNT", true));
    return meta;
  }

  /** The table as "id:name:amount" lines, by id. */
  private List<String> table() throws Exception {
    List<String> lines = new ArrayList<>();
    // The amount as text: Hop reads a DECIMAL column as a Number, which loses the scale.
    for (Object[] r :
        h2.query("SELECT ID, NAME, CAST(AMOUNT AS VARCHAR(20)) FROM TARGET ORDER BY ID")) {
      lines.add(r[0] + ":" + r[1] + ":" + (r[2] == null ? "null" : r[2]));
    }
    return lines;
  }

  private static List<Long> ids(List<Object[]> rows) {
    List<Long> ids = new ArrayList<>();
    for (Object[] r : rows) {
      ids.add((Long) r[0]);
    }
    return ids;
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void insertsNewKeys(Mode mode) throws Exception {
    MergeIntoTable transform =
        h2.run(
            meta(mode, 2),
            inputRowMeta(),
            List.of(row(1, "a", "1.50"), row(2, "b", "2.00"), row(3, "c", "3.25")),
            false);

    assertEquals(List.of("1:a:1.50", "2:b:2.00", "3:c:3.25"), table());
    assertEquals(List.of(1L, 2L, 3L), ids(h2.output));
    assertEquals(3, transform.getLinesOutput());
    assertEquals(0, transform.getErrors());
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void updatesExistingKeysAndInsertsNewOnes(Mode mode) throws Exception {
    h2.execute(
        "INSERT INTO TARGET VALUES (1, 'old1', 1)", "INSERT INTO TARGET VALUES (2, 'old2', 2)");

    MergeIntoTable transform =
        h2.run(
            meta(mode, 5),
            inputRowMeta(),
            List.of(row(1, "new1", "10"), row(2, "new2", "20"), row(3, "new3", "30")),
            false);

    assertEquals(List.of("1:new1:10.00", "2:new2:20.00", "3:new3:30.00"), table());
    assertEquals(2, UpdateCountingTrigger.UPDATES.get());
    // One count per statement: inserts and updates are both rows written.
    assertEquals(3, transform.getLinesOutput());
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void insertOnlyLeavesExistingRowsAlone(Mode mode) throws Exception {
    h2.execute("INSERT INTO TARGET VALUES (1, 'old1', 1)");
    MergeIntoTableMeta meta = meta(mode, 5);
    meta.setUpdateExisting(false);

    h2.run(meta, inputRowMeta(), List.of(row(1, "new1", "10"), row(2, "new2", "20")), false);

    assertEquals(List.of("1:old1:1.00", "2:new2:20.00"), table());
    assertEquals(0, UpdateCountingTrigger.UPDATES.get());
    assertEquals(List.of(1L, 2L), ids(h2.output));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void updateFlagOffWritesTheColumnOnInsertOnly(Mode mode) throws Exception {
    h2.execute("INSERT INTO TARGET VALUES (1, 'old1', 1)");
    MergeIntoTableMeta meta = meta(mode, 5);
    meta.getValueFields().get(0).setUpdate(false);

    h2.run(meta, inputRowMeta(), List.of(row(1, "new1", "10"), row(2, "new2", "20")), false);

    assertEquals(List.of("1:old1:10.00", "2:new2:20.00"), table());
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void updateOnlyChangedSkipsRowsThatAreTheSame(Mode mode) throws Exception {
    h2.execute(
        "INSERT INTO TARGET VALUES (1, 'same', 1)",
        "INSERT INTO TARGET VALUES (2, 'old', 2)",
        "INSERT INTO TARGET VALUES (3, 'nulls', NULL)",
        "INSERT INTO TARGET VALUES (4, 'tonull', 4)",
        "INSERT INTO TARGET VALUES (5, 'fromnull', NULL)");
    MergeIntoTableMeta meta = meta(mode, 10);
    meta.setUpdateOnlyChanged(true);

    h2.run(
        meta,
        inputRowMeta(),
        List.of(
            row(1, "same", "1"),
            row(2, "new", "2"),
            row(3, "nulls", null),
            row(4, "tonull", null),
            row(5, "fromnull", "5")),
        false);

    assertEquals(
        List.of("1:same:1.00", "2:new:2.00", "3:nulls:null", "4:tonull:null", "5:fromnull:5.00"),
        table());
    // Rows 1 and 3 are equal, NULL to NULL included: only 2, 4 and 5 are updated.
    assertEquals(3, UpdateCountingTrigger.UPDATES.get());
    // Every row is still passed on.
    assertEquals(List.of(1L, 2L, 3L, 4L, 5L), ids(h2.output));
  }

  @Test
  void withoutUpdateOnlyChangedEveryMatchedRowIsUpdated() throws Exception {
    h2.execute("INSERT INTO TARGET VALUES (1, 'same', 1)", "INSERT INTO TARGET VALUES (2, 'x', 2)");

    h2.run(meta(true, 10), inputRowMeta(), List.of(row(1, "same", "1"), row(2, "y", "2")), false);

    assertEquals(2, UpdateCountingTrigger.UPDATES.get());
  }

  /**
   * A CLOB can't be compared with {@code <>} on every database (ORA-00932 on Oracle): with one in
   * the updated columns, "update only changed rows" updates every matched row instead, and the
   * values are written. The column types are read for it although H2 doesn't cast.
   */
  @Test
  void updateOnlyChangedWithALargeObjectUpdatesEveryMatchedRow() throws Exception {
    h2.execute(
        "CREATE TABLE DOCS (ID BIGINT PRIMARY KEY, NAME VARCHAR(20) NOT NULL, NOTES CLOB)",
        "CREATE TRIGGER DOCS_UPDATES AFTER UPDATE ON DOCS FOR EACH ROW CALL '"
            + UpdateCountingTrigger.class.getName()
            + "'",
        "INSERT INTO DOCS VALUES (1, 'same', 'same notes')",
        "INSERT INTO DOCS VALUES (2, 'same', 'old notes')");
    MergeIntoTableMeta meta = new MergeIntoTableMeta();
    meta.setDefault();
    meta.setTableName("DOCS");
    meta.setCommitSize("10");
    meta.setUpdateOnlyChanged(true);
    meta.getKeyFields().add(new MergeKeyField("id", "ID"));
    meta.getValueFields().add(new MergeValueField("name", "NAME", true));
    meta.getValueFields().add(new MergeValueField("notes", "NOTES", true));
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("notes"));

    MergeIntoTable transform =
        h2.run(
            meta,
            rowMeta,
            List.of(
                new Object[] {1L, "same", "same notes"},
                new Object[] {2L, "same", "new notes"},
                new Object[] {3L, "new", "notes"}),
            false);

    String sql = transform.getData().statement.getSql();
    assertFalse(sql.contains("<>"), sql);
    // Both existing rows are updated, the unchanged one too: it can't be told apart.
    assertEquals(2, UpdateCountingTrigger.UPDATES.get());
    List<String> docs = new ArrayList<>();
    for (Object[] r :
        h2.query("SELECT ID, NAME, CAST(NOTES AS VARCHAR(50)) FROM DOCS ORDER BY ID")) {
      docs.add(r[0] + ":" + r[1] + ":" + r[2]);
    }
    assertEquals(List.of("1:same:same notes", "2:same:new notes", "3:new:notes"), docs);
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void writesNulls(Mode mode) throws Exception {
    h2.execute("INSERT INTO TARGET VALUES (1, 'a', 5)");

    h2.run(meta(mode, 5), inputRowMeta(), List.of(row(1, "a", null), row(2, "b", null)), false);

    assertEquals(List.of("1:a:null", "2:b:null"), table());
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void sameKeyTwiceInOneBatchIsInsertedThenUpdated(Mode mode) throws Exception {
    MergeIntoTable transform =
        h2.run(
            meta(mode, 10),
            inputRowMeta(),
            List.of(row(7, "first", "1"), row(7, "second", "2")),
            false);

    assertEquals(List.of("7:second:2.00"), table());
    assertEquals(List.of(7L, 7L), ids(h2.output));
    assertEquals(2, transform.getLinesOutput());
    assertEquals(0, transform.getErrors());
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void errorHopTakesOnlyTheBadRow(Mode mode) throws Exception {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 7; id++) {
      // NAME is NOT NULL: row 3, in the middle of the first batch of five, is refused.
      rows.add(row(id, id == 3 ? null : "n" + id, Long.toString(id)));
    }

    MergeIntoTable transform = h2.run(meta(mode, 5), inputRowMeta(), rows, true);

    assertEquals(1, h2.errors.size());
    assertEquals(3L, h2.errors.get(0).row()[0]);
    assertNotNull(h2.errors.get(0).description());
    assertEquals(List.of(1L, 2L, 4L, 5L, 6L, 7L), ids(h2.output));
    assertEquals(
        List.of("1:n1:1.00", "2:n2:2.00", "4:n4:4.00", "5:n5:5.00", "6:n6:6.00", "7:n7:7.00"),
        table());
    assertEquals(6, transform.getLinesOutput());
    assertEquals(1, transform.getLinesRejected());
    assertEquals(7, transform.getLinesRead());
    assertEquals(0, transform.getErrors());
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void withoutErrorHopAFailureStopsAndRollsBackTheOpenTransaction(Mode mode) throws Exception {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 8; id++) {
      // Row 7 is in the second commit window of five: the first five are committed before it.
      rows.add(row(id, id == 7 ? null : "n" + id, Long.toString(id)));
    }

    MergeIntoTable transform = h2.run(meta(mode, 5), inputRowMeta(), rows, false);

    assertEquals(1, transform.getErrors());
    assertTrue(h2.errors.isEmpty());
    assertEquals(List.of("1:n1:1.00", "2:n2:2.00", "3:n3:3.00", "4:n4:4.00", "5:n5:5.00"), table());
    if (mode.batched()) {
      // In batch mode a row is only passed on once it is committed.
      assertEquals(List.of(1L, 2L, 3L, 4L, 5L), ids(h2.output));
    } else {
      // Row by row a row is passed on once it is written: the sixth was, before the seventh failed.
      assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L), ids(h2.output));
    }
    assertEquals(0, transform.getLinesRejected());
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void sameKeyTwiceAroundARefusedRowWithAnErrorHop(Mode mode) throws Exception {
    MergeIntoTable transform =
        h2.run(
            meta(mode, 10),
            inputRowMeta(),
            List.of(row(7, "first", "1"), row(8, null, "8"), row(7, "second", "2")),
            true);

    assertEquals(List.of("7:second:2.00"), table());
    assertEquals(List.of(7L, 7L), ids(h2.output));
    assertEquals(1, h2.errors.size());
    assertEquals(8L, h2.errors.get(0).row()[0]);
    assertEquals(mode.batched() ? "MIT002" : "MIT001", h2.errors.get(0).code());
    assertEquals(2, transform.getLinesOutput());
    assertEquals(1, transform.getLinesRejected());
    assertEquals(0, transform.getErrors());
  }

  /** Every row reaches exactly one stream, in its input order, over several batches. */
  @ParameterizedTest(name = "{0}")
  @EnumSource(Mode.class)
  void everyRowReachesOneStreamOverSeveralBatches(Mode mode) throws Exception {
    h2.execute(
        "INSERT INTO TARGET VALUES (2, 'old', 2)", "INSERT INTO TARGET VALUES (9, 'old', 9)");
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 23; id++) {
      // Every fifth row is refused: 5, 10, 15 and 20, two of them existing keys.
      rows.add(row(id % 7, id % 5 == 0 ? null : "n" + id, Long.toString(id)));
    }

    MergeIntoTable transform = h2.run(meta(mode, 4), inputRowMeta(), rows, true);

    List<Long> expected = new ArrayList<>();
    for (long id = 1; id <= 23; id++) {
      if (id % 5 != 0) {
        expected.add(id % 7);
      }
    }
    assertEquals(expected, ids(h2.output));
    assertEquals(List.of(5L, 3L, 1L, 6L), ids(errorRows()));
    assertEquals(19, transform.getLinesOutput());
    assertEquals(4, transform.getLinesRejected());
    assertEquals(23, transform.getLinesRead());
    // The last written row of each key wins.
    assertEquals(
        List.of(
            "0:n21:21.00",
            "1:n22:22.00",
            "2:n23:23.00",
            "3:n17:17.00",
            "4:n18:18.00",
            "5:n19:19.00",
            "6:n13:13.00",
            "9:old:9.00"),
        table());
  }

  private List<Object[]> errorRows() {
    List<Object[]> rows = new ArrayList<>();
    for (MergeIntoTableH2Harness.ErrorRow error : h2.errors) {
      rows.add(error.row());
    }
    return rows;
  }

  @Test
  void aContractThatIsNotValidatedWritesRowByRow() throws Exception {
    h2.input.add(row(1, "a", "1"));
    h2.input.add(row(2, "b", "2"));
    MergeIntoTable transform =
        h2.create(meta(Mode.BATCH_NOT_VALIDATED, 100), inputRowMeta(), false);
    assertTrue(transform.init());
    try {
      assertTrue(transform.processRow());
      assertTrue(transform.processRow());
      // Not held in a batch: every row is a statement of its own and passed on at once.
      assertEquals(List.of(1L, 2L), ids(h2.output));
      assertFalse(transform.processRow());
    } finally {
      transform.dispose();
    }
    assertEquals(List.of("1:a:1.00", "2:b:2.00"), table());
  }

  @Test
  void batchCompleteRunsAndCommitsThePendingBatch() throws Exception {
    h2.input.add(row(1, "a", "1"));
    h2.input.add(row(2, "b", "2"));
    MergeIntoTable transform = h2.create(meta(true, 100), inputRowMeta(), false);
    assertTrue(transform.init());
    try {
      assertTrue(transform.processRow());
      assertTrue(transform.processRow());
      assertTrue(h2.output.isEmpty(), "rows wait in the batch until it is committed");
      assertTrue(table().isEmpty());

      transform.batchComplete();

      assertEquals(List.of(1L, 2L), ids(h2.output));
      assertEquals(List.of("1:a:1.00", "2:b:2.00"), table());
      assertFalse(transform.processRow());
    } finally {
      transform.dispose();
    }
    assertEquals(2, h2.output.size());
  }

  @Test
  void disposeAfterStopDiscardsThePendingBatch() throws Exception {
    h2.input.add(row(1, "a", "1"));
    h2.input.add(row(2, "b", "2"));
    MergeIntoTable transform = h2.create(meta(true, 100), inputRowMeta(), false);
    assertTrue(transform.init());
    assertTrue(transform.processRow());
    assertTrue(transform.processRow());

    transform.setStopped(true);
    transform.dispose();

    assertTrue(h2.output.isEmpty());
    assertTrue(table().isEmpty());
  }

  @Test
  void castParametersReadsTheColumnTypes() throws Exception {
    h2 = new MergeIntoTableH2Harness(DmlSyntax.STANDARD_MERGE.withCastParameters(true));
    h2.execute(CREATE_TARGET, "INSERT INTO TARGET VALUES (1, 'old', 1)");

    h2.run(meta(true, 5), inputRowMeta(), List.of(row(1, "new", "1.5"), row(2, "b", "2")), false);

    assertEquals(List.of("1:new:1.50", "2:b:2.00"), table());
  }

  @Test
  void initRefusesADatabaseWithoutMerge() throws Exception {
    h2 = new MergeIntoTableH2Harness(null);
    MergeIntoTable transform = h2.create(meta(true, 5), inputRowMeta(), false);

    assertFalse(transform.init());
    String reason = DmlSupport.unsupportedReason(h2.databaseMeta, null);
    assertNotNull(reason);
    assertTrue(h2.loggedErrors.contains(reason), h2.loggedErrors.toString());
  }

  @Test
  void initRefusesAServerVersionTooOld() throws Exception {
    h2 =
        new MergeIntoTableH2Harness(
            DmlSyntax.builder()
                .upsert(DmlSyntax.Upsert.of(UpsertForm.MERGE).withMinimumMajorVersion(99))
                .build());
    MergeIntoTable transform = h2.create(meta(true, 5), inputRowMeta(), false);

    assertFalse(transform.init());
    String reason;
    try (Database db =
        new Database(new LoggingObject("version"), new Variables(), h2.databaseMeta)) {
      db.connect();
      reason =
          DmlSupport.unsupportedReason(h2.databaseMeta, db.getDatabaseMetaData(), UpsertForm.MERGE);
    }
    assertNotNull(reason);
    assertTrue(reason.contains("99"), reason);
    assertTrue(h2.loggedErrors.contains(reason), h2.loggedErrors.toString());
  }

  @Test
  void checkReportsAnUnsupportedDatabaseAsAnError() throws Exception {
    h2 = new MergeIntoTableH2Harness(null);
    List<ICheckResult> remarks = check(h2, meta(true, 5));

    String reason = DmlSupport.unsupportedReason(h2.databaseMeta, null);
    assertTrue(
        remarks.stream()
            .anyMatch(
                r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR && reason.equals(r.getText())),
        remarks.toString());
  }

  @Test
  void checkHasNoErrorOnASupportedDatabase() throws Exception {
    List<ICheckResult> remarks = check(h2, meta(true, 5));

    assertNull(
        remarks.stream()
            .filter(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR)
            .findFirst()
            .orElse(null));
  }

  @Test
  void checkReportsMissingColumns() throws Exception {
    MergeIntoTableMeta meta = meta(true, 5);
    meta.getValueFields().add(new MergeValueField("name", "NO_SUCH_COLUMN", true));

    List<ICheckResult> remarks = check(h2, meta);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("NO_SUCH_COLUMN")),
        remarks.toString());
  }

  /**
   * Verify resolves the stream fields and the columns as the transform does when it runs: a key set
   * by metadata injection to a variable is not reported missing.
   */
  @Test
  void checkResolvesVariablesInFieldsAndColumns() throws Exception {
    MergeIntoTableMeta meta = meta(true, 5);
    meta.getKeyFields().clear();
    meta.getKeyFields().add(new MergeKeyField("${KEY_FIELD}", "${KEY_COLUMN}"));
    Variables variables = new Variables();
    variables.setVariable("KEY_FIELD", "id");
    variables.setVariable("KEY_COLUMN", "ID");

    List<ICheckResult> remarks = check(h2, meta, variables);

    assertNull(
        remarks.stream()
            .filter(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR)
            .findFirst()
            .orElse(null),
        remarks.toString());

    // A variable that resolves to a column that isn't there is reported resolved.
    variables.setVariable("KEY_COLUMN", "NO_SUCH_KEY");
    remarks = check(h2, meta, variables);
    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("NO_SUCH_KEY")),
        remarks.toString());
  }

  private static List<ICheckResult> check(MergeIntoTableH2Harness h2, MergeIntoTableMeta meta) {
    return check(h2, meta, new Variables());
  }

  private static List<ICheckResult> check(
      MergeIntoTableH2Harness h2, MergeIntoTableMeta meta, Variables variables) {
    meta.setConnection(MergeIntoTableH2Harness.CONNECTION);
    TransformMeta transformMeta = new TransformMeta("merge", meta);
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        h2.pipelineMeta,
        transformMeta,
        inputRowMeta(),
        new String[] {"input"},
        new String[0],
        null,
        variables,
        h2.pipelineMeta.getMetadataProvider());
    return remarks;
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.database.batch.MultiRowBatch;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Table Output with several rows per INSERT, end to end on H2 and DuckDB, whose dialects both
 * declare a multi-row INSERT: clean loads with remainder chunks, a table name in a field, the error
 * hop with bad rows, a failure without one, batchComplete and dispose, partitioning, specified
 * fields, and the cases that fall back to one row per statement.
 */
class TableOutputMultiRowTest {

  private static final String CONNECTION = "multi-row";

  @TempDir static Path tempDir;

  private static int duckDbFiles;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(CastingH2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(PickyH2DatabaseMeta.class);
  }

  /**
   * H2, DuckDB, H2 casting its VALUES lists to the column types as SQL Server does, and H2 ruling
   * out several rows per statement for some tables (not among the databases every test runs on).
   */
  enum Db {
    H2,
    DUCKDB,
    CASTING_H2,
    PICKY_H2
  }

  /** A row handed to the error stream. */
  record ErrorRow(Object[] row, String description, String code) {}

  /** Runs a real Table Output on a real database; only the row hand-off is replaced. */
  static final class Harness {
    final DatabaseMeta databaseMeta;
    final PipelineMeta pipelineMeta = new PipelineMeta();
    final Deque<Object[]> input = new ArrayDeque<>();
    final List<Object[]> output = new ArrayList<>();
    final List<ErrorRow> errors = new ArrayList<>();

    Harness(Db db) throws HopException {
      databaseMeta =
          db != Db.DUCKDB
              ? new DatabaseMeta(
                  CONNECTION,
                  db == Db.H2 ? "H2" : db.name(),
                  "Native",
                  "",
                  "mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
                  "",
                  "",
                  "")
              : new DatabaseMeta(
                  CONNECTION,
                  "DuckDB",
                  "Native",
                  "",
                  tempDir.resolve("out-" + (++duckDbFiles) + ".duckdb").toString(),
                  "",
                  "",
                  "");
      MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
      metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
      pipelineMeta.setMetadataProvider(metadataProvider);
    }

    void execute(String... statements) throws HopException {
      try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
        db.connect();
        for (String statement : statements) {
          db.execStatement(statement);
        }
        try {
          // DuckDB refuses a commit in auto-commit mode, where there is nothing left to commit.
          if (!db.getConnection().getAutoCommit()) {
            db.commit(true);
          }
        } catch (SQLException e) {
          throw new HopException(e);
        }
      }
    }

    List<String> query(String sql) throws HopException {
      try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta)) {
        db.connect();
        List<String> lines = new ArrayList<>();
        for (Object[] row : db.getRows(sql, -1)) {
          lines.add(row[0] + ":" + row[1]);
        }
        return lines;
      }
    }

    TableOutput create(TableOutputMeta meta, IRowMeta inputRowMeta, boolean errorHandling) {
      meta.setConnection(CONNECTION);
      TransformMeta transformMeta = new TransformMeta("out", meta);
      pipelineMeta.addTransform(transformMeta);
      TableOutput transform =
          new TableOutput(
              transformMeta,
              meta,
              new TableOutputData(),
              0,
              pipelineMeta,
              new LocalPipelineEngine()) {
            @Override
            public IRowMeta getInputRowMeta() {
              return inputRowMeta;
            }

            @Override
            public Object[] getRow() {
              Object[] row = input.poll();
              if (row != null) {
                incrementLinesRead();
              }
              return row;
            }

            @Override
            public void putRow(IRowMeta rowMeta, Object[] row) {
              output.add(row);
            }

            @Override
            public void putError(
                IRowMeta rowMeta,
                Object[] row,
                long nrErrors,
                String errorDescriptions,
                String fieldNames,
                String errorCodes) {
              errors.add(new ErrorRow(row, errorDescriptions, errorCodes));
            }
          };
      if (errorHandling) {
        TransformMeta target = new TransformMeta("errors", new TableOutputMeta());
        pipelineMeta.addTransform(target);
        pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
        TransformErrorMeta errorMeta = new TransformErrorMeta(transformMeta, target);
        errorMeta.setEnabled(true);
        transformMeta.setTransformErrorMeta(errorMeta);
      }
      return transform;
    }

    TableOutput run(
        TableOutputMeta meta, IRowMeta inputRowMeta, List<Object[]> rows, boolean errorHandling)
        throws HopException {
      input.addAll(rows);
      TableOutput transform = create(meta, inputRowMeta, errorHandling);
      if (!transform.init()) {
        throw new HopException("init failed");
      }
      try {
        while (transform.processRow()) {
          // keep going
        }
      } finally {
        transform.dispose();
      }
      return transform;
    }
  }

  private static Harness harness(Db db, String... tables) throws HopException {
    Harness harness = new Harness(db);
    for (String table : tables) {
      harness.execute(
          "CREATE TABLE " + table + " (ID BIGINT PRIMARY KEY, LABEL VARCHAR(20) NOT NULL)");
    }
    return harness;
  }

  static Stream<Arguments> databasesAndSizes() {
    List<Arguments> arguments = new ArrayList<>();
    for (Db db : List.of(Db.H2, Db.DUCKDB, Db.CASTING_H2)) {
      for (int rows : new int[] {1, 2, 5, 100}) {
        arguments.add(Arguments.of(db, rows));
      }
    }
    return arguments.stream();
  }

  private static IRowMeta rowMeta(boolean withTable) {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("ID"));
    rowMeta.addValueMeta(new ValueMetaString("LABEL"));
    if (withTable) {
      rowMeta.addValueMeta(new ValueMetaString("TABLE_NAME"));
    }
    return rowMeta;
  }

  private static TableOutputMeta meta(int commitSize, int rowsPerStatement) {
    TableOutputMeta meta = new TableOutputMeta();
    meta.setDefault();
    meta.setTableName("TARGET");
    meta.setCommitSize(Integer.toString(commitSize));
    meta.setUseBatchUpdate(true);
    meta.setRowsPerStatement(Integer.toString(rowsPerStatement));
    return meta;
  }

  /** Rows 1..count; the bad ones have a null label, which the NOT NULL column refuses. */
  private static List<Object[]> rows(int count, Set<Long> bad) {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= count; id++) {
      rows.add(new Object[] {id, bad.contains(id) ? null : "r" + id});
    }
    return rows;
  }

  private static List<String> lines(int count, Set<Long> bad) {
    List<String> lines = new ArrayList<>();
    for (long id = 1; id <= count; id++) {
      if (!bad.contains(id)) {
        lines.add(id + ":r" + id);
      }
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

  private static List<Long> errorIds(Harness h) {
    List<Long> ids = new ArrayList<>();
    for (ErrorRow error : h.errors) {
      ids.add((Long) error.row()[0]);
    }
    return ids;
  }

  /** The plan of the tables written with several rows per statement, the same for all; or null. */
  static DmlPlan multiRowPlan(TableOutput transform) {
    return transform.getData().insertPlans.values().stream()
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  private static void assertMode(TableOutput transform, int rowsPerStatement) {
    TableOutputData data = transform.getData();
    if (rowsPerStatement > 1) {
      assertNotNull(multiRowPlan(transform), "several rows per statement are planned");
      assertEquals(rowsPerStatement, multiRowPlan(transform).getRowsPerStatement());
      assertTrue(data.multiRowCandidate);
    } else {
      assertNull(multiRowPlan(transform));
      assertFalse(data.multiRowCandidate);
    }
  }

  @ParameterizedTest(name = "{0} x{1}")
  @MethodSource("databasesAndSizes")
  void cleanLoadWithRemainderChunks(Db db, int rowsPerStatement) throws Exception {
    Harness h = harness(db, "TARGET");

    // 23 rows with a commit size of 10: two full flushes and a remainder of 3.
    TableOutput transform =
        h.run(meta(10, rowsPerStatement), rowMeta(false), rows(23, Set.of()), false);

    assertMode(transform, rowsPerStatement);
    assertEquals(lines(23, Set.of()), h.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
    assertEquals(ids(rows(23, Set.of())), ids(h.output));
    assertEquals(23, transform.getLinesOutput());
    assertEquals(0, transform.getErrors());
  }

  @ParameterizedTest(name = "{0} x{1}")
  @MethodSource("databasesAndSizes")
  void theErrorHopGetsExactlyTheBadRows(Db db, int rowsPerStatement) throws Exception {
    Harness h = harness(db, "TARGET");
    // Row 20 is already there: a duplicate key, refused too.
    h.execute("INSERT INTO TARGET VALUES (20, 'seeded')");
    Set<Long> bad = Set.of(3L, 8L, 9L, 17L);

    TableOutput transform = h.run(meta(10, rowsPerStatement), rowMeta(false), rows(23, bad), true);

    assertMode(transform, rowsPerStatement);
    assertEquals(List.of(3L, 8L, 9L, 17L, 20L), errorIds(h));
    for (ErrorRow error : h.errors) {
      assertNotNull(error.description());
      assertEquals("TOP0002", error.code());
    }
    List<String> expected = new ArrayList<>(lines(23, bad));
    expected.set(expected.indexOf("20:r20"), "20:seeded");
    assertEquals(expected, h.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
    List<Long> written = new ArrayList<>(ids(rows(23, Set.of())));
    written.removeAll(List.of(3L, 8L, 9L, 17L, 20L));
    assertEquals(written, ids(h.output));
    assertEquals(0, transform.getErrors());
  }

  @ParameterizedTest(name = "{0} x{1}")
  @MethodSource("databasesAndSizes")
  void withoutAnErrorHopAFailureStopsAndKeepsOnlyCommittedFlushes(Db db, int rowsPerStatement)
      throws Exception {
    Harness h = harness(db, "TARGET");

    // Row 13 is in the second flush of ten: the first ten are committed before it.
    TableOutput transform =
        h.run(meta(10, rowsPerStatement), rowMeta(false), rows(25, Set.of(13L)), false);

    assertEquals(1, transform.getErrors());
    assertTrue(h.errors.isEmpty());
    assertEquals(lines(10, Set.of()), h.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
    assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), ids(h.output));
  }

  @ParameterizedTest(name = "{0} x{1}")
  @MethodSource("databasesAndSizes")
  void aTableNameInAFieldKeepsABatchPerTable(Db db, int rowsPerStatement) throws Exception {
    Harness h = harness(db, "T_ODD", "T_EVEN");
    Set<Long> bad = Set.of(4L, 7L, 15L);
    List<Object[]> rows = new ArrayList<>();
    for (Object[] r : rows(23, bad)) {
      long id = (Long) r[0];
      rows.add(new Object[] {r[0], r[1], id % 2 == 1 ? "T_ODD" : "T_EVEN"});
    }
    TableOutputMeta meta = meta(5, rowsPerStatement);
    meta.setTableNameInField(true);
    meta.setTableNameField("TABLE_NAME");
    meta.setTableNameInTable(false);

    TableOutput transform = h.run(meta, rowMeta(true), rows, true);

    assertMode(transform, rowsPerStatement);
    assertEquals(List.of(4L, 7L, 15L), errorIds(h).stream().sorted().toList());
    List<String> odd = new ArrayList<>();
    List<String> even = new ArrayList<>();
    for (String line : lines(23, bad)) {
      long id = Long.parseLong(line.substring(0, line.indexOf(':')));
      (id % 2 == 1 ? odd : even).add(line);
    }
    assertEquals(odd, h.query("SELECT ID, LABEL FROM T_ODD ORDER BY ID"));
    assertEquals(even, h.query("SELECT ID, LABEL FROM T_EVEN ORDER BY ID"));
    assertEquals(20, h.output.size());
    assertEquals(20, transform.getLinesOutput());
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"H2", "DUCKDB"})
  void batchCompleteRunsAndCommitsThePendingRows(String name) throws Exception {
    Harness h = harness(Db.valueOf(name), "TARGET");
    h.input.addAll(rows(7, Set.of()));
    TableOutput transform = h.create(meta(100, 5), rowMeta(false), false);
    assertTrue(transform.init());
    try {
      for (int i = 0; i < 7; i++) {
        assertTrue(transform.processRow());
      }
      assertEquals(1, transform.getData().batches.size());
      assertInstanceOf(MultiRowBatch.class, transform.getData().batches.get("TARGET"));
      assertTrue(h.output.isEmpty(), "rows wait in the batch until it is committed");

      transform.batchComplete();

      assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L), ids(h.output));
      assertEquals(lines(7, Set.of()), h.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
    } finally {
      transform.dispose();
    }
    assertEquals(7, h.output.size());
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"H2", "DUCKDB"})
  void disposeFlushesTheLastRowsOrDiscardsThemAfterAStop(String name) throws Exception {
    Harness h = harness(Db.valueOf(name), "TARGET");
    h.input.addAll(rows(3, Set.of()));
    TableOutput transform = h.create(meta(100, 2), rowMeta(false), false);
    assertTrue(transform.init());
    for (int i = 0; i < 3; i++) {
      assertTrue(transform.processRow());
    }
    transform.setStopped(true);
    transform.dispose();

    assertTrue(h.output.isEmpty());
    assertTrue(h.query("SELECT ID, LABEL FROM TARGET").isEmpty());

    Harness again = harness(Db.valueOf(name), "TARGET");
    again.input.addAll(rows(3, Set.of()));
    TableOutput second = again.create(meta(100, 2), rowMeta(false), false);
    assertTrue(second.init());
    for (int i = 0; i < 3; i++) {
      assertTrue(second.processRow());
    }
    second.dispose();

    assertEquals(List.of(1L, 2L, 3L), ids(again.output));
    assertEquals(lines(3, Set.of()), again.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"H2", "DUCKDB"})
  void specifiedFieldsMapStreamFieldsToColumns(String name) throws Exception {
    Harness h = harness(Db.valueOf(name), "TARGET");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("ignored"));
    rowMeta.addValueMeta(new ValueMetaString("text"));
    rowMeta.addValueMeta(new ValueMetaInteger("key"));
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 9; id++) {
      rows.add(new Object[] {"x", "t" + id, id});
    }
    TableOutputMeta meta = meta(4, 3);
    meta.setSpecifyFields(true);
    meta.getFields().add(new TableOutputField("ID", "key"));
    meta.getFields().add(new TableOutputField("LABEL", "text"));

    TableOutput transform = h.run(meta, rowMeta, rows, true);

    assertEquals(3, multiRowPlan(transform).getRowsPerStatement());
    List<String> expected = new ArrayList<>();
    for (long id = 1; id <= 9; id++) {
      expected.add(id + ":t" + id);
    }
    assertEquals(expected, h.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
    assertEquals(9, h.output.size());
  }

  @Test
  void partitionedTablesGetABatchEach() throws Exception {
    Harness h = harness(Db.H2, "TARGET_202601", "TARGET_202602");
    IRowMeta rowMeta = rowMeta(false);
    rowMeta.addValueMeta(new ValueMetaDate("DAY"));
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= 12; id++) {
      rows.add(new Object[] {id, "r" + id, date(2026, id % 2 == 1 ? 1 : 2, (int) id)});
    }
    TableOutputMeta meta = meta(100, 4);
    meta.setSpecifyFields(true);
    meta.getFields().add(new TableOutputField("ID", "ID"));
    meta.getFields().add(new TableOutputField("LABEL", "LABEL"));
    meta.setPartitioningEnabled(true);
    meta.setPartitioningField("DAY");
    meta.setPartitioningMonthly(true);
    meta.setPartitioningDaily(false);

    TableOutput transform = h.run(meta, rowMeta, rows, false);

    assertEquals(4, multiRowPlan(transform).getRowsPerStatement());
    assertEquals(
        List.of("1:r1", "3:r3", "5:r5", "7:r7", "9:r9", "11:r11"),
        h.query("SELECT ID, LABEL FROM TARGET_202601 ORDER BY ID"));
    assertEquals(
        List.of("2:r2", "4:r4", "6:r6", "8:r8", "10:r10", "12:r12"),
        h.query("SELECT ID, LABEL FROM TARGET_202602 ORDER BY ID"));
    assertEquals(12, h.output.size());
  }

  private static Date date(int year, int month, int day) {
    Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
    calendar.clear();
    calendar.set(year, month - 1, day, 12, 0);
    return calendar.getTime();
  }

  /**
   * H2 as a dialect that rules out several rows per statement for a table whose name starts with
   * SLOW, as MySQL does for a MyISAM table.
   */
  @DatabaseMetaPlugin(type = "PICKY_H2", typeDescription = "H2 refusing some tables")
  public static class PickyH2DatabaseMeta extends H2DatabaseMeta {
    @Override
    public DmlSyntax getDmlSyntax() {
      return DmlSyntax.builder()
          .insertForm(DmlSyntax.InsertForm.VALUES_LIST)
          .maxParameters(100_000)
          .build();
    }

    @Override
    public BatchFailureContract getBatchFailureContract() {
      return BatchFailureContract.MARKS_FAILED_ROWS;
    }

    @Override
    public String getMultiRowDmlRefusal(Connection connection, DmlSpec spec) {
      return spec.getQuotedTable().toUpperCase(Locale.ROOT).contains("SLOW")
          ? "a table without transactions"
          : null;
    }
  }

  /**
   * Each table is planned on its first row: the database can rule out several rows per statement
   * for one table and not for another, and the rows of both still go out in input order.
   */
  @Test
  void eachTableIsPlannedOnItsOwn() throws Exception {
    Harness h = harness(Db.PICKY_H2, "T_FAST", "T_SLOW");
    List<Object[]> rows = new ArrayList<>();
    for (Object[] r : rows(23, Set.of(6L, 9L))) {
      long id = (Long) r[0];
      rows.add(new Object[] {r[0], r[1], id % 2 == 1 ? "T_FAST" : "T_SLOW"});
    }
    TableOutputMeta meta = meta(5, 4);
    meta.setTableNameInField(true);
    meta.setTableNameField("TABLE_NAME");
    meta.setTableNameInTable(false);

    TableOutput transform = h.run(meta, rowMeta(true), rows, true);

    Map<String, DmlPlan> plans = transform.getData().insertPlans;
    assertEquals(4, plans.get("T_FAST").getRowsPerStatement());
    assertTrue(plans.containsKey("T_SLOW"));
    assertNull(plans.get("T_SLOW"), "the refused table gets one row per statement");
    assertEquals(List.of(6L, 9L), errorIds(h));
    List<Long> written = new ArrayList<>(ids(rows(23, Set.of())));
    written.removeAll(List.of(6L, 9L));
    assertEquals(written, ids(h.output));
  }

  /**
   * A value that can't be bound (text in an Integer field) goes to the error hop with TOP001, in
   * its turn, whether it is bound when it arrives (one row per statement) or when its batch is
   * flushed (several rows per statement).
   */
  @ParameterizedTest(name = "{0} x{1}")
  @MethodSource("databasesAndSizes")
  void aValueThatCantBeBoundGoesToTheErrorHopWithTop001(Db db, int rowsPerStatement)
      throws Exception {
    Harness h = harness(db, "TARGET");
    List<Object[]> rows = rows(12, Set.of());
    rows.set(4, new Object[] {"x5", "r5"});

    TableOutput transform = h.run(meta(10, rowsPerStatement), rowMeta(false), rows, true);

    assertMode(transform, rowsPerStatement);
    assertEquals(1, h.errors.size());
    assertEquals("x5", h.errors.get(0).row()[0]);
    assertEquals("TOP001", h.errors.get(0).code());
    assertEquals(lines(12, Set.of(5L)), h.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
    List<Long> written = new ArrayList<>(ids(rows(12, Set.of())));
    written.remove(5L);
    assertEquals(written, ids(h.output));
    assertEquals(0, transform.getErrors());
  }

  /**
   * Without an error hop a value that can't be bound stops the transform, and only the flushes
   * committed before it stay. With several rows per statement the error names the table and the
   * row.
   */
  @ParameterizedTest(name = "{0} x{1}")
  @MethodSource("databasesAndSizes")
  void withoutAnErrorHopAValueThatCantBeBoundStops(Db db, int rowsPerStatement) throws Exception {
    Harness h = harness(db, "TARGET");
    List<Object[]> rows = rows(25, Set.of());
    rows.set(12, new Object[] {"x13", "r13"});

    TableOutput transform = h.run(meta(10, rowsPerStatement), rowMeta(false), rows, false);

    assertEquals(1, transform.getErrors());
    assertTrue(h.errors.isEmpty());
    assertEquals(lines(10, Set.of()), h.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
    assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), ids(h.output));
    if (rowsPerStatement > 1) {
      String log =
          HopLogStore.getAppender().getBuffer(transform.getLogChannelId(), false).toString();
      assertTrue(
          log.contains("Error inserting row into table [TARGET] with values: [x13, r13]"), log);
    }
  }

  /**
   * H2 as a dialect that casts the VALUES list of several rows to the column types, as SQL Server.
   */
  @DatabaseMetaPlugin(type = "CASTING_H2", typeDescription = "H2 casting its VALUES lists")
  public static class CastingH2DatabaseMeta extends H2DatabaseMeta {
    @Override
    public DmlSyntax getDmlSyntax() {
      return DmlSyntax.builder()
          .castParameters(true)
          .castValuesLists(true)
          .insertForm(DmlSyntax.InsertForm.VALUES_LIST)
          // It runs on H2, which takes at most 100,000 parameters per statement.
          .maxParameters(100_000)
          .build();
    }

    @Override
    public BatchFailureContract getBatchFailureContract() {
      return BatchFailureContract.MARKS_FAILED_ROWS;
    }
  }

  /**
   * A dialect whose VALUES lists of several rows are cast needs the column types: they are read
   * from the table, as SQL Server needs it, and a refused row still goes to the error hop alone.
   */
  @Test
  void aDialectThatCastsItsValuesListsGetsTheColumnTypes() throws Exception {
    Harness h = harness(Db.CASTING_H2, "TARGET");

    TableOutput transform = h.run(meta(10, 5), rowMeta(false), rows(23, Set.of(8L)), true);

    assertEquals(5, multiRowPlan(transform).getRowsPerStatement());
    DmlSpec spec = transform.getData().insertSpecs.get("TARGET");
    assertEquals(
        List.of("BIGINT", "CHARACTER VARYING(20)"),
        spec.getSourceColumns().stream().map(DmlColumn::nativeType).toList());
    assertEquals(List.of(8L), errorIds(h));
    assertEquals(lines(23, Set.of(8L)), h.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
    assertEquals(22, h.output.size());
  }

  static Stream<Arguments> databasesAndWideSizes() {
    List<Arguments> arguments = new ArrayList<>();
    for (Db db : List.of(Db.H2, Db.DUCKDB, Db.CASTING_H2)) {
      for (int rows : new int[] {100, 1000}) {
        arguments.add(Arguments.of(db, rows));
      }
    }
    return arguments.stream();
  }

  /**
   * A table of 1000 columns without an error hop, so a statement refused for its size stops the
   * transform where one row per statement works. 1003 rows with a commit size of 1000: full
   * statements and a remainder of three rows.
   */
  @ParameterizedTest(name = "{0} x{1}")
  @MethodSource("databasesAndWideSizes")
  void aWideTableIsWrittenInStatementsOfSeveralRows(Db db, int rowsPerStatement) throws Exception {
    Harness h = new Harness(db);
    int columns = 999;
    StringBuilder ddl = new StringBuilder("CREATE TABLE TARGET (ID BIGINT PRIMARY KEY");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("ID"));
    for (int c = 1; c <= columns; c++) {
      String name = String.format(Locale.ROOT, "C%04d", c);
      ddl.append(", ").append(name).append(" VARCHAR(20)");
      rowMeta.addValueMeta(new ValueMetaString(name));
    }
    h.execute(ddl.append(')').toString());
    List<Object[]> rows = new ArrayList<>();
    List<String> first = new ArrayList<>();
    List<String> last = new ArrayList<>();
    for (long id = 1; id <= 1003; id++) {
      Object[] row = new Object[columns + 1];
      row[0] = id;
      for (int c = 1; c <= columns; c++) {
        row[c] = id + "-" + c;
      }
      rows.add(row);
      first.add(id + ":" + id + "-1");
      last.add(id + ":" + id + "-" + columns);
    }

    TableOutput transform = h.run(meta(1000, rowsPerStatement), rowMeta, rows, false);

    // H2 takes at most 100,000 parameters per statement: 100 rows of 1000 columns.
    int expected = db == Db.DUCKDB ? rowsPerStatement : Math.min(rowsPerStatement, 100);
    assertEquals(expected, multiRowPlan(transform).getRowsPerStatement());
    assertEquals(0, transform.getErrors());
    assertEquals(1003, transform.getLinesOutput());
    assertEquals(1003, h.output.size());
    assertEquals(first, h.query("SELECT ID, C0001 FROM TARGET ORDER BY ID"));
    assertEquals(last, h.query("SELECT ID, C0999 FROM TARGET ORDER BY ID"));
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"ignoreErrors", "noBatch", "unlimitedCommit"})
  void optionsThatRuleOutSeveralRowsPerStatementKeepTodaysPath(String option) throws Exception {
    Harness h = harness(Db.H2, "TARGET");
    TableOutputMeta meta = meta(10, 100);
    switch (option) {
      case "ignoreErrors" -> meta.setIgnoreErrors(true);
      case "noBatch" -> meta.setUseBatchUpdate(false);
      default -> meta.setCommitSize("0");
    }

    TableOutput transform = h.run(meta, rowMeta(false), rows(12, Set.of()), false);

    assertFalse(transform.getData().multiRowCandidate);
    assertNull(multiRowPlan(transform));
    assertEquals(lines(12, Set.of()), h.query("SELECT ID, LABEL FROM TARGET ORDER BY ID"));
    assertEquals(12, h.output.size());
  }

  @Test
  void aNewTransformAsksForAHundredRowsPerStatementALoadedOneForOne() throws Exception {
    TableOutputMeta meta = new TableOutputMeta();
    meta.setDefault();
    assertEquals("100", meta.getRowsPerStatement());
    assertEquals(100, meta.getRowsPerStatement(new Variables()));

    TableOutputMeta loaded = new TableOutputMeta();
    loaded.loadXml(
        XmlHandler.loadXmlString("<transform><commit>10</commit></transform>").getDocumentElement(),
        new MemoryMetadataProvider());
    assertEquals(1, loaded.getRowsPerStatement(new Variables()));

    meta.setRowsPerStatement("${ROWS}");
    Variables variables = new Variables();
    variables.setVariable("ROWS", "25");
    assertEquals(25, meta.getRowsPerStatement(variables));
    meta.setRowsPerStatement("nonsense");
    assertEquals(1, meta.getRowsPerStatement(variables));

    meta.setRowsPerStatement("42");
    TableOutputMeta copy = new TableOutputMeta();
    copy.loadXml(
        XmlHandler.loadXmlString("<transform>" + meta.getXml() + "</transform>")
            .getDocumentElement(),
        new MemoryMetadataProvider());
    assertEquals("42", copy.getRowsPerStatement());
  }

  /**
   * Times 100,000 rows into DuckDB, one row per statement against 100. Run with {@code
   * -Dhop.dml.benchmark=true}; nothing is asserted on the times.
   */
  @Test
  @EnabledIfSystemProperty(named = "hop.dml.benchmark", matches = "true")
  void measureDuckDb() throws Exception {
    for (int rowsPerStatement : new int[] {1, 100, 1, 100}) {
      Harness h = harness(Db.DUCKDB, "TARGET");
      long start = System.nanoTime();
      TableOutput transform =
          h.run(meta(1000, rowsPerStatement), rowMeta(false), rows(100_000, Set.of()), true);
      long millis = (System.nanoTime() - start) / 1_000_000;
      assertEquals(100_000, transform.getLinesOutput());
      System.out.println(
          "Table output, DuckDB, 100000 rows, "
              + rowsPerStatement
              + " row(s) per statement: "
              + millis
              + " ms");
    }
  }
}

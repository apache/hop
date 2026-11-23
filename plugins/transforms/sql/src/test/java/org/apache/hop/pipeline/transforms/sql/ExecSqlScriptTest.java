/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.sql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the Execute SQL script transform against an in-memory H2 database: text replacement of
 * {@code ?} markers, bind parameters, execute once versus per row, the counter fields and variable
 * substitution.
 */
class ExecSqlScriptTest {
  private static final String CONNECTION = "exec-sql-test";

  private DatabaseMeta databaseMeta;
  private Database db;

  @TempDir Path tempDir;

  @BeforeAll
  static void initEnvironment() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    databaseMeta =
        new DatabaseMeta(
            CONNECTION,
            "H2",
            "Native",
            "",
            "mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
            "",
            "",
            "");
    db = new Database(new LoggingObject("exec-sql-test"), new Variables(), databaseMeta);
    db.connect();
    db.execStatement("CREATE TABLE t (id INTEGER, name VARCHAR(50))");
  }

  @AfterEach
  void tearDown() throws Exception {
    db.execStatement("DROP ALL OBJECTS");
    db.disconnect();
  }

  // ---------------------------------------------------------------------------------------------
  // Harness
  // ---------------------------------------------------------------------------------------------

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  private static ExecSqlMeta meta(String sql, boolean params, String... arguments) {
    ExecSqlMeta meta = new ExecSqlMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setSql(sql);
    meta.setSingleStatement(true);
    meta.setExecutedEachInputRow(true);
    meta.setParams(params);
    for (String argument : arguments) {
      meta.getArguments().add(new ExecSqlArgumentItem(argument));
    }
    return meta;
  }

  /** Runs the transform over the given input rows and returns the rows it wrote. */
  private List<Object[]> run(ExecSqlMeta meta, Object[]... rows) throws HopException {
    return run(meta, transform -> {}, rows);
  }

  private List<Object[]> run(ExecSqlMeta meta, Consumer<ExecSql> beforeInit, Object[]... rows)
      throws HopException {
    IRowMeta rowMeta = inputRowMeta();
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);
    TransformMeta transformMeta = new TransformMeta("exec sql", meta);
    pipelineMeta.addTransform(transformMeta);

    List<Object[]> input = new ArrayList<>(List.of(rows));
    List<Object[]> output = new ArrayList<>();
    ExecSql transform =
        new ExecSql(
            transformMeta, meta, new ExecSqlData(), 0, pipelineMeta, new LocalPipelineEngine()) {
          @Override
          public Object[] getRow() {
            return input.isEmpty() ? null : input.remove(0);
          }

          @Override
          public IRowMeta getInputRowMeta() {
            return rowMeta;
          }

          @Override
          public void putRow(IRowMeta outputRowMeta, Object[] row) {
            output.add(row);
          }
        };
    beforeInit.accept(transform);
    assertTrue(transform.init(), "init");
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }
    return output;
  }

  private List<String> rows() throws Exception {
    List<String> result = new ArrayList<>();
    try (Statement statement = db.getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT id, name FROM t ORDER BY id, name")) {
      while (resultSet.next()) {
        result.add(resultSet.getObject(1) + "|" + resultSet.getString(2));
      }
    }
    return result;
  }

  // ---------------------------------------------------------------------------------------------
  // Text replacement (parameters are not bound): each '?' is replaced by an argument value
  // ---------------------------------------------------------------------------------------------

  @Test
  void textReplacementSubstitutesEachMarkerWithItsArgumentPerRow() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, ?)", false, "id", "name");
    meta.setQuoteString(true);

    List<Object[]> output = run(meta, new Object[] {1L, "one"}, new Object[] {2L, "two"});

    assertEquals(List.of("1|one", "2|two"), rows());
    assertEquals(2, output.size());
  }

  @Test
  void textReplacementMapsMarkersToArgumentsInOrder() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (name, id) VALUES (?, ?)", false, "name", "id");
    meta.setQuoteString(true);

    run(meta, new Object[] {7L, "seven"});

    assertEquals(List.of("7|seven"), rows());
  }

  @Test
  void textReplacementWithoutQuotingInsertsTheRawStringAsSql() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, ?)", false, "id", "name");
    meta.setQuoteString(false);

    run(meta, new Object[] {3L, "UPPER('abc')"});

    assertEquals(List.of("3|ABC"), rows());
  }

  @Test
  void textReplacementWithQuotingEscapesEmbeddedQuotes() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, ?)", false, "id", "name");
    meta.setQuoteString(true);

    run(meta, new Object[] {4L, "O'Brien"});

    assertEquals(List.of("4|O'Brien"), rows());
  }

  @Test
  void textReplacementWithQuotingDoesNotQuoteNumbers() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (? + 1, 'n')", false, "id");
    meta.setQuoteString(true);

    run(meta, new Object[] {4L, "ignored"});

    assertEquals(List.of("5|n"), rows());
  }

  @Test
  void textReplacementReplacesANullValueWithAnEmptyString() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, ?)", false, "id", "name");
    meta.setQuoteString(true);

    run(meta, new Object[] {5L, null});

    assertEquals(List.of("5|"), rows());
  }

  @Test
  void textReplacementWithMoreArgumentsThanMarkersUsesTheFirstArguments() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, 'fixed')", false, "id", "name");

    run(meta, new Object[] {6L, "unused"});

    assertEquals(List.of("6|fixed"), rows());
  }

  @Test
  void textReplacementWithoutMarkersRunsTheScriptOncePerRow() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (1, 'same')", false);

    run(meta, new Object[] {1L, "a"}, new Object[] {2L, "b"}, new Object[] {3L, "c"});

    assertEquals(List.of("1|same", "1|same", "1|same"), rows());
  }

  @Test
  void textReplacementWithAnUnknownArgumentFieldFails() {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, 'x')", false, "missing");

    HopTransformException e =
        assertThrows(HopTransformException.class, () -> run(meta, new Object[] {1L, "a"}));
    assertTrue(e.getMessage().contains("missing"), e.getMessage());
  }

  @Test
  void textReplacementRunsEveryStatementOfAScript() throws Exception {
    ExecSqlMeta meta =
        meta(
            "INSERT INTO t (id, name) VALUES (?, 'first');\n"
                + "INSERT INTO t (id, name) VALUES (?, 'second');",
            false,
            "id",
            "id");
    meta.setSingleStatement(false);

    run(meta, new Object[] {8L, "x"});

    assertEquals(List.of("8|first", "8|second"), rows());
  }

  // ---------------------------------------------------------------------------------------------
  // #9676: a '?' in a literal, quoted identifier or comment is not a marker
  // ---------------------------------------------------------------------------------------------

  /** #9676: the '?' inside the string literal is left alone; the single argument fills the id. */
  @Test
  void textReplacementLeavesAQuestionMarkInALiteralAlone() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, 'what?')", false, "id");

    run(meta, new Object[] {1L, "unused"});

    assertEquals(List.of("1|what?"), rows());
  }

  /** #9676: an extra argument no longer replaces the '?' inside the literal. */
  @Test
  void textReplacementDoesNotReplaceAQuestionMarkInALiteral() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, 'what?')", false, "id", "name");
    meta.setQuoteString(false);

    run(meta, new Object[] {1L, "x"});

    assertEquals(List.of("1|what?"), rows());
  }

  /** #9676: a literal '?' runs as is when the script has no arguments at all. */
  @Test
  void textReplacementRunsALiteralQuestionMarkWithoutArguments() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (1, 'it''s ? or ?')", false);

    run(meta, new Object[] {1L, "unused"});

    assertEquals(List.of("1|it's ? or ?"), rows());
  }

  /** #9676: a '?' inside a line comment is not a marker. */
  @Test
  void textReplacementIgnoresAQuestionMarkInAComment() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, 'a') -- why?", false, "id");

    run(meta, new Object[] {1L, "unused"});

    assertEquals(List.of("1|a"), rows());
  }

  /** #9676: a '?' inside a quoted identifier or block comment is not a marker. */
  @Test
  void textReplacementIgnoresAQuestionMarkInAQuotedIdentifier() throws Exception {
    db.execStatement("CREATE TABLE \"q?\" (id INTEGER)");
    ExecSqlMeta meta = meta("INSERT INTO \"q?\" (id) VALUES (?) /* or? */", false, "id");

    run(meta, new Object[] {11L, "unused"});

    try (Statement statement = db.getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT id FROM \"q?\"")) {
      assertTrue(resultSet.next());
      assertEquals(11, resultSet.getInt(1));
      assertFalse(resultSet.next());
    }
  }

  /** #9676: markers between literals keep their argument order. */
  @Test
  void textReplacementMapsMarkersAroundLiteralsToArgumentsInOrder() throws Exception {
    ExecSqlMeta meta =
        meta(
            "INSERT INTO t (id, name) /* ? */ VALUES (?, CONCAT('?', ?, '?'))",
            false,
            "id",
            "name");
    meta.setQuoteString(true);

    run(meta, new Object[] {2L, "mid"});

    assertEquals(List.of("2|?mid?"), rows());
  }

  /** More markers than arguments fails with a message that names the counts. */
  @Test
  void textReplacementWithMoreMarkersThanArgumentsFailsWithAClearMessage() {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, ?) -- ok?", false, "id");

    HopTransformException e =
        assertThrows(HopTransformException.class, () -> run(meta, new Object[] {1L, "unused"}));
    assertTrue(e.getMessage().contains("2 '?' parameter markers"), e.getMessage());
    assertTrue(e.getMessage().contains("only 1 arguments"), e.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // Bind parameters
  // ---------------------------------------------------------------------------------------------

  @Test
  void bindParametersBindsArgumentsInOrder() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (name, id) VALUES (?, ?)", true, "name", "id");

    run(meta, new Object[] {1L, "O'Brien"}, new Object[] {2L, null});

    assertEquals(List.of("1|O'Brien", "2|null"), rows());
  }

  @Test
  void bindParametersLeaveAQuestionMarkInALiteralAlone() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (?, 'what?')", true, "id");

    run(meta, new Object[] {1L, "unused"});

    assertEquals(List.of("1|what?"), rows());
  }

  @Test
  void bindParametersWithoutArgumentsRunALiteralQuestionMark() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (1, 'what?')", true);

    run(meta, new Object[] {1L, "unused"});

    assertEquals(List.of("1|what?"), rows());
  }

  // ---------------------------------------------------------------------------------------------
  // Counter fields
  // ---------------------------------------------------------------------------------------------

  @Test
  void eachRowAppendsTheCountersInUpdateInsertDeleteReadOrder() throws Exception {
    db.execStatement("INSERT INTO t (id, name) VALUES (1, 'a')");
    db.execStatement("INSERT INTO t (id, name) VALUES (1, 'b')");
    db.execStatement("INSERT INTO t (id, name) VALUES (2, 'c')");

    ExecSqlMeta update = meta("UPDATE t SET name = 'u' WHERE id = ?", true, "id");
    update.setUpdateField("upd");
    update.setInsertField("ins");
    update.setDeleteField("del");
    update.setReadField("read");
    List<Object[]> output = run(update, new Object[] {1L, "x"});
    assertEquals(1, output.size());
    assertArrayEquals(new Object[] {1L, "x", 2L, 0L, 0L, 0L}, trimmed(output.get(0), 6));

    ExecSqlMeta insert = meta("INSERT INTO t (id, name) VALUES (?, ?)", true, "id", "name");
    insert.setUpdateField("upd");
    insert.setInsertField("ins");
    insert.setDeleteField("del");
    output = run(insert, new Object[] {9L, "new"});
    assertArrayEquals(new Object[] {9L, "new", 0L, 1L, 0L}, trimmed(output.get(0), 5));

    ExecSqlMeta delete = meta("DELETE FROM t WHERE id = ?", false, "id");
    delete.setDeleteField("del");
    output = run(delete, new Object[] {1L, "x"});
    assertArrayEquals(new Object[] {1L, "x", 2L}, trimmed(output.get(0), 3));

    assertEquals(List.of("2|c", "9|new"), rows());
  }

  @Test
  void executeOnceRunsTheScriptAtInitAndWritesOneCounterRow() throws Exception {
    ExecSqlMeta meta =
        meta(
            "INSERT INTO t (id, name) VALUES (1, 'a');\n"
                + "INSERT INTO t (id, name) VALUES (2, 'b');\n"
                + "UPDATE t SET name = 'z' WHERE id = 1;\n"
                + "DELETE FROM t WHERE id = 2;",
            false);
    meta.setExecutedEachInputRow(false);
    meta.setSingleStatement(false);
    meta.setUpdateField("upd");
    meta.setInsertField("ins");
    meta.setDeleteField("del");

    List<Object[]> output = run(meta, new Object[] {99L, "never read"});

    assertEquals(1, output.size());
    assertArrayEquals(new Object[] {1L, 2L, 1L}, trimmed(output.get(0), 3));
    assertEquals(List.of("1|z"), rows());
  }

  @Test
  void executeOnceLeavesQuestionMarksToTheDatabase() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO t (id, name) VALUES (1, 'what?')", false);
    meta.setExecutedEachInputRow(false);

    run(meta);

    assertEquals(List.of("1|what?"), rows());
  }

  // ---------------------------------------------------------------------------------------------
  // Variables and SQL from a file
  // ---------------------------------------------------------------------------------------------

  @Test
  void variablesAreResolvedBeforeMarkersAreReplaced() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO ${TABLE_NAME} (id, name) VALUES (?, ?)", false, "id");
    meta.getArguments().add(new ExecSqlArgumentItem("name"));
    meta.setReplaceVariables(true);
    meta.setQuoteString(true);

    run(meta, transform -> transform.setVariable("TABLE_NAME", "t"), new Object[] {1L, "v"});

    assertEquals(List.of("1|v"), rows());
  }

  @Test
  void variablesAreLeftAloneWhenReplacementIsOff() throws Exception {
    ExecSqlMeta meta = meta("INSERT INTO ${TABLE_NAME} (id, name) VALUES (?, 'n')", false, "id");
    meta.setReplaceVariables(false);

    assertThrows(
        HopTransformException.class,
        () ->
            run(
                meta,
                transform -> transform.setVariable("TABLE_NAME", "t"),
                new Object[] {1L, "v"}));
    assertDoesNotContainRows();
  }

  @Test
  void sqlFromAFileIsUsedWithMarkers() throws Exception {
    Path script = tempDir.resolve("insert.sql");
    Files.writeString(
        script, "INSERT INTO t (id, name) VALUES (?, 'from file')", StandardCharsets.UTF_8);
    ExecSqlMeta meta = meta("SELECT 1", false, "id");
    meta.setSqlFromFile(script.toString());

    run(meta, new Object[] {12L, "x"});

    assertEquals(List.of("12|from file"), rows());
  }

  private void assertDoesNotContainRows() throws Exception {
    assertFalse(rows().iterator().hasNext());
  }

  private static Object[] trimmed(Object[] row, int size) {
    Object[] result = new Object[size];
    System.arraycopy(row, 0, result, 0, size);
    return result;
  }
}

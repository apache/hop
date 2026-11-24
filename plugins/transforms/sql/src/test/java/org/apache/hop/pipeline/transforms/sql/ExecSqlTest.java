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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.DbCacheEntry;
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
import org.apache.hop.core.variables.IVariables;
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
import org.opentest4j.AssertionFailedError;

/**
 * Behaviour of the Execute SQL script transform against an in-memory H2 database, per mode:
 * executed once or for each row, bind parameters or "?" text replacement, single statement or
 * script, and the row counts it reports in the result fields and the transform metrics.
 */
class ExecSqlTest {

  private static final String CONNECTION_NAME = "exec-sql-test";

  private DatabaseMeta databaseMeta;
  private Database setupDb;

  /** Wraps the transform's connection after init(), on top of the counting proxy. */
  private UnaryOperator<Connection> connectionWrapper;

  /** Called for every row the transform passes on. */
  private Consumer<Object[]> onPutRow;

  /** Stop the transform, as a pipeline stop would, once this many rows were read. */
  private int stopAfterRows = -1;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(SavepointH2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    connectionWrapper = null;
    onPutRow = null;
    stopAfterRows = -1;
    connect("");
  }

  private void connect(String urlOptions) throws Exception {
    connect("H2", urlOptions);
  }

  private void connect(String databaseType, String urlOptions) throws Exception {
    connectTo(databaseType, "mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1" + urlOptions);
  }

  private void connectTo(String databaseType, String databaseName) throws Exception {
    if (setupDb != null) {
      tearDown();
    }
    databaseMeta =
        new DatabaseMeta(CONNECTION_NAME, databaseType, "Native", "", databaseName, "", "", "");
    setupDb = new Database(new LoggingObject("ExecSqlTest"), new Variables(), databaseMeta);
    setupDb.connect();
    setupDb.execStatements(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(50), v INTEGER);"
            + "CREATE TABLE counter (n INTEGER);"
            + "INSERT INTO counter VALUES (0);");
  }

  @AfterEach
  void tearDown() throws Exception {
    setupDb.execStatement("DROP ALL OBJECTS");
    setupDb.disconnect();
    setupDb = null;
  }

  // ------------------------------------------------------------------------------------------
  // Harness
  // ------------------------------------------------------------------------------------------

  /** Counts the statements the transform asks the JDBC connection for. */
  static final class CountingConnection {
    final AtomicInteger prepareCalls = new AtomicInteger();
    final AtomicInteger createCalls = new AtomicInteger();
    final AtomicInteger commitCalls = new AtomicInteger();
    final AtomicInteger rollbackCalls = new AtomicInteger();
    final AtomicInteger savepointCalls = new AtomicInteger();
    final List<Statement> statements = new ArrayList<>();

    Connection wrap(Connection target) {
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if (method.getName().equals("prepareStatement")) {
                  prepareCalls.incrementAndGet();
                } else if (method.getName().equals("createStatement")) {
                  createCalls.incrementAndGet();
                } else if (method.getName().equals("commit")) {
                  commitCalls.incrementAndGet();
                } else if (method.getName().equals("rollback")) {
                  rollbackCalls.incrementAndGet();
                } else if (method.getName().equals("setSavepoint")) {
                  savepointCalls.incrementAndGet();
                }
                try {
                  Object result = method.invoke(target, args);
                  if (result instanceof Statement statement) {
                    statements.add(statement);
                  }
                  return result;
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
              });
    }

    long openStatements() throws SQLException {
      long open = 0;
      for (Statement statement : statements) {
        if (!statement.isClosed()) {
          open++;
        }
      }
      return open;
    }
  }

  /** What a run of the transform produced. */
  static final class Run {
    ExecSql transform;
    PipelineMeta pipelineMeta;
    final List<Object[]> output = new ArrayList<>();
    final List<Object[]> errors = new ArrayList<>();
    final List<String> errorDescriptions = new ArrayList<>();
    final CountingConnection counter = new CountingConnection();
    long openStatementsBeforeDispose;
    boolean autoCommitBeforeDispose;
  }

  private static ExecSqlMeta meta(String sql) {
    ExecSqlMeta meta = new ExecSqlMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION_NAME);
    meta.setSql(sql);
    meta.setSingleStatement(true);
    meta.setInsertField("ins");
    meta.setUpdateField("upd");
    meta.setDeleteField("del");
    meta.setReadField("rd");
    return meta;
  }

  private static ExecSqlMeta eachRow(String sql, boolean params, String... arguments) {
    ExecSqlMeta meta = meta(sql);
    meta.setExecutedEachInputRow(true);
    meta.setParams(params);
    for (String argument : arguments) {
      meta.getArguments().add(new ExecSqlArgumentItem(argument));
    }
    return meta;
  }

  private static IRowMeta idNameRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  private Run run(ExecSqlMeta meta, IRowMeta inputRowMeta, List<Object[]> rows) throws Exception {
    return run(meta, inputRowMeta, rows, false, null);
  }

  private Run run(
      ExecSqlMeta meta,
      IRowMeta inputRowMeta,
      List<Object[]> rows,
      boolean errorHandling,
      IVariables extraVariables)
      throws Exception {
    Run run = new Run();

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);
    TransformMeta transformMeta = spy(new TransformMeta("exec sql", meta));
    pipelineMeta.addTransform(transformMeta);
    run.pipelineMeta = spy(pipelineMeta);
    doReturn(errorHandling).when(transformMeta).isDoingErrorHandling();

    ExecSqlData data = new ExecSqlData();
    List<Object[]> input = new ArrayList<>(rows);
    input.add(null);
    AtomicInteger next = new AtomicInteger();
    // A subclass rather than a Mockito spy: the per-row calls stay cheap enough to time.
    ExecSql transform =
        new ExecSql(
            transformMeta, meta, data, 0, run.pipelineMeta, spy(new LocalPipelineEngine())) {
          @Override
          public Object[] getRow() {
            if (stopAfterRows >= 0 && next.get() == stopAfterRows) {
              setStopped(true);
            }
            return input.get(next.getAndIncrement());
          }

          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) {
            if (onPutRow != null) {
              onPutRow.accept(row);
            }
            run.output.add(row);
          }

          @Override
          public void putError(
              IRowMeta rowMeta,
              Object[] row,
              long nrErrors,
              String errorDescriptions,
              String fieldNames,
              String errorCodes) {
            run.errors.add(row);
            run.errorDescriptions.add(errorDescriptions);
          }
        };
    run.transform = transform;
    if (extraVariables != null) {
      for (String name : extraVariables.getVariableNames()) {
        transform.setVariable(name, extraVariables.getVariable(name));
      }
    }

    // Execute-once runs its SQL in init(), before a proxy could be put in place.
    if (!meta.isExecutedEachInputRow()) {
      assertTrue(transform.init(), "init failed");
    } else {
      assertTrue(transform.init(), "init failed");
      Connection connection = data.db.getConnection();
      if (connectionWrapper != null) {
        connection = connectionWrapper.apply(connection);
      }
      data.db.setConnection(run.counter.wrap(connection));
    }
    try {
      // As the pipeline's RunThread: a stopped transform gets no more processRow() calls.
      while (transform.processRow() && !transform.isStopped()) {
        // keep going
      }
    } finally {
      run.openStatementsBeforeDispose = run.counter.openStatements();
      run.autoCommitBeforeDispose = data.db.getConnection().getAutoCommit();
      transform.dispose();
    }
    return run;
  }

  private List<Object[]> query(String sql) throws Exception {
    List<Object[]> result = new ArrayList<>();
    try (Statement statement = setupDb.getConnection().createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      int columns = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        Object[] row = new Object[columns];
        for (int i = 0; i < columns; i++) {
          row[i] = rs.getObject(i + 1);
        }
        result.add(row);
      }
    }
    return result;
  }

  /** The four result fields (upd, ins, del, rd) appended after the input fields. */
  private static Object[] counts(Object[] outputRow, int inputSize) {
    return Arrays.copyOfRange(outputRow, inputSize, inputSize + 4);
  }

  private static Object[] counts(long upd, long ins, long del, long rd) {
    return new Object[] {upd, ins, del, rd};
  }

  // ------------------------------------------------------------------------------------------
  // Execute once
  // ------------------------------------------------------------------------------------------

  @Test
  void executeOnceSingleStatementReportsCountsInOneRow() throws Exception {
    setupDb.execStatements("INSERT INTO t VALUES (1,'a',1); INSERT INTO t VALUES (2,'b',1);");
    Run run = run(meta("UPDATE t SET v = 5"), new RowMeta(), List.of());

    assertEquals(1, run.output.size());
    assertArrayEquals(counts(2, 0, 0, 0), counts(run.output.get(0), 0));
    assertEquals(2, run.transform.getLinesUpdated());
    assertEquals(
        List.of(5, 5), query("SELECT v FROM t ORDER BY id").stream().map(r -> r[0]).toList());
  }

  @Test
  void executeOnceScriptAddsUpCountsOfEveryStatement() throws Exception {
    ExecSqlMeta meta =
        meta(
            "INSERT INTO t VALUES (1,'a',1);\n"
                + "INSERT INTO t VALUES (2,'b',1);\n"
                + "INSERT INTO t VALUES (3,'c',1);\n"
                + "UPDATE t SET v = 2 WHERE id < 3;\n"
                + "DELETE FROM t WHERE id = 3;\n"
                + "SELECT * FROM t;\n");
    meta.setSingleStatement(false);
    Run run = run(meta, new RowMeta(), List.of());

    assertEquals(1, run.output.size());
    assertArrayEquals(counts(2, 3, 1, 2), counts(run.output.get(0), 0));
    assertEquals(3, run.transform.getLinesOutput());
    assertEquals(3, run.transform.getLinesUpdated()); // updated + deleted
    assertEquals(2, run.transform.getLinesInput());
  }

  // ------------------------------------------------------------------------------------------
  // For each row, bind parameters
  // ------------------------------------------------------------------------------------------

  @Test
  void eachRowBindParametersInsertsBoundValues() throws Exception {
    Run run =
        run(
            eachRow("INSERT INTO t (id, name) VALUES (?, ?)", true, "id", "name"),
            idNameRowMeta(),
            List.of(
                new Object[] {1L, "it's"}, new Object[] {2L, null}, new Object[] {3L, "?three"}));

    assertEquals(3, run.output.size());
    for (Object[] row : run.output) {
      assertArrayEquals(counts(0, 1, 0, 0), counts(row, 2));
    }
    assertArrayEquals(new Object[] {1L, "it's"}, Arrays.copyOf(run.output.get(0), 2));
    assertEquals(3, run.transform.getLinesOutput());
    List<Object[]> stored = query("SELECT id, name FROM t ORDER BY id");
    assertArrayEquals(new Object[] {1, "it's"}, stored.get(0));
    assertArrayEquals(new Object[] {2, null}, stored.get(1));
    assertArrayEquals(new Object[] {3, "?three"}, stored.get(2));
  }

  @Test
  void eachRowBindParametersReportTheCountOfEachRowNotTheLastOne() throws Exception {
    setupDb.execStatements(
        "INSERT INTO t VALUES (1,'a',0); INSERT INTO t VALUES (2,'a',0);"
            + " INSERT INTO t VALUES (3,'b',0);");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    Run run =
        run(
            eachRow("UPDATE t SET v = v + 1 WHERE name = ?", true, "name"),
            rowMeta,
            List.of(new Object[] {"a"}, new Object[] {"zz"}, new Object[] {"b"}));

    assertEquals(3, run.output.size());
    assertArrayEquals(counts(2, 0, 0, 0), counts(run.output.get(0), 1));
    assertArrayEquals(counts(0, 0, 0, 0), counts(run.output.get(1), 1));
    assertArrayEquals(counts(1, 0, 0, 0), counts(run.output.get(2), 1));
    assertEquals(3, run.transform.getLinesUpdated());
  }

  @Test
  void eachRowBindParametersMergeCountsAsUpdated() throws Exception {
    setupDb.execStatement("INSERT INTO t VALUES (1,'a',0)");
    Run run =
        run(
            eachRow(
                "MERGE INTO t AS tgt USING (SELECT CAST(? AS INTEGER) AS id, CAST(? AS VARCHAR(50))"
                    + " AS name) AS src ON tgt.id = src.id"
                    + " WHEN MATCHED THEN UPDATE SET name = src.name"
                    + " WHEN NOT MATCHED THEN INSERT (id, name) VALUES (src.id, src.name)",
                true,
                "id",
                "name"),
            idNameRowMeta(),
            List.of(new Object[] {1L, "x"}, new Object[] {2L, "y"}));

    assertArrayEquals(counts(1, 0, 0, 0), counts(run.output.get(0), 2));
    assertArrayEquals(counts(1, 0, 0, 0), counts(run.output.get(1), 2));
    assertEquals(2, run.transform.getLinesUpdated());
    assertEquals(2, query("SELECT * FROM t").size());
  }

  @Test
  void eachRowBindParametersDeleteCountsAsDeleted() throws Exception {
    setupDb.execStatements("INSERT INTO t VALUES (1,'a',0); INSERT INTO t VALUES (2,'b',0);");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    Run run =
        run(
            eachRow("-- remove one row\nDELETE FROM t WHERE id = ?", true, "id"),
            rowMeta,
            List.of(new Object[] {1L}, new Object[] {9L}, new Object[] {2L}));

    assertArrayEquals(counts(0, 0, 1, 0), counts(run.output.get(0), 1));
    assertArrayEquals(counts(0, 0, 0, 0), counts(run.output.get(1), 1));
    assertArrayEquals(counts(0, 0, 1, 0), counts(run.output.get(2), 1));
    assertEquals(2, run.transform.getLinesUpdated());
    assertEquals(0, query("SELECT * FROM t").size());
  }

  @Test
  void eachRowSingleSelectReportsNoCounts() throws Exception {
    setupDb.execStatement("INSERT INTO t VALUES (1,'a',0)");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    Run run =
        run(
            eachRow("SELECT * FROM t WHERE id = ?", true, "id"),
            rowMeta,
            List.of(new Object[] {1L}, new Object[] {1L}));

    assertEquals(2, run.output.size());
    assertArrayEquals(counts(0, 0, 0, 0), counts(run.output.get(0), 1));
    assertEquals(0, run.transform.getLinesInput());
  }

  @Test
  void eachRowVariablesAreResolvedOnceBeforeBinding() throws Exception {
    ExecSqlMeta meta = eachRow("INSERT INTO ${TABLE} (id, name) VALUES (?, ?)", true, "id", "name");
    meta.setReplaceVariables(true);
    Variables variables = new Variables();
    variables.setVariable("TABLE", "t");
    Run run =
        run(
            meta,
            idNameRowMeta(),
            List.of(new Object[] {1L, "a"}, new Object[] {2L, "b"}),
            false,
            variables);

    assertEquals(2, run.output.size());
    assertEquals(2, query("SELECT * FROM t").size());
  }

  @Test
  void eachRowBindParametersScriptRunsEveryStatementWithTheSameParameters() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    ExecSqlMeta meta =
        eachRow(
            "INSERT INTO t (id, name) VALUES (?, 'a');\nUPDATE t SET name = 'b' WHERE id = ?;",
            true,
            "id");
    meta.setSingleStatement(false);
    Run run = run(meta, rowMeta, List.of(new Object[] {1L}, new Object[] {2L}));

    assertArrayEquals(counts(1, 1, 0, 0), counts(run.output.get(0), 1));
    assertArrayEquals(counts(1, 1, 0, 0), counts(run.output.get(1), 1));
    assertEquals(
        List.of("b", "b"),
        query("SELECT name FROM t ORDER BY id").stream().map(r -> r[0]).toList());
  }

  // ------------------------------------------------------------------------------------------
  // For each row, "?" replaced by the field values in the SQL text
  // ------------------------------------------------------------------------------------------

  @Test
  void eachRowTextReplacementQuotesStrings() throws Exception {
    ExecSqlMeta meta = eachRow("INSERT INTO t (id, name) VALUES (?, ?)", false, "id", "name");
    meta.setQuoteString(true);
    Run run =
        run(
            meta,
            idNameRowMeta(),
            List.of(new Object[] {1L, "O'Brien"}, new Object[] {2L, "plain"}));

    assertEquals(2, run.output.size());
    assertArrayEquals(counts(0, 1, 0, 0), counts(run.output.get(0), 2));
    assertEquals(
        List.of("O'Brien", "plain"),
        query("SELECT name FROM t ORDER BY id").stream().map(r -> r[0]).toList());
  }

  @Test
  void eachRowTextReplacementWithoutQuotingInsertsRawText() throws Exception {
    setupDb.execStatements("INSERT INTO t VALUES (1,'a',10); INSERT INTO t VALUES (2,'b',20);");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("delta"));
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    Run run =
        run(
            eachRow("UPDATE t SET v = v + ? WHERE id = ?", false, "delta", "id"),
            rowMeta,
            List.of(new Object[] {5L, 1L}, new Object[] {5L, 1L}, new Object[] {7L, 2L}));

    assertArrayEquals(counts(1, 0, 0, 0), counts(run.output.get(2), 2));
    assertEquals(3, run.transform.getLinesUpdated());
    assertEquals(
        List.of(20, 27), query("SELECT v FROM t ORDER BY id").stream().map(r -> r[0]).toList());
  }

  @Test
  void eachRowConstantSqlRunsOncePerRow() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    List<Object[]> rows = new ArrayList<>();
    for (long i = 0; i < 5; i++) {
      rows.add(new Object[] {i});
    }
    Run run = run(eachRow("UPDATE counter SET n = n + 1", false), rowMeta, rows);

    assertEquals(5, run.output.size());
    for (Object[] row : run.output) {
      assertArrayEquals(counts(1, 0, 0, 0), counts(row, 1));
    }
    assertEquals(5, query("SELECT n FROM counter").get(0)[0]);
  }

  @Test
  void eachRowDdlClearsTheDatabaseCache() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    DbCacheEntry entry = new DbCacheEntry(CONNECTION_NAME, "SELECT * FROM t");
    DbCache.getInstance().put(entry, new RowMeta());

    Run run =
        run(
            eachRow("CREATE TABLE IF NOT EXISTS extra (id INTEGER)", false),
            rowMeta,
            List.of(new Object[] {1L}, new Object[] {2L}, new Object[] {3L}));

    assertEquals(3, run.output.size());
    assertNull(DbCache.getInstance().get(entry));
    assertEquals(0, query("SELECT * FROM extra").size());
  }

  @Test
  void eachRowTextReplacementDdlCreatesOneTablePerRow() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    run(
        eachRow("CREATE TABLE tab_? (id INTEGER)", false, "id"),
        rowMeta,
        List.of(new Object[] {1L}, new Object[] {2L}));

    assertEquals(0, query("SELECT * FROM tab_1").size());
    assertEquals(0, query("SELECT * FROM tab_2").size());
  }

  // ------------------------------------------------------------------------------------------
  // Errors
  // ------------------------------------------------------------------------------------------

  @Test
  void eachRowErrorGoesToTheErrorHopAndLaterRowsStillRun() throws Exception {
    setupDb.execStatement("INSERT INTO t VALUES (2,'existing',0)");
    Run run =
        run(
            eachRow("INSERT INTO t (id, name) VALUES (?, ?)", true, "id", "name"),
            idNameRowMeta(),
            List.of(
                new Object[] {1L, "a"},
                new Object[] {2L, "duplicate"},
                new Object[] {3L, "c"},
                new Object[] {3L, "duplicate"},
                new Object[] {4L, "d"}),
            true,
            null);

    assertEquals(3, run.output.size());
    assertEquals(2, run.errors.size());
    assertArrayEquals(new Object[] {2L, "duplicate"}, run.errors.get(0));
    assertTrue(
        run.errorDescriptions.get(0).contains("Couldn't execute SQL"),
        run.errorDescriptions.get(0));
    assertEquals(4, query("SELECT * FROM t").size());
    assertEquals(3, run.transform.getLinesOutput());
  }

  @Test
  void eachRowTextReplacementErrorGoesToTheErrorHop() throws Exception {
    setupDb.execStatement("INSERT INTO t VALUES (2,'existing',0)");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    Run run =
        run(
            eachRow("INSERT INTO t (id) VALUES (?)", false, "id"),
            rowMeta,
            List.of(new Object[] {2L}, new Object[] {2L}, new Object[] {3L}),
            true,
            null);

    assertEquals(1, run.output.size());
    assertEquals(2, run.errors.size());
  }

  @Test
  void eachRowErrorWithoutErrorHandlingFailsTheTransform() throws Exception {
    setupDb.execStatement("INSERT INTO t VALUES (1,'existing',0)");
    HopTransformException e =
        assertThrows(
            HopTransformException.class,
            () ->
                run(
                    eachRow("INSERT INTO t (id, name) VALUES (?, ?)", true, "id", "name"),
                    idNameRowMeta(),
                    List.<Object[]>of(new Object[] {1L, "duplicate"})));
    assertTrue(e.getMessage().contains("Couldn't execute SQL"), e.getMessage());
  }

  @Test
  void unknownArgumentFieldFailsTheTransform() {
    assertThrows(
        HopException.class,
        () ->
            run(
                eachRow("INSERT INTO t (id) VALUES (?)", true, "missing"),
                idNameRowMeta(),
                List.<Object[]>of(new Object[] {1L, "a"})));
  }

  // ------------------------------------------------------------------------------------------
  // Edge cases, as the transform behaves today
  // ------------------------------------------------------------------------------------------

  private static IRowMeta idRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    return rowMeta;
  }

  @Test
  void eachRowWithoutInputRowsRunsNothing() throws Exception {
    Run run = run(eachRow("UPDATE counter SET n = n + 1", false), idRowMeta(), List.of());

    assertEquals(0, run.output.size());
    assertEquals(0, query("SELECT n FROM counter").get(0)[0]);
  }

  @Test
  void eachRowTextReplacementQuotesANullStringAsAnEmptyString() throws Exception {
    ExecSqlMeta meta = eachRow("INSERT INTO t (id, name) VALUES (?, ?)", false, "id", "name");
    meta.setQuoteString(true);

    Run run = run(meta, idNameRowMeta(), List.<Object[]>of(new Object[] {1L, null}));

    assertEquals(1, run.output.size());
    assertEquals("", query("SELECT name FROM t WHERE id = 1").get(0)[0]);
  }

  /**
   * A question mark inside a string literal is not a marker (#9676): the single argument fills the
   * id and the literal is stored as it is.
   */
  @Test
  void eachRowTextReplacementLeavesAQuestionMarkInAStringLiteralAlone() throws Exception {
    Run run =
        run(
            eachRow("INSERT INTO t (id, name) VALUES (?, 'why?')", false, "id"),
            idRowMeta(),
            List.<Object[]>of(new Object[] {1L}));

    assertEquals(1, run.output.size());
    assertEquals("why?", query("SELECT name FROM t WHERE id = 1").get(0)[0]);
  }

  @Test
  void eachRowDeletedRowsAreAlsoCountedAsLinesUpdated() throws Exception {
    setupDb.execStatements("INSERT INTO t VALUES (1,'a',1); INSERT INTO t VALUES (2,'b',1);");

    Run run =
        run(
            eachRow("DELETE FROM t WHERE id = ?", true, "id"),
            idRowMeta(),
            List.<Object[]>of(new Object[] {1L}, new Object[] {2L}));

    assertArrayEquals(counts(0, 0, 1, 0), counts(run.output.get(0), 1));
    assertEquals(2, run.transform.getLinesUpdated());
    assertEquals(0, run.transform.getLinesOutput());
  }

  @Test
  void executeOnceWithAFailingStatementFailsInit() {
    AssertionFailedError e =
        assertThrows(
            AssertionFailedError.class,
            () -> run(meta("INSERT INTO missing_table VALUES (1)"), new RowMeta(), List.of()));
    assertTrue(e.getMessage().startsWith("init failed"), e.getMessage());
  }

  @Test
  void eachRowOutputIsTheInputFollowedByTheFourIntegerCountFields() throws Exception {
    Run run =
        run(
            eachRow("UPDATE counter SET n = n + 1", false),
            idRowMeta(),
            List.<Object[]>of(new Object[] {1L}));

    IRowMeta outputRowMeta = ((ExecSqlData) run.transform.getData()).outputRowMeta;
    assertArrayEquals(
        new String[] {"id", "upd", "ins", "del", "rd"}, outputRowMeta.getFieldNames());
    for (int i = 1; i < outputRowMeta.size(); i++) {
      assertEquals("Integer", outputRowMeta.getValueMeta(i).getTypeDesc());
    }
    assertArrayEquals(counts(1, 0, 0, 0), counts(run.output.get(0), 1));
  }

  // ------------------------------------------------------------------------------------------
  // Statement reuse
  // ------------------------------------------------------------------------------------------

  private static List<Object[]> idNameRows(int count) {
    List<Object[]> rows = new ArrayList<>();
    for (long i = 1; i <= count; i++) {
      rows.add(new Object[] {i, "name " + i});
    }
    return rows;
  }

  @Test
  void bindParametersPrepareTheStatementOnce() throws Exception {
    Run run =
        run(
            eachRow("INSERT INTO t (id, name) VALUES (?, ?)", true, "id", "name"),
            idNameRowMeta(),
            idNameRows(50));

    assertEquals(50, query("SELECT * FROM t").size());
    assertEquals(1, run.counter.prepareCalls.get());
    assertEquals(0, run.counter.createCalls.get());
    assertEquals(1, run.openStatementsBeforeDispose);
    assertEquals(0, run.counter.openStatements(), "dispose closes the statement");
  }

  @Test
  void anErrorClosesTheStatementAndTheNextRowPreparesAgain() throws Exception {
    setupDb.execStatement("INSERT INTO t VALUES (2,'existing',0)");
    Run run =
        run(
            eachRow("INSERT INTO t (id, name) VALUES (?, ?)", true, "id", "name"),
            idNameRowMeta(),
            idNameRows(4),
            true,
            null);

    assertEquals(3, run.output.size());
    assertEquals(1, run.errors.size());
    assertEquals(2, run.counter.prepareCalls.get());
    assertEquals(0, run.counter.openStatements());
  }

  @Test
  void constantSqlWithoutParametersIsPreparedOnce() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    List<Object[]> rows = new ArrayList<>();
    for (long i = 0; i < 10; i++) {
      rows.add(new Object[] {i});
    }
    Run run = run(eachRow("UPDATE counter SET n = n + 1", false), rowMeta, rows);

    assertEquals(10, query("SELECT n FROM counter").get(0)[0]);
    // The first row runs as a plain statement, the repeat is prepared and kept.
    assertEquals(1, run.counter.createCalls.get());
    assertEquals(1, run.counter.prepareCalls.get());
    assertEquals(0, run.counter.openStatements());
  }

  @Test
  void textReplacementPreparesAgainOnlyWhenTheTextChanges() throws Exception {
    setupDb.execStatements("INSERT INTO t VALUES (1,'a',0); INSERT INTO t VALUES (2,'b',0);");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    Run run =
        run(
            eachRow("UPDATE t SET v = v + 1 WHERE id = ?", false, "id"),
            rowMeta,
            List.of(
                new Object[] {1L},
                new Object[] {1L},
                new Object[] {1L},
                new Object[] {2L},
                new Object[] {2L}));

    for (Object[] row : run.output) {
      assertArrayEquals(counts(1, 0, 0, 0), counts(row, 1));
    }
    assertEquals(
        List.of(3, 2), query("SELECT v FROM t ORDER BY id").stream().map(r -> r[0]).toList());
    assertEquals(2, run.counter.createCalls.get());
    assertEquals(2, run.counter.prepareCalls.get());
    assertEquals(0, run.counter.openStatements());
  }

  @Test
  void textReplacementDoesNotLookUpTheConnectionPerRow() throws Exception {
    ExecSqlMeta meta = eachRow("INSERT INTO t (id, name) VALUES (?, ?)", false, "id", "name");
    meta.setQuoteString(true);
    Run run = run(meta, idNameRowMeta(), idNameRows(5));

    assertEquals(5, query("SELECT * FROM t").size());
    verify(run.pipelineMeta, times(1)).findDatabase(anyString(), any(IVariables.class));
  }

  @Test
  void scriptsKeepTheirPerStatementPath() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    ExecSqlMeta meta =
        eachRow("INSERT INTO t (id) VALUES (?);\nUPDATE t SET v = 1 WHERE id = ?;", true, "id");
    meta.setSingleStatement(false);
    Run run = run(meta, rowMeta, List.of(new Object[] {1L}, new Object[] {2L}));

    assertEquals(4, run.counter.prepareCalls.get());
    assertEquals(0, run.counter.openStatements());
  }

  // ------------------------------------------------------------------------------------------
  // Commit size
  // ------------------------------------------------------------------------------------------

  private static ExecSqlMeta insertEachRow(String commitSize) {
    ExecSqlMeta meta = eachRow("INSERT INTO t (id, name) VALUES (?, ?)", true, "id", "name");
    meta.setCommitSize(commitSize);
    return meta;
  }

  private int committedRows() throws Exception {
    return ((Number) query("SELECT COUNT(*) FROM t").get(0)[0]).intValue();
  }

  @Test
  void emptyOrZeroCommitSizeKeepsAutoCommit() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("EMPTY_COMMIT_SIZE", "");
    for (String commitSize : new String[] {null, "", "0", "${EMPTY_COMMIT_SIZE}"}) {
      setupDb.execStatement("DELETE FROM t");
      List<Integer> committedAtPutRow = new ArrayList<>();
      onPutRow = row -> committedAtPutRow.add(committedRowsUnchecked());
      Run run = run(insertEachRow(commitSize), idNameRowMeta(), idNameRows(5), false, variables);

      assertTrue(run.autoCommitBeforeDispose, "auto-commit for commit size " + commitSize);
      assertEquals(0, run.counter.commitCalls.get());
      // Every row is in the database before it is passed on.
      assertEquals(List.of(1, 2, 3, 4, 5), committedAtPutRow);
    }
  }

  private int committedRowsUnchecked() {
    try {
      return committedRows();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  void commitSizeCommitsEveryNRowsAndPassesRowsOnAfterTheCommit() throws Exception {
    List<Integer> committedAtPutRow = new ArrayList<>();
    onPutRow = row -> committedAtPutRow.add(committedRowsUnchecked());
    Run run = run(insertEachRow("10"), idNameRowMeta(), idNameRows(25));

    assertFalse(run.autoCommitBeforeDispose);
    assertEquals(25, run.output.size());
    assertEquals(25, committedRows());
    List<Integer> expected = new ArrayList<>();
    for (int i = 1; i <= 25; i++) {
      expected.add(Math.min(25, ((i + 9) / 10) * 10));
    }
    assertEquals(expected, committedAtPutRow);
    assertEquals(25, run.transform.getLinesOutput());
    // The statement is still prepared once.
    assertEquals(1, run.counter.prepareCalls.get());
  }

  @Test
  void commitSizeFromAVariable() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("COMMIT", "4");
    List<Integer> committedAtPutRow = new ArrayList<>();
    onPutRow = row -> committedAtPutRow.add(committedRowsUnchecked());
    run(insertEachRow("${COMMIT}"), idNameRowMeta(), idNameRows(5), false, variables);

    assertEquals(List.of(4, 4, 4, 4, 5), committedAtPutRow);
  }

  @Test
  void invalidCommitSizeIsAnError() {
    ExecSqlMeta meta = insertEachRow("many");
    HopException e =
        assertThrows(HopException.class, () -> meta.resolveCommitSize(new Variables()));
    assertTrue(e.getMessage().contains("many"), e.getMessage());
  }

  @Test
  void commitSizeAcceptsZeroNegativeAndEmptyAsAutoCommit() throws Exception {
    for (String commitSize : new String[] {null, "", " ", "0", "-5"}) {
      assertEquals(0, insertEachRow(commitSize).resolveCommitSize(new Variables()), commitSize);
    }
    assertEquals(1000, insertEachRow("1k").resolveCommitSize(new Variables()));
  }

  @Test
  void commitSizeRollsBackTheOpenTransactionWhenARowFailsWithoutErrorHop() throws Exception {
    setupDb.execStatement("INSERT INTO t VALUES (13,'existing',0)");
    List<Object[]> rows = idNameRows(15);
    assertThrows(
        HopTransformException.class, () -> run(insertEachRow("10"), idNameRowMeta(), rows));

    // Rows 1-10 were committed, 11 and 12 rolled back with the failure of 13.
    List<Object[]> stored = query("SELECT id FROM t ORDER BY id");
    assertEquals(11, stored.size());
    assertEquals(10, stored.get(9)[0]);
    assertEquals(13, stored.get(10)[0]);
  }

  @Test
  void commitSizeOnlyPassesOnCommittedRowsWhenARowFailsWithoutErrorHop() throws Exception {
    setupDb.execStatement("INSERT INTO t VALUES (13,'existing',0)");
    List<Object[]> passedOn = new ArrayList<>();
    onPutRow = passedOn::add;
    assertThrows(
        HopTransformException.class,
        () -> run(insertEachRow("10"), idNameRowMeta(), idNameRows(15)));

    assertEquals(10, passedOn.size());
  }

  @Test
  void commitSizeKeepsTheCountsOfEachRow() throws Exception {
    setupDb.execStatements(
        "INSERT INTO t VALUES (1,'a',0); INSERT INTO t VALUES (2,'a',0);"
            + " INSERT INTO t VALUES (3,'b',0);");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    ExecSqlMeta meta = eachRow("UPDATE t SET v = v + 1 WHERE name = ?", true, "name");
    meta.setCommitSize("2");
    Run run =
        run(meta, rowMeta, List.of(new Object[] {"a"}, new Object[] {"zz"}, new Object[] {"b"}));

    assertEquals(3, run.output.size());
    assertArrayEquals(counts(2, 0, 0, 0), counts(run.output.get(0), 1));
    assertArrayEquals(counts(0, 0, 0, 0), counts(run.output.get(1), 1));
    assertArrayEquals(counts(1, 0, 0, 0), counts(run.output.get(2), 1));
    assertEquals(3, run.transform.getLinesUpdated());
  }

  @Test
  void commitSizeWithErrorHopOnADatabaseThatKeepsTheTransaction() throws Exception {
    setupDb.execStatement("INSERT INTO t VALUES (3,'existing',0)");
    Run run = run(insertEachRow("10"), idNameRowMeta(), idNameRows(5), true, null);

    assertEquals(4, run.output.size());
    assertEquals(1, run.errors.size());
    assertEquals(3L, run.errors.get(0)[0]);
    assertEquals(5, committedRows());
    // H2 does not need savepoints: a failed statement leaves the transaction usable.
    assertEquals(0, run.counter.savepointCalls.get());
  }

  /** Text that changes per row can't run on one prepared statement: it keeps the savepoints. */
  @Test
  void textReplacementOnAnAbortingDatabaseKeepsASavepointPerRow() throws Exception {
    connect("H2_SAVEPOINTS", "");
    setupDb.execStatement("INSERT INTO t VALUES (3,'existing',0)");
    AbortingConnection aborting = new AbortingConnection();
    connectionWrapper = aborting::wrap;
    ExecSqlMeta meta = eachRow("INSERT INTO t (id, name) VALUES (?, 'x')", false, "id");
    meta.setCommitSize("10");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    List<Object[]> rows = new ArrayList<>();
    for (long i = 1; i <= 5; i++) {
      rows.add(new Object[] {i});
    }
    Run run = run(meta, rowMeta, rows, true, null);

    assertEquals(4, run.output.size());
    assertEquals(1, run.errors.size());
    assertEquals(5, committedRows());
    assertEquals(5, run.counter.savepointCalls.get());
  }

  /** A script runs statement by statement: it keeps the savepoints. */
  @Test
  void aScriptOnAnAbortingDatabaseKeepsASavepointPerRow() throws Exception {
    connect("H2_SAVEPOINTS", "");
    setupDb.execStatement("INSERT INTO t VALUES (3,'existing',0)");
    AbortingConnection aborting = new AbortingConnection();
    connectionWrapper = aborting::wrap;
    ExecSqlMeta meta =
        eachRow(
            "INSERT INTO t (id) VALUES (?);\nUPDATE counter SET n = n + 1 WHERE ? > 0;",
            true,
            "id");
    meta.setSingleStatement(false);
    meta.setCommitSize("10");
    Run run = run(meta, idNameRowMeta(), idNameRows(5), true, null);

    assertEquals(4, run.output.size());
    assertEquals(1, run.errors.size());
    assertEquals(5, committedRows());
    assertEquals(5, run.counter.savepointCalls.get());
    assertEquals(4, query("SELECT n FROM counter").get(0)[0]);
  }

  /** A query returns rows, which a row window can't run: it keeps the savepoints. */
  @Test
  void aQueryOnAnAbortingDatabaseKeepsASavepointPerRow() throws Exception {
    connect("H2_SAVEPOINTS", "");
    ExecSqlMeta meta = eachRow("SELECT * FROM t WHERE id = ?", true, "id");
    meta.setCommitSize("10");
    Run run = run(meta, idNameRowMeta(), idNameRows(3), true, null);

    assertEquals(3, run.output.size());
    assertEquals(3, run.counter.savepointCalls.get());
  }

  @Test
  void savepointsAreOnlyUsedWithAnErrorHop() throws Exception {
    connect("H2_SAVEPOINTS", "");
    Run run = run(insertEachRow("10"), idNameRowMeta(), idNameRows(5), false, null);

    assertEquals(5, committedRows());
    assertEquals(0, run.counter.savepointCalls.get());
  }

  @Test
  void savepointsAreNotUsedInAutoCommitMode() throws Exception {
    connect("H2_SAVEPOINTS", "");
    Run run = run(insertEachRow("0"), idNameRowMeta(), idNameRows(5), true, null);

    assertEquals(5, committedRows());
    assertEquals(0, run.counter.savepointCalls.get());
  }

  @Test
  void stoppingRollsBackTheOpenTransactionInDispose() throws Exception {
    stopAfterRows = 15;
    Run run = run(insertEachRow("10"), idNameRowMeta(), idNameRows(30));

    assertTrue(run.transform.isStopped());
    // The first ten were committed and passed on; 11-15 were rolled back and dropped.
    assertEquals(10, committedRows());
    assertEquals(10, run.output.size());
    assertEquals(10, run.transform.getLinesOutput());
  }

  @Test
  void anErrorElsewhereRollsBackTheOpenTransactionInDispose() throws Exception {
    // Another transform failed: errors are set while this one still has uncommitted rows.
    Run run =
        runWithoutEnd(insertEachRow("100"), idNameRows(5), transform -> transform.setErrors(1));

    assertEquals(0, committedRows());
    assertEquals(0, run.output.size());
  }

  private Run runWithoutEnd(ExecSqlMeta meta, List<Object[]> rows, Consumer<ExecSql> beforeDispose)
      throws Exception {
    Run run = new Run();
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);
    TransformMeta transformMeta = new TransformMeta("exec sql", meta);
    pipelineMeta.addTransform(transformMeta);
    AtomicInteger next = new AtomicInteger();
    ExecSql transform =
        new ExecSql(
            transformMeta,
            meta,
            new ExecSqlData(),
            0,
            pipelineMeta,
            spy(new LocalPipelineEngine())) {
          @Override
          public Object[] getRow() {
            return rows.get(next.getAndIncrement());
          }

          @Override
          public IRowMeta getInputRowMeta() {
            return idNameRowMeta();
          }

          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) {
            run.output.add(row);
          }
        };
    run.transform = transform;
    assertTrue(transform.init(), "init failed");
    for (int i = 0; i < rows.size(); i++) {
      assertTrue(transform.processRow());
    }
    beforeDispose.accept(transform);
    transform.dispose();
    return run;
  }

  /**
   * A connection that behaves like PostgreSQL: once a statement fails, every following statement
   * fails until the transaction is rolled back (fully or to a savepoint), and a commit of an
   * aborted transaction rolls it back.
   */
  static final class AbortingConnection {
    final AtomicInteger aborts = new AtomicInteger();
    private boolean aborted;

    Connection wrap(Connection target) {
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                switch (method.getName()) {
                  case "commit":
                    if (aborted) {
                      aborted = false;
                      target.rollback();
                      return null;
                    }
                    break;
                  case "rollback":
                    aborted = false;
                    break;
                  default:
                    break;
                }
                Object result = invoke(target, method, args);
                if (result instanceof PreparedStatement statement) {
                  return wrapStatement(statement, PreparedStatement.class, target);
                }
                if (result instanceof Statement statement) {
                  return wrapStatement(statement, Statement.class, target);
                }
                return result;
              });
    }

    private <T extends Statement> T wrapStatement(T statement, Class<T> type, Connection target) {
      return type.cast(
          Proxy.newProxyInstance(
              type.getClassLoader(),
              new Class<?>[] {type},
              (proxy, method, args) -> {
                if (!method.getName().startsWith("execute")) {
                  return invoke(statement, method, args);
                }
                if (aborted) {
                  throw new SQLException(
                      "current transaction is aborted, commands ignored until end of transaction"
                          + " block",
                      "25P02");
                }
                try {
                  return invoke(statement, method, args);
                } catch (SQLException e) {
                  if (!target.getAutoCommit()) {
                    aborted = true;
                    aborts.incrementAndGet();
                  }
                  throw e;
                }
              }));
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }
}

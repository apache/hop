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

package org.apache.hop.junit.dml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.database.batch.IBatchRowListener;
import org.apache.hop.core.database.batch.MultiRowBatch;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatements;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.exception.HopDatabaseException;

/**
 * Runs the INSERT of several rows a dialect declares in its {@link DmlSyntax} through a {@link
 * MultiRowBatch} against a real JDBC driver, and checks both the table and the verdict of every
 * row: rows that fail on their own (a NOT NULL violation, a duplicate key) are the only ones
 * rejected, in input order, each row once. Database plugins use it to prove their declaration on
 * their own database.
 *
 * <p>The table is {@code (ID INTEGER, K2 INTEGER, LABEL VARCHAR(50) NOT NULL, AMOUNT NUMERIC(10,2),
 * PRIMARY KEY (ID, K2))}.
 */
public final class RealDriverDmlScenario {

  private RealDriverDmlScenario() {}

  /** One row of the table. */
  public record Row(int id, int k2, String label, BigDecimal amount) {
    Object[] values() {
      return new Object[] {id, k2, label, amount};
    }

    @Override
    public String toString() {
      return id + "/" + k2 + ":" + label + ":" + amount;
    }
  }

  private static final List<String> COLUMNS = List.of("ID", "K2", "LABEL", "AMOUNT");
  private static final int[] SQL_TYPES = {
    Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.NUMERIC
  };

  /** A connection, its dialect and the columns of the test table. */
  private static final class Context {
    final Connection connection;
    final DatabaseMeta databaseMeta;
    final IDatabase dialect;
    final String table;
    final int rowsPerStatement;
    final List<DmlColumn> columns;
    final List<String> statements = new ArrayList<>();

    Context(Connection connection, DatabaseMeta databaseMeta, String table, int rowsPerStatement)
        throws HopDatabaseException, SQLException {
      this.connection = connection;
      this.databaseMeta = databaseMeta;
      this.dialect = databaseMeta.getIDatabase();
      this.table = table;
      this.rowsPerStatement = rowsPerStatement;
      this.columns = DmlColumns.fromTable(connection.getMetaData(), dialect, null, table, COLUMNS);
    }

    DmlSpec insert() throws HopDatabaseException {
      return DmlSpec.insert(table, COLUMNS.stream().map(this::column).toList());
    }

    DmlColumn column(String name) {
      return columns.get(COLUMNS.indexOf(name));
    }
  }

  private static DmlPlan plan(Context context, DmlSpec spec) throws Exception {
    DmlPlan plan =
        DmlSupport.planStatements(
            context.databaseMeta, context.connection.getMetaData(), spec, context.rowsPerStatement);
    assertTrue(plan.isSupported(), plan.getUnsupportedReason());
    return plan;
  }

  private static DmlStatements statements(Context context, DmlPlan plan, DmlSpec spec) {
    return new DmlStatements(
        context.databaseMeta,
        plan,
        spec,
        context.connection,
        (statement, parameterIndex, sourceIndex, value) -> {
          String column = spec.getSourceColumns().get(sourceIndex).column();
          int sqlType = SQL_TYPES[COLUMNS.indexOf(column.toUpperCase(Locale.ROOT))];
          try {
            if (value == null) {
              statement.setNull(parameterIndex, sqlType);
            } else if (value instanceof BigDecimal decimal) {
              // Derby drops the scale of a BigDecimal set as an object of type NUMERIC.
              statement.setBigDecimal(parameterIndex, decimal);
            } else {
              statement.setObject(parameterIndex, value, sqlType);
            }
          } catch (SQLException e) {
            throw new HopDatabaseException(e);
          }
        });
  }

  private static MultiRowBatch batch(Context context, DmlStatements statements) {
    BatchFailureContract contract = context.dialect.getBatchFailureContract();
    return new MultiRowBatch(
            context.connection,
            statements,
            contract == null ? BatchFailureContract.UNKNOWN : contract,
            true)
        .withFailedEntryLocator(context.dialect::getFailedBatchEntry);
  }

  /** Inserts rows through a multi-row batch and returns the verdicts, "w:id/k2" or "r:id/k2". */
  private static List<String> write(Context context, Row... rows) throws Exception {
    DmlSpec spec = context.insert();
    DmlPlan plan = plan(context, spec);
    assertEquals(
        context.rowsPerStatement,
        plan.getRowsPerStatement(),
        "rows per statement: " + plan.getNote());
    DmlStatements statements = statements(context, plan, spec);
    context.statements.add(statements.statement(plan.getRowsPerStatement()).getSql());

    MultiRowBatch batch = batch(context, statements);
    List<String> verdicts = new ArrayList<>();
    List<Integer> counts = new ArrayList<>();
    try {
      for (Row row : rows) {
        batch.add(row.values(), new Object[] {row});
      }
      batch.flush(
          new IBatchRowListener() {
            @Override
            public void rowWritten(Object[] outputRow) {
              throw new AssertionError("the count is passed on");
            }

            @Override
            public void rowWritten(Object[] outputRow, int updateCount) {
              Row row = (Row) outputRow[0];
              verdicts.add("w:" + key(row));
              counts.add(updateCount);
            }

            @Override
            public void rowRejected(Object[] outputRow, String message, boolean attempted) {
              Row row = (Row) outputRow[0];
              verdicts.add("r:" + key(row));
            }
          });
    } finally {
      batch.close();
    }
    assertEquals(rows.length, verdicts.size(), "every row gets one verdict: " + verdicts);
    for (int count : counts) {
      assertTrue(count == 1 || count == Statement.SUCCESS_NO_INFO, "INSERT count " + count);
    }
    return verdicts;
  }

  /**
   * Runs the scenarios on a fresh table and drops it again: a load with a NOT NULL violation and a
   * key repeated in the same batch, which are the only rows refused, and a load of amounts of
   * different scales in one statement, which must all be stored as given.
   *
   * @param connection an open connection; auto-commit is switched off
   * @param databaseMeta a connection of the dialect, for quoting and its DML syntax
   * @param table an unquoted table name that doesn't exist yet
   * @param rowsPerStatement the rows per statement to ask for, within the dialect's limits
   * @return the SQL of the statements used, for the report
   */
  public static List<String> run(
      Connection connection, DatabaseMeta databaseMeta, String table, int rowsPerStatement)
      throws Exception {
    connection.setAutoCommit(false);
    createTable(connection, table);
    try {
      Context context = new Context(connection, databaseMeta, table, rowsPerStatement);

      List<Row> rows = new ArrayList<>();
      for (int id = 1; id <= 12; id++) {
        rows.add(row(id, 1, "label" + id, id + ".50"));
      }
      // A NOT NULL violation, and a key that is already in the same batch.
      rows.set(4, row(5, 1, null, "5.50"));
      rows.add(8, row(3, 1, "again", null));
      List<String> verdicts = write(context, rows.toArray(new Row[0]));

      List<String> expected = new ArrayList<>();
      for (Row row : rows) {
        expected.add((row.label() == null || "again".equals(row.label()) ? "r:" : "w:") + key(row));
      }
      assertEquals(expected, verdicts, "INSERT verdicts");
      List<Row> table1 = new ArrayList<>();
      for (int id = 1; id <= 12; id++) {
        if (id != 5) {
          table1.add(row(id, 1, "label" + id, id + ".50"));
        }
      }
      assertTable(context, table1);

      // Amounts of different scales next to each other: SQL Server types a VALUES list of several
      // rows from its parameters, and would cut 2.25 to 2.00 next to a 1 without the casts.
      List<Row> scales = new ArrayList<>();
      String[] amounts = {"1", "2.25", "300", "0.5", "-7.05", null};
      for (int i = 0; i < Math.max(rowsPerStatement, amounts.length); i++) {
        scales.add(row(100 + i, 2, "scale" + i, amounts[i % amounts.length]));
      }
      verdicts = write(context, scales.toArray(new Row[0]));
      assertTrue(verdicts.stream().allMatch(v -> v.startsWith("w:")), verdicts.toString());
      List<Row> table2 = new ArrayList<>(table1);
      for (Row row : scales) {
        table2.add(
            new Row(
                row.id(),
                row.k2(),
                row.label(),
                row.amount() == null ? null : row.amount().setScale(2)));
      }
      assertTable(context, table2);
      return context.statements;
    } finally {
      try {
        connection.rollback();
      } catch (SQLException e) {
        // nothing pending
      }
      dropQuietly(connection, table);
    }
  }

  /**
   * Inserts statements as large as the dialect allows: the plan for {@code rowsPerStatement} rows
   * is capped by the dialect's {@link DmlSyntax#maxParameters()} and {@link
   * DmlSyntax#maxRowsPerStatement()}, and two full statements plus a remainder of three rows are
   * written and checked in the table. Every row must be written: a statement the server refuses at
   * the limit fails the test.
   *
   * @param connection an open connection; auto-commit is switched off
   * @param databaseMeta a connection of the dialect, for quoting and its DML syntax
   * @param table an unquoted table name that doesn't exist yet
   * @param rowsPerStatement the rows per statement to ask for, normally more than the limits allow
   * @return a line with the rows per statement used and why, for the report
   */
  public static String runAtLimits(
      Connection connection, DatabaseMeta databaseMeta, String table, int rowsPerStatement)
      throws Exception {
    connection.setAutoCommit(false);
    createTable(connection, table);
    try {
      Context probe = new Context(connection, databaseMeta, table, rowsPerStatement);
      DmlPlan plan = plan(probe, probe.insert());
      int n = plan.getRowsPerStatement();
      Context context = new Context(connection, databaseMeta, table, n);
      int rows = 2 * n + 3;
      Row[] batch = new Row[rows];
      for (int i = 0; i < rows; i++) {
        int id = i + 1;
        batch[i] = row(id, 1, "i-" + id, Integer.toString(id % 10000));
      }
      long start = System.nanoTime();
      List<String> verdicts = write(context, batch);
      long millis = (System.nanoTime() - start) / 1_000_000;
      assertTrue(verdicts.stream().allMatch(v -> v.startsWith("w:")), "every row written");
      assertEquals(rows, count(context), "INSERT at the limit");
      return "INSERT: "
          + n
          + " rows/statement, "
          + rows
          + " rows in "
          + millis
          + " ms, statement "
          + context.statements.get(0).length()
          + " chars"
          + (plan.getNote() == null ? "" : " - " + plan.getNote());
    } finally {
      try {
        connection.rollback();
      } catch (SQLException e) {
        // nothing pending
      }
      dropQuietly(connection, table);
    }
  }

  /**
   * Inserts rows of a wide table, {@code ID INTEGER} and {@code columns} value columns of {@code
   * columnType}, in statements as large as the dialect allows for {@code rowsPerStatement}: one
   * full statement and a remainder of three rows. The rows go without recovery, the way Table
   * output writes them without an error hop, so a statement the server or the driver refuses for
   * its size fails the test where one row per statement would have worked. Every value is read
   * back.
   *
   * @param connection an open connection; auto-commit is switched off
   * @param databaseMeta a connection of the dialect, for quoting and its DML syntax
   * @param table an unquoted table name that doesn't exist yet
   * @param columns the number of value columns
   * @param columnType the type of the value columns, for example {@code VARCHAR(10)}
   * @param valueLength the length of every value, at most what {@code columnType} holds
   * @param rowsPerStatement the rows per statement to ask for
   * @return a line with the rows per statement used, the length of the statement and the size of
   *     its values, for the report
   */
  public static String runWide(
      Connection connection,
      DatabaseMeta databaseMeta,
      String table,
      int columns,
      String columnType,
      int valueLength,
      int rowsPerStatement)
      throws Exception {
    connection.setAutoCommit(false);
    List<String> names = new ArrayList<>();
    names.add("ID");
    StringBuilder ddl =
        new StringBuilder("CREATE TABLE ")
            .append(table)
            .append(" (ID INTEGER NOT NULL PRIMARY KEY");
    for (int c = 1; c <= columns; c++) {
      String name = String.format(Locale.ROOT, "C%04d", c);
      names.add(name);
      ddl.append(", ").append(name).append(' ').append(columnType);
    }
    ddl.append(')');
    dropQuietly(connection, table);
    execute(connection, ddl.toString());
    connection.commit();
    try {
      IDatabase dialect = databaseMeta.getIDatabase();
      DmlSpec spec =
          DmlSpec.insert(
              table, DmlColumns.fromTable(connection.getMetaData(), dialect, null, table, names));
      DmlPlan plan =
          DmlSupport.planStatements(databaseMeta, connection.getMetaData(), spec, rowsPerStatement);
      assertTrue(plan.isSupported(), plan.getUnsupportedReason());
      int n = plan.getRowsPerStatement();
      DmlStatements statements =
          new DmlStatements(
              databaseMeta,
              plan,
              spec,
              connection,
              (statement, parameterIndex, sourceIndex, value) -> {
                try {
                  if (sourceIndex == 0) {
                    statement.setInt(parameterIndex, (Integer) value);
                  } else {
                    statement.setString(parameterIndex, (String) value);
                  }
                } catch (SQLException e) {
                  throw new HopDatabaseException(e);
                }
              });
      String sql = statements.statement(n).getSql();
      BatchFailureContract contract = dialect.getBatchFailureContract();
      MultiRowBatch batch =
          new MultiRowBatch(
                  connection,
                  statements,
                  contract == null ? BatchFailureContract.UNKNOWN : contract,
                  true)
              .withFailedEntryLocator(dialect::getFailedBatchEntry);
      int rows = n + 3;
      int[] written = {0};
      long start = System.nanoTime();
      try {
        for (int id = 1; id <= rows; id++) {
          Object[] values = wideRow(id, columns, valueLength);
          batch.add(values, values);
        }
        batch.flushOrFail(
            new IBatchRowListener() {
              @Override
              public void rowWritten(Object[] outputRow) {
                written[0]++;
              }

              @Override
              public void rowWritten(Object[] outputRow, int updateCount) {
                written[0]++;
              }

              @Override
              public void rowRejected(Object[] outputRow, String message, boolean attempted) {
                throw new AssertionError("row rejected: " + message);
              }
            });
      } finally {
        batch.close();
      }
      long millis = (System.nanoTime() - start) / 1_000_000;
      assertEquals(rows, written[0], "every row written");

      int read = 0;
      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("SELECT * FROM " + table + " ORDER BY ID")) {
        while (resultSet.next()) {
          read++;
          Object[] expected = wideRow(read, columns, valueLength);
          assertEquals(read, resultSet.getInt(1), "ID");
          for (int c = 1; c <= columns; c++) {
            assertEquals(
                expected[c], resultSet.getString(c + 1), "row " + read + " " + names.get(c));
          }
        }
      }
      connection.commit();
      assertEquals(rows, read, "rows in the table");
      return String.format(
          Locale.ROOT,
          "wide INSERT: %d columns of %s, %d rows/statement asked, %d used, %d rows in %d ms,"
              + " statement %d chars, %d value bytes%s",
          columns + 1,
          columnType,
          rowsPerStatement,
          n,
          rows,
          millis,
          sql.length(),
          (long) n * columns * valueLength,
          plan.getNote() == null ? "" : " - " + plan.getNote());
    } finally {
      try {
        connection.rollback();
      } catch (SQLException e) {
        // nothing pending
      }
      dropQuietly(connection, table);
    }
  }

  /** Row {@code id} of the wide table: the key and a value of {@code length} per column. */
  private static Object[] wideRow(int id, int columns, int length) {
    Object[] values = new Object[columns + 1];
    values[0] = id;
    for (int c = 1; c <= columns; c++) {
      String seed = id + "-" + c + "-";
      StringBuilder value = new StringBuilder(length + seed.length());
      while (value.length() < length) {
        value.append(seed);
      }
      values[c] = value.substring(0, length);
    }
    return values;
  }

  /**
   * Times {@code rows} inserted rows through a {@link MultiRowBatch}, one row per statement against
   * {@code rowsPerStatement}. Nothing is asserted but the end state.
   *
   * @return one line per pass, for the report
   */
  public static List<String> measure(
      Connection connection,
      DatabaseMeta databaseMeta,
      String table,
      int rows,
      int rowsPerStatement,
      int commitSize)
      throws Exception {
    List<String> report = new ArrayList<>();
    connection.setAutoCommit(false);
    for (int n : new int[] {1, rowsPerStatement}) {
      createTable(connection, table);
      try {
        Context context = new Context(connection, databaseMeta, table, n);
        DmlSpec spec = context.insert();
        DmlPlan plan = plan(context, spec);
        MultiRowBatch batch = batch(context, statements(context, plan, spec));
        int[] written = {0};
        IBatchRowListener listener =
            new IBatchRowListener() {
              @Override
              public void rowWritten(Object[] outputRow) {
                written[0]++;
              }

              @Override
              public void rowRejected(Object[] outputRow, String message, boolean attempted) {
                throw new AssertionError("row rejected: " + message);
              }
            };
        long start = System.nanoTime();
        for (int i = 0; i < rows; i++) {
          int id = i + 1;
          Object[] values = {id, 1, "label" + id, new BigDecimal(id).movePointLeft(2)};
          batch.add(values, values);
          if (batch.size() >= commitSize) {
            batch.flush(listener);
          }
        }
        batch.flush(listener);
        long millis = (System.nanoTime() - start) / 1_000_000;
        batch.close();
        assertEquals(rows, written[0]);
        report.add(
            String.format(
                Locale.ROOT,
                "INSERT %6d rows, %4d rows/statement: %6d ms, %8.0f rows/s, %d statements",
                rows,
                plan.getRowsPerStatement(),
                millis,
                rows * 1000.0 / Math.max(1, millis),
                batch.getStatements()));
      } finally {
        dropQuietly(connection, table);
      }
    }
    return report;
  }

  /** Fails when the dialect declares no INSERT of several rows. */
  public static void assertDeclared(DatabaseMeta databaseMeta) {
    DmlSyntax syntax = databaseMeta.getIDatabase().getDmlSyntax();
    assertNotNull(syntax, "the dialect declares a DML syntax");
    assertNotNull(syntax.insertForm(), "the dialect declares an INSERT of several rows");
  }

  private static int count(Context context) throws SQLException {
    try (Statement statement = context.connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + context.table)) {
      resultSet.next();
      int count = resultSet.getInt(1);
      context.connection.commit();
      return count;
    }
  }

  private static String key(Row row) {
    return row.id() + "/" + row.k2();
  }

  private static Row row(int id, int k2, String label, String amount) {
    return new Row(id, k2, label, amount == null ? null : new BigDecimal(amount));
  }

  private static List<Row> readTable(Context context) throws SQLException {
    List<Row> rows = new ArrayList<>();
    try (Statement statement = context.connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT ID, K2, LABEL, AMOUNT FROM " + context.table + " ORDER BY ID, K2")) {
      while (resultSet.next()) {
        BigDecimal amount = resultSet.getBigDecimal(4);
        rows.add(
            new Row(
                resultSet.getInt(1),
                resultSet.getInt(2),
                resultSet.getString(3),
                amount == null ? null : amount.setScale(2)));
      }
    }
    context.connection.commit();
    return rows;
  }

  private static void assertTable(Context context, List<Row> expected) throws SQLException {
    List<Row> sorted = new ArrayList<>(expected);
    sorted.sort(
        (a, b) ->
            a.id() != b.id() ? Integer.compare(a.id(), b.id()) : Integer.compare(a.k2(), b.k2()));
    assertEquals(sorted, readTable(context));
  }

  private static void createTable(Connection connection, String table) throws SQLException {
    dropQuietly(connection, table);
    execute(
        connection,
        "CREATE TABLE "
            + table
            + " (ID INTEGER NOT NULL, K2 INTEGER NOT NULL, LABEL VARCHAR(50) NOT NULL,"
            + " AMOUNT NUMERIC(10,2), PRIMARY KEY (ID, K2))");
    connection.commit();
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void dropQuietly(Connection connection, String table) {
    try {
      execute(connection, "DROP TABLE " + table);
      connection.commit();
    } catch (SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException ignored) {
        // nothing to undo
      }
    }
  }
}

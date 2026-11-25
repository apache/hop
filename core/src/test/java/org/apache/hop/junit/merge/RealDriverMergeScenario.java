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

package org.apache.hop.junit.merge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.NoneDatabaseMeta;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.database.batch.IBatchRowListener;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatement;
import org.apache.hop.core.database.dml.DmlStatementBuilder;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.core.exception.HopDatabaseException;

/**
 * Runs the one-row upsert statements of {@link DmlStatementBuilder} for a dialect's declared {@link
 * DmlSyntax} against a real JDBC driver, and checks what the table holds afterwards: insert,
 * update, insert only, update only changed rows, the same key twice in one JDBC batch, and a {@link
 * RecoverableBatch} with a bad row under the dialect's batch failure contract. Database plugins use
 * it to prove that their declaration works on their own database.
 *
 * <p>The table is {@code (ID INTEGER PRIMARY KEY, LABEL VARCHAR(50) NOT NULL, AMOUNT NUMERIC(10,2),
 * CODE CHAR(3))}. The native types for parameter casts are read back from the JDBC metadata with
 * {@link DmlColumns#fromTable}, as the transform does.
 */
public final class RealDriverMergeScenario {

  private RealDriverMergeScenario() {}

  /** One row of the table, or of a MERGE source. */
  public record Row(int id, String label, BigDecimal amount, String code) {
    Object[] values() {
      return new Object[] {id, label, amount, code};
    }
  }

  /**
   * The update counts the driver returned, for the report: they differ between databases.
   *
   * @param upsertInsert a MERGE that inserted
   * @param upsertUpdate a MERGE that updated
   * @param insertOnlyExisting an insert-only MERGE on an existing key
   * @param unchangedSkipped an update-only-changed MERGE of an identical row
   * @param batch the counts of the batch, which merges the same key twice
   */
  public record Report(
      int upsertInsert,
      int upsertUpdate,
      int insertOnlyExisting,
      int unchangedSkipped,
      int[] batch) {
    @Override
    public String toString() {
      return "insert="
          + upsertInsert
          + ", update="
          + upsertUpdate
          + ", insert-only on existing key="
          + insertOnlyExisting
          + ", unchanged skipped="
          + unchangedSkipped
          + ", batch="
          + Arrays.toString(batch);
    }
  }

  private static final List<String> COLUMNS = List.of("ID", "LABEL", "AMOUNT", "CODE");
  private static final int[] SQL_TYPES = {Types.INTEGER, Types.VARCHAR, Types.NUMERIC, Types.CHAR};

  /**
   * Runs every scenario with the dialect's MERGE, see {@link #run(Connection, DatabaseMeta, String,
   * UpsertForm)}.
   */
  public static Report run(Connection connection, DatabaseMeta databaseMeta, String table)
      throws Exception {
    return run(connection, databaseMeta, table, UpsertForm.MERGE);
  }

  /**
   * Runs every scenario on a fresh table and drops it again.
   *
   * @param connection an open connection; auto-commit is switched off
   * @param databaseMeta a connection of the dialect, for quoting and its DML syntax
   * @param table an unquoted table name that doesn't exist yet
   * @param form the declared upsert form to run; the first declaration of it the server qualifies
   *     for is used
   * @return the update counts the driver returned
   */
  public static Report run(
      Connection connection, DatabaseMeta databaseMeta, String table, UpsertForm form)
      throws Exception {
    IDatabase dialect = databaseMeta.getIDatabase();
    DmlSyntax syntax = dialect.getDmlSyntax();
    assertNotNull(syntax, "the dialect must declare a DML syntax");
    DmlSyntax.Upsert merge = null;
    for (DmlSyntax.Upsert candidate : syntax.upserts(form)) {
      DmlSyntax only = syntax.withUpserts(List.of(candidate));
      DatabaseMeta probe = new DatabaseMeta();
      probe.setIDatabase(new SyntaxOnly(only, dialect.getPluginName()));
      if (DmlSupport.unsupportedReason(probe, connection.getMetaData(), form) == null) {
        merge = candidate;
        break;
      }
    }
    assertNotNull(merge, "the dialect must declare " + form + " for this server");
    // Counts differ between the forms: MySQL reports 2 for an update, and 1 for a row it found but
    // left alone. Only the MERGE and ON CONFLICT counts are checked; the table is always checked.
    boolean checkCounts = form != UpsertForm.ON_DUPLICATE_KEY;
    connection.setAutoCommit(false);
    dropQuietly(connection, table);
    execute(
        connection,
        "CREATE TABLE "
            + table
            + " (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(50) NOT NULL,"
            + " AMOUNT NUMERIC(10,2), CODE CHAR(3))");
    connection.commit();
    try {
      List<DmlColumn> columns =
          DmlColumns.fromTable(connection.getMetaData(), dialect, null, table, COLUMNS);
      List<DmlColumn> keys = columns.subList(0, 1);
      List<DmlColumn> others = columns.subList(1, columns.size());

      DmlStatement upsert =
          DmlStatementBuilder.upsert(
              databaseMeta, syntax, merge, DmlSpec.upsert(table, keys, others, others, false));
      DmlStatement insertOnly =
          DmlStatementBuilder.upsert(
              databaseMeta, syntax, merge, DmlSpec.upsert(table, keys, others, List.of(), false));
      DmlStatement changedOnly =
          DmlStatementBuilder.upsert(
              databaseMeta, syntax, merge, DmlSpec.upsert(table, keys, others, others, true));
      String name = dialect.getClass().getSimpleName() + " " + form;
      System.out.println(name + ": " + upsert.getSql());
      System.out.println(name + " changed-only: " + changedOnly.getSql());

      seed(connection, table, row(1, "one", "1.10", "AAA"), row(2, "two", "2.20", "BBB"));

      // Insert of a new key, with NULLs in the nullable columns; update of an existing key.
      int upsertInsert = merge(connection, upsert, row(3, "three", null, null));
      int upsertUpdate = merge(connection, upsert, row(2, "TWO", "22.22", "BBX"));
      if (checkCounts) {
        assertEquals(1, upsertInsert, "an upsert that inserts reports one row");
        assertEquals(1, upsertUpdate, "an upsert that updates reports one row");
      }
      assertTable(
          connection,
          table,
          row(1, "one", "1.10", "AAA"),
          row(2, "TWO", "22.22", "BBX"),
          row(3, "three", null, null));

      // Insert-only leaves an existing row alone.
      int insertOnlyExisting = merge(connection, insertOnly, row(1, "changed", "9.99", "ZZZ"));
      assertEquals(1, merge(connection, insertOnly, row(4, "four", "4.40", "DDD")));
      if (checkCounts) {
        assertEquals(
            0, insertOnlyExisting, "an insert-only upsert on an existing key does nothing");
      }
      assertTable(
          connection,
          table,
          row(1, "one", "1.10", "AAA"),
          row(2, "TWO", "22.22", "BBX"),
          row(3, "three", null, null),
          row(4, "four", "4.40", "DDD"));

      // Update only what changed, comparing NULLs as equal.
      int unchangedSkipped = merge(connection, changedOnly, row(1, "one", "1.10", "AAA"));
      int nullsEqual = merge(connection, changedOnly, row(3, "three", null, null));
      int toNull = merge(connection, changedOnly, row(2, "TWO", null, "BBX"));
      int fromNull = merge(connection, changedOnly, row(3, "three", "3.30", null));
      int newKey = merge(connection, changedOnly, row(5, "five", null, "EEE"));
      if (checkCounts) {
        assertEquals(0, unchangedSkipped, "an identical row is not updated");
        assertEquals(0, nullsEqual, "NULL equals NULL");
        assertEquals(1, toNull, "a value that becomes NULL is a change");
        assertEquals(1, fromNull, "a NULL that gets a value is a change");
        assertEquals(1, newKey, "a new key is inserted");
      }
      assertTable(
          connection,
          table,
          row(1, "one", "1.10", "AAA"),
          row(2, "TWO", null, "BBX"),
          row(3, "three", "3.30", null),
          row(4, "four", "4.40", "DDD"),
          row(5, "five", null, "EEE"));

      // One JDBC batch with the same key twice: the second must update the row of the first.
      int[] batchCounts;
      try (PreparedStatement statement = connection.prepareStatement(upsert.getSql())) {
        for (Row r :
            List.of(
                row(10, "ten", "10.00", "TTT"),
                row(11, "eleven", null, null),
                row(10, "TEN", "10.10", "TTX"),
                row(1, "ONE", "1.11", "AAX"))) {
          bind(statement, upsert, r);
          statement.addBatch();
        }
        batchCounts = statement.executeBatch();
      }
      connection.commit();
      assertTable(
          connection,
          table,
          row(1, "ONE", "1.11", "AAX"),
          row(2, "TWO", null, "BBX"),
          row(3, "three", "3.30", null),
          row(4, "four", "4.40", "DDD"),
          row(5, "five", null, "EEE"),
          row(10, "TEN", "10.10", "TTX"),
          row(11, "eleven", null, null));

      recoverableBatch(connection, dialect, table, upsert);
      assertTable(
          connection,
          table,
          row(1, "ONE", "1.11", "AAX"),
          row(2, "TWO", null, "BBX"),
          row(3, "three", "3.30", null),
          row(4, "four", "4.40", "DDD"),
          row(5, "FIVE", "5.50", "EEE"),
          row(10, "TEN", "10.10", "TTX"),
          row(11, "eleven", null, null),
          row(20, "twenty", null, null),
          row(22, "twenty-two", "22.00", "VVV"));

      return new Report(
          upsertInsert, upsertUpdate, insertOnlyExisting, unchangedSkipped, batchCounts);
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
   * "Update only changed rows" with a column the database can't compare with {@code <>} (a CLOB,
   * json, xml, ...): the statement must not compare it, or every row fails ("operator does not
   * exist: json <> json", ORA-00932). Every matched row is updated instead. Runs on a fresh table
   * {@code (ID INTEGER PRIMARY KEY, DOC type)} and drops it again.
   *
   * @param connection an open connection; auto-commit is switched off
   * @param databaseMeta a connection of the dialect
   * @param table an unquoted table name that doesn't exist yet
   * @param form the declared upsert form to run
   * @param type the column type, as a CREATE TABLE writes it
   * @param first the value written by the insert, bound as a string
   * @param second another value, bound as a string, that the update must write; it may differ from
   *     the first only in case, for a type that compares ignoring case
   */
  public static void uncomparableColumn(
      Connection connection,
      DatabaseMeta databaseMeta,
      String table,
      UpsertForm form,
      String type,
      String first,
      String second)
      throws Exception {
    IDatabase dialect = databaseMeta.getIDatabase();
    DmlSyntax syntax = dialect.getDmlSyntax();
    DmlSyntax.Upsert upsert = declared(connection, databaseMeta, form);
    connection.setAutoCommit(false);
    dropQuietly(connection, table);
    execute(
        connection,
        "CREATE TABLE " + table + " (ID INTEGER NOT NULL PRIMARY KEY, DOC " + type + ")");
    connection.commit();
    try {
      List<DmlColumn> columns =
          DmlColumns.fromTable(
              connection.getMetaData(), dialect, null, table, List.of("ID", "DOC"));
      assertFalse(columns.get(1).comparable(), type + " must count as not comparable");
      List<DmlColumn> doc = columns.subList(1, 2);
      DmlStatement changedOnly =
          DmlStatementBuilder.upsert(
              databaseMeta,
              syntax,
              upsert,
              DmlSpec.upsert(table, columns.subList(0, 1), doc, doc, true));
      System.out.println(type + " changed-only: " + changedOnly.getSql());
      for (String value : new String[] {first, second, second}) {
        try (PreparedStatement statement = connection.prepareStatement(changedOnly.getSql())) {
          List<DmlColumn> parameters = changedOnly.getRowColumns();
          for (int i = 0; i < parameters.size(); i++) {
            if (parameters.get(i).column().equalsIgnoreCase("ID")) {
              statement.setInt(i + 1, 1);
            } else {
              statement.setString(i + 1, value);
            }
          }
          statement.executeUpdate();
        }
        connection.commit();
        try (Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT DOC FROM " + table)) {
          assertTrue(resultSet.next());
          assertEquals(
              value.replaceAll("\\s", ""),
              String.valueOf(resultSet.getString(1)).replaceAll("\\s", ""),
              type + ": the row holds the last value written");
          assertFalse(resultSet.next());
        }
        connection.commit();
      }
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
   * @return the first declaration of the form that the server qualifies for
   */
  private static DmlSyntax.Upsert declared(
      Connection connection, DatabaseMeta databaseMeta, UpsertForm form) throws SQLException {
    IDatabase dialect = databaseMeta.getIDatabase();
    DmlSyntax syntax = dialect.getDmlSyntax();
    assertNotNull(syntax, "the dialect must declare a DML syntax");
    for (DmlSyntax.Upsert candidate : syntax.upserts(form)) {
      DmlSyntax only = syntax.withUpserts(List.of(candidate));
      DatabaseMeta probe = new DatabaseMeta();
      probe.setIDatabase(new SyntaxOnly(only, dialect.getPluginName()));
      if (DmlSupport.unsupportedReason(probe, connection.getMetaData(), form) == null) {
        return candidate;
      }
    }
    throw new AssertionError("the dialect must declare " + form + " for this server");
  }

  /** A batch with one row that violates NOT NULL: only that row may be rejected. */
  private static void recoverableBatch(
      Connection connection, IDatabase dialect, String table, DmlStatement upsert)
      throws Exception {
    BatchFailureContract contract = dialect.getBatchFailureContract();
    Set<Integer> written = new TreeSet<>();
    Map<Integer, String> rejected = new TreeMap<>();
    try (PreparedStatement statement = connection.prepareStatement(upsert.getSql())) {
      RecoverableBatch batch =
          new RecoverableBatch(
                  connection,
                  statement,
                  (s, bindRow) -> {
                    try {
                      bind(s, upsert, (Row) bindRow[0]);
                    } catch (SQLException e) {
                      throw new HopDatabaseException(e);
                    }
                  },
                  contract,
                  true)
              .withFailedEntryLocator(dialect::getFailedBatchEntry)
              .withStatementFactory(() -> connection.prepareStatement(upsert.getSql()));
      for (Row r :
          List.of(
              row(20, "twenty", null, null),
              row(21, null, "21.00", "UUU"),
              row(22, "twenty-two", "22.00", "VVV"),
              row(5, "FIVE", "5.50", "EEE"))) {
        batch.add(new Object[] {r}, new Object[] {r.id()});
      }
      batch.flush(
          new IBatchRowListener() {
            @Override
            public void rowWritten(Object[] outputRow) {
              written.add((Integer) outputRow[0]);
            }

            @Override
            public void rowRejected(Object[] outputRow, String message, boolean attempted) {
              rejected.put((Integer) outputRow[0], message);
            }
          });
      if (batch.getStatement() != statement) {
        batch.getStatement().close();
      }
    }
    System.out.println(
        dialect.getClass().getSimpleName()
            + " recoverable upsert batch ("
            + contract
            + "): written "
            + written
            + ", rejected "
            + rejected);
    assertEquals(Set.of(20, 22, 5), written, "every row but the bad one is written");
    assertEquals(Set.of(21), rejected.keySet(), "only the NOT NULL violation is rejected");
  }

  /** A dialect that only declares the given syntax, to ask whether the server qualifies. */
  private static final class SyntaxOnly extends NoneDatabaseMeta {
    private final DmlSyntax syntax;

    SyntaxOnly(DmlSyntax syntax, String pluginName) {
      this.syntax = syntax;
      setPluginName(pluginName);
    }

    @Override
    public DmlSyntax getDmlSyntax() {
      return syntax;
    }
  }

  private static Row row(int id, String label, String amount, String code) {
    return new Row(id, label, amount == null ? null : new BigDecimal(amount), code);
  }

  private static void seed(Connection connection, String table, Row... rows) throws SQLException {
    try (PreparedStatement insert =
        connection.prepareStatement(
            "INSERT INTO " + table + " (ID, LABEL, AMOUNT, CODE) VALUES (?, ?, ?, ?)")) {
      for (Row r : rows) {
        Object[] values = r.values();
        for (int i = 0; i < values.length; i++) {
          setValue(insert, i + 1, values[i], SQL_TYPES[i]);
        }
        insert.executeUpdate();
      }
    }
    connection.commit();
  }

  private static int merge(Connection connection, DmlStatement merge, Row r) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(merge.getSql())) {
      bind(statement, merge, r);
      int count = statement.executeUpdate();
      connection.commit();
      return count;
    }
  }

  private static void bind(PreparedStatement statement, DmlStatement merge, Row r)
      throws SQLException {
    Object[] values = r.values();
    List<DmlColumn> source = merge.getRowColumns();
    for (int i = 0; i < source.size(); i++) {
      int index = indexOf(source.get(i).column());
      setValue(statement, i + 1, values[index], SQL_TYPES[index]);
    }
  }

  private static int indexOf(String column) {
    for (int i = 0; i < COLUMNS.size(); i++) {
      if (COLUMNS.get(i).equalsIgnoreCase(column)) {
        return i;
      }
    }
    throw new IllegalArgumentException(column);
  }

  private static void setValue(PreparedStatement statement, int index, Object value, int sqlType)
      throws SQLException {
    if (value == null) {
      statement.setNull(index, sqlType);
    } else {
      statement.setObject(index, value, sqlType);
    }
  }

  private static void assertTable(Connection connection, String table, Row... expected)
      throws SQLException {
    List<Row> actual = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT ID, LABEL, AMOUNT, CODE FROM " + table + " ORDER BY ID")) {
      while (resultSet.next()) {
        BigDecimal amount = resultSet.getBigDecimal(3);
        actual.add(
            new Row(
                resultSet.getInt(1),
                resultSet.getString(2),
                amount == null ? null : amount.setScale(2),
                resultSet.getString(4)));
      }
    }
    connection.commit();
    assertEquals(Arrays.asList(expected), actual);
    for (Row r : actual) {
      assertTrue(r.code() == null || r.code().length() == 3, "CHAR(3) keeps its length: " + r);
    }
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

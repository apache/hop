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

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatement;
import org.apache.hop.core.database.dml.DmlStatementBuilder;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.dml.UpsertForm;

/**
 * Writes one value through every upsert form a dialect declares, and through the plain one-row
 * INSERT or UPDATE, on a real database. The plain statement is the reference: the upsert, whose
 * parameters may be cast ({@link DmlSyntax#castParameters()}), must store the same value, match the
 * same rows, or refuse the value as the plain statement does. A cast that cuts or rounds the value
 * shows up as a difference.
 *
 * <p>Value forms write the value into column {@code v} of {@code (id INTEGER PRIMARY KEY, v type)}.
 * The key form matches on {@code (id, v)} in {@code (id INTEGER, v type, note VARCHAR(10))}, where
 * the existing rows hold the value the tested one would become if it were truncated or rounded
 * ({@link Case#near()}).
 */
public final class RealDriverCastScenario {

  /** What a statement did: the table afterwards, or that it refused the row. */
  public static final String REFUSED = "REFUSED";

  private RealDriverCastScenario() {}

  /** Binds a value as Hop binds it. */
  @FunctionalInterface
  public interface Binder {
    void bind(PreparedStatement statement, int index, Object value) throws SQLException;
  }

  /** setString, setBigDecimal, setTimestamp, setBytes, setBoolean or setLong, by Java type. */
  public static final Binder PLAIN =
      (statement, index, value) -> {
        switch (value) {
          case null -> statement.setNull(index, Types.VARCHAR);
          case String string -> statement.setString(index, string);
          case BigDecimal decimal -> statement.setBigDecimal(index, decimal);
          case Timestamp timestamp -> statement.setTimestamp(index, timestamp);
          case byte[] bytes -> statement.setBytes(index, bytes);
          case Boolean bool -> statement.setBoolean(index, bool);
          case Long number -> statement.setLong(index, number);
          case Integer number -> statement.setLong(index, number);
          default -> statement.setObject(index, value);
        }
      };

  /** A string as an untyped parameter: how Hop binds a UUID, JSON or INET value on PostgreSQL. */
  public static final Binder OTHER =
      (statement, index, value) -> statement.setObject(index, value, Types.OTHER);

  /**
   * One value to write.
   *
   * @param name a short name for messages
   * @param columnType the column type, as written in CREATE TABLE
   * @param value the value tested
   * @param good a value the column takes as it is, for the other row and the existing rows
   * @param near what the value becomes when it is truncated or rounded to the column; null to skip
   *     the key forms
   * @param binder how the value is bound
   */
  public record Case(
      String name, String columnType, Object value, Object good, Object near, Binder binder) {

    public Case(String name, String columnType, Object value, Object good, Object near) {
      this(name, columnType, value, good, near, PLAIN);
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /** An upsert form of one row. */
  public enum Form {
    /** Upsert of a new row, against a plain INSERT. */
    UPSERT_NEW_ONE,
    /** Upsert of an existing row, against a plain UPDATE. */
    UPSERT_EXISTING_ONE,
    /** MERGE of a row matched on (id, v), against a plain UPDATE, or INSERT if none matched. */
    UPSERT_BY_KEY_ONE
  }

  /**
   * What the plain statements and the tested statement did.
   *
   * @param reference the table after the plain one-row statements, or {@link #REFUSED}
   * @param actual the table after the tested statement, or {@link #REFUSED}
   * @param sql the tested statement
   * @param message the error of the tested statement, or null
   */
  public record Outcome(String reference, String actual, String sql, String message) {
    public boolean same() {
      return reference.equals(actual);
    }
  }

  /**
   * Runs a case through each upsert form the dialect declares.
   *
   * @return the outcome of each form, keyed by the form and, for an upsert, its form ("MERGE
   *     UPSERT_NEW_ONE")
   */
  public static Map<String, Outcome> run(
      Connection connection, DatabaseMeta databaseMeta, String table, Case kase) throws Exception {
    connection.setAutoCommit(true);
    DmlSyntax syntax = databaseMeta.getIDatabase().getDmlSyntax();
    Map<String, Outcome> outcomes = new LinkedHashMap<>();
    Runner runner = new Runner(connection, databaseMeta, table, kase, syntax);
    try {
      for (DmlSyntax.Upsert upsert : syntax.upserts()) {
        String prefix = upsert.form() + " ";
        outcomes.put(prefix + Form.UPSERT_NEW_ONE, runner.upsert(upsert, false));
        outcomes.put(prefix + Form.UPSERT_EXISTING_ONE, runner.upsert(upsert, true));
      }
      DmlSyntax.Upsert merge = syntax.upsert(UpsertForm.MERGE);
      if (kase.near() != null && merge != null) {
        outcomes.put("MERGE " + Form.UPSERT_BY_KEY_ONE, runner.upsertByKey(merge));
      }
    } finally {
      runner.drop();
    }
    return outcomes;
  }

  /**
   * @return the forms whose statement did something other than the plain statements, with what it
   *     did
   */
  public static Map<String, String> differences(Map<String, Outcome> outcomes) {
    Map<String, String> differences = new LinkedHashMap<>();
    outcomes.forEach(
        (form, outcome) -> {
          if (!outcome.same()) {
            differences.put(form, outcome.actual());
          }
        });
    return differences;
  }

  /** Asserts that every form did what the plain statements did. */
  public static void assertSameAsPlain(String label, Map<String, Outcome> outcomes) {
    print(label, outcomes);
    assertEquals(Map.of(), differences(outcomes), label + ": forms that differ from plain SQL");
  }

  /** Prints each form's reference and outcome, with the statement and its error. */
  public static void print(String label, Map<String, Outcome> outcomes) {
    outcomes.forEach(
        (form, outcome) ->
            System.out.println(
                label
                    + " | "
                    + form
                    + " | plain: "
                    + outcome.reference()
                    + " | "
                    + (outcome.same() ? "same" : "DIFFERS: " + outcome.actual())
                    + (outcome.message() == null ? "" : " | " + outcome.message())
                    + " | "
                    + outcome.sql()));
  }

  /** Runs the statements of one case. */
  private static final class Runner {
    private final Connection connection;
    private final DatabaseMeta databaseMeta;
    private final String table;
    private final Case kase;
    private final DmlSyntax syntax;

    Runner(
        Connection connection,
        DatabaseMeta databaseMeta,
        String table,
        Case kase,
        DmlSyntax syntax) {
      this.connection = connection;
      this.databaseMeta = databaseMeta;
      this.table = table;
      this.kase = kase;
      this.syntax = syntax;
    }

    // ------------------------------------------------------------------------------ value forms

    Outcome upsert(DmlSyntax.Upsert upsert, boolean existing) throws Exception {
      Runnable setUp =
          existing ? () -> createValueTable(kase.good(), kase.good()) : this::createValueTable;
      setUp.run();
      List<DmlColumn> columns = columns("id", "v");
      List<Map<String, Object>> bindRows = rows(1, kase.value());
      String reference =
          plain(
              existing ? update(columns.subList(0, 1), columns.subList(1, 2)) : insert(columns),
              bindRows,
              setUp,
              false);
      setUp.run();
      DmlSpec spec =
          DmlSpec.upsert(
              table, columns.subList(0, 1), columns.subList(1, 2), columns.subList(1, 2), false);
      return tested(reference, spec, upsert, bindRows);
    }

    // -------------------------------------------------------------------------------- key forms

    Outcome upsertByKey(DmlSyntax.Upsert merge) throws Exception {
      Runnable setUp = this::createKeyTable;
      setUp.run();
      List<DmlColumn> columns = columns("id", "v", "note");
      List<Map<String, Object>> bindRows = keyRows().subList(0, 1);
      // The reference: a plain UPDATE, and a plain INSERT when it matched no row.
      String reference;
      try {
        int updated = execute(update(columns.subList(0, 2), columns.subList(2, 3)), bindRows);
        if (updated == 0) {
          execute(insert(columns), bindRows);
        }
        reference = dump(true);
      } catch (SQLException e) {
        reference = REFUSED;
      }
      setUp.run();
      DmlSpec spec =
          DmlSpec.upsert(
              table, columns.subList(0, 2), columns.subList(2, 3), columns.subList(2, 3), false);
      return tested(reference, spec, merge, bindRows);
    }

    private List<Map<String, Object>> keyRows() {
      List<Map<String, Object>> rows = new ArrayList<>();
      rows.add(row(1, kase.value()));
      rows.add(row(2, kase.near()));
      rows.forEach(row -> row.put("note", "new"));
      return rows;
    }

    // ------------------------------------------------------------------------------ statements

    /** A plain one-row INSERT of the columns. */
    private DmlStatement insert(List<DmlColumn> columns) {
      StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" (");
      StringBuilder values = new StringBuilder();
      for (int i = 0; i < columns.size(); i++) {
        sql.append(i == 0 ? "" : ", ").append(quote(columns.get(i)));
        values.append(i == 0 ? "?" : ", ?");
      }
      sql.append(") VALUES (").append(values).append(')');
      return new DmlStatement(sql.toString(), columns, new int[columns.size()]);
    }

    /** A plain one-row UPDATE of the set columns, matched on the keys. */
    private DmlStatement update(List<DmlColumn> keys, List<DmlColumn> set) {
      StringBuilder sql = new StringBuilder("UPDATE ").append(table).append(" SET ");
      for (int i = 0; i < set.size(); i++) {
        sql.append(i == 0 ? "" : ", ").append(quote(set.get(i))).append(" = ?");
      }
      sql.append(" WHERE ");
      for (int i = 0; i < keys.size(); i++) {
        sql.append(i == 0 ? "" : " AND ").append(quote(keys.get(i))).append(" = ?");
      }
      List<DmlColumn> parameters = new ArrayList<>(set);
      parameters.addAll(keys);
      return new DmlStatement(sql.toString(), parameters, new int[parameters.size()]);
    }

    private String quote(DmlColumn column) {
      return databaseMeta.quoteField(column.column());
    }

    /** Runs the plain statement for each row, the reference for the upsert. */
    private String plain(
        DmlStatement statement, List<Map<String, Object>> rows, Runnable setUp, boolean note)
        throws Exception {
      try {
        for (Map<String, Object> row : rows) {
          execute(statement, List.of(row));
        }
        return dump(note);
      } catch (SQLException e) {
        return REFUSED;
      }
    }

    private Outcome tested(
        String reference, DmlSpec spec, DmlSyntax.Upsert upsert, List<Map<String, Object>> rows)
        throws Exception {
      DmlStatement statement = DmlStatementBuilder.upsert(databaseMeta, syntax, upsert, spec);
      try {
        execute(statement, rows);
        return new Outcome(reference, dump(hasNote(spec)), statement.getSql(), null);
      } catch (SQLException e) {
        return new Outcome(reference, REFUSED, statement.getSql(), e.getMessage());
      }
    }

    /** Runs a one-row statement with the values of the one row given. */
    private int execute(DmlStatement statement, List<Map<String, Object>> rows)
        throws SQLException {
      try (PreparedStatement prepared = connection.prepareStatement(statement.getSql())) {
        for (Map<String, Object> row : rows) {
          for (int p = 0; p < statement.getParameterCount(); p++) {
            String column = statement.getRowColumns().get(p).column();
            Object value = row.get(column.toLowerCase(Locale.ROOT));
            int index = p + 1;
            switch (column.toLowerCase(Locale.ROOT)) {
              case "id" -> prepared.setLong(index, ((Number) value).longValue());
              case "note" -> prepared.setString(index, (String) value);
              default -> kase.binder().bind(prepared, index, value);
            }
          }
        }
        return prepared.executeUpdate();
      }
    }

    // ---------------------------------------------------------------------------------- tables

    private void createValueTable(Object... values) {
      create("CREATE TABLE " + table + " (id INTEGER NOT NULL PRIMARY KEY, v " + type() + ")");
      insert(false, values);
    }

    private void createKeyTable() {
      create(
          "CREATE TABLE " + table + " (id INTEGER NOT NULL, v " + type() + ", note VARCHAR(10))");
      insert(true, kase.near(), kase.near());
    }

    private String type() {
      return kase.columnType();
    }

    private void create(String ddl) {
      try {
        drop();
        try (Statement statement = connection.createStatement()) {
          statement.execute(ddl);
        }
      } catch (SQLException e) {
        throw new IllegalStateException(ddl, e);
      }
    }

    private void insert(boolean note, Object... values) {
      String sql =
          "INSERT INTO "
              + table
              + (note ? " (id, v, note) VALUES (?, ?, 'old')" : " (id, v) VALUES (?, ?)");
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        for (int i = 0; i < values.length; i++) {
          statement.setLong(1, i + 1L);
          kase.binder().bind(statement, 2, values[i]);
          statement.executeUpdate();
        }
      } catch (SQLException e) {
        throw new IllegalStateException("The good values of " + kase + " were refused", e);
      }
    }

    void drop() {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS " + table);
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }

    /** The table as "1:v 2:v" or "1:v:note 2:v:note", ordered by id. */
    private String dump(boolean note) throws SQLException {
      List<String> rows = new ArrayList<>();
      String sql = "SELECT id, v" + (note ? ", note" : "") + " FROM " + table + " ORDER BY id";
      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          StringBuilder row = new StringBuilder();
          row.append(resultSet.getLong(1)).append(':').append(text(resultSet.getObject(2)));
          if (note) {
            row.append(':').append(resultSet.getString(3));
          }
          rows.add(row.toString());
        }
      }
      return String.join(" ", rows);
    }

    /** The key form has a note column, the value forms don't. */
    private static boolean hasNote(DmlSpec spec) {
      return spec.getSourceColumns().size() > 2;
    }

    private List<DmlColumn> columns(String... names) throws Exception {
      return DmlColumns.fromTable(
          connection.getMetaData(), databaseMeta.getIDatabase(), null, table, List.of(names));
    }
  }

  /** A stored value as text: a number without exponent, bytes in hex, a LOB by its content. */
  public static String text(Object value) throws SQLException {
    return switch (value) {
      case null -> "null";
      case BigDecimal decimal -> decimal.toPlainString();
      case byte[] bytes -> "0x" + HexFormat.of().formatHex(bytes);
      case Clob clob -> clob.getSubString(1, (int) clob.length());
      case Blob blob -> text(blob.getBytes(1, (int) blob.length()));
      default -> value.toString();
    };
  }

  private static List<Map<String, Object>> rows(Object... idsAndValues) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < idsAndValues.length; i += 2) {
      rows.add(row((Integer) idsAndValues[i], idsAndValues[i + 1]));
    }
    return rows;
  }

  private static Map<String, Object> row(int id, Object value) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("id", id);
    row.put("v", value);
    return row;
  }
}

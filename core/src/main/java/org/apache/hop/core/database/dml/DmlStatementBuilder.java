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

package org.apache.hop.core.database.dml;

import java.util.List;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;

/**
 * Writes the upsert statement of one row in the {@link UpsertForm} a dialect declares in its {@link
 * DmlSyntax}. The standard MERGE, for example:
 *
 * <pre>
 * MERGE INTO target t
 * USING (VALUES (?, ?, ?)) AS s (id, name, amount)
 * ON (t.id = s.id)
 * WHEN MATCHED [AND (changed)] THEN UPDATE SET name = s.name, amount = s.amount
 * WHEN NOT MATCHED THEN INSERT (id, name, amount) VALUES (s.id, s.name, s.amount)
 * </pre>
 *
 * <p>The MERGE reads its row from a derived table aliased {@code s}, written as the {@link
 * DmlSyntax.RowSource} says, and matches it to the target aliased {@code t}. Columns in a SET list
 * are not qualified, because PostgreSQL refuses a qualified target column there and every other
 * dialect accepts the plain name. Key columns are never updated: Oracle refuses to update a column
 * of a MERGE's ON clause.
 */
public final class DmlStatementBuilder {

  /** The alias of the inserted row in {@code INSERT ... AS new ON DUPLICATE KEY UPDATE}. */
  static final String ROW_ALIAS = "new";

  private DmlStatementBuilder() {}

  /**
   * Builds the upsert statement of a plan.
   *
   * @param databaseMeta the connection, for quoting
   * @param plan the chosen form, which must be supported
   * @param spec what the statement does
   */
  public static DmlStatement upsert(DatabaseMeta databaseMeta, DmlPlan plan, DmlSpec spec)
      throws HopDatabaseException {
    if (!plan.isSupported()) {
      throw new HopDatabaseException(plan.getUnsupportedReason());
    }
    return upsert(databaseMeta, plan.getSyntax(), plan.getUpsert(), spec);
  }

  /**
   * Builds an upsert statement.
   *
   * @param databaseMeta the connection, for quoting
   * @param syntax the dialect's declaration, for the row source and the parameter casts
   * @param upsert the upsert form
   * @param spec what the statement does
   * @throws HopDatabaseException when the form can't write the spec
   */
  public static DmlStatement upsert(
      DatabaseMeta databaseMeta, DmlSyntax syntax, DmlSyntax.Upsert upsert, DmlSpec spec)
      throws HopDatabaseException {
    if (syntax == null || upsert == null) {
      throw new HopDatabaseException("An upsert needs a DML syntax and an upsert form");
    }
    Writer writer = new Writer(databaseMeta, syntax, spec);
    return switch (upsert.form()) {
      case MERGE -> writer.merge(upsert);
      case ON_CONFLICT -> writer.onConflict();
      case ON_DUPLICATE_KEY -> writer.onDuplicateKey(upsert.rowAlias());
    };
  }

  /** Writes the statements of one spec. */
  private static final class Writer {
    private final DatabaseMeta databaseMeta;
    private final DmlSyntax syntax;
    private final DmlSpec spec;
    private final String table;

    Writer(DatabaseMeta databaseMeta, DmlSyntax syntax, DmlSpec spec) {
      this.databaseMeta = databaseMeta;
      this.syntax = syntax;
      this.spec = spec;
      this.table = spec.getQuotedTable();
    }

    DmlStatement merge(DmlSyntax.Upsert upsert) throws HopDatabaseException {
      List<DmlColumn> keys = spec.getKeys();
      List<DmlColumn> source = spec.getSourceColumns();
      List<DmlColumn> set = spec.getSetColumns();
      StringBuilder sql = new StringBuilder("MERGE INTO ").append(table).append(" t USING ");
      sql.append(source(source)).append(" ON (").append(keyMatch(keys)).append(')');
      if (!set.isEmpty()) {
        boolean updateWhere = upsert.changedCondition() == DmlSyntax.ChangedCondition.UPDATE_WHERE;
        String changed = spec.isUpdateOnlyChanged() ? changedCondition(set, "s") : null;
        sql.append(" WHEN MATCHED");
        if (changed != null && !updateWhere) {
          sql.append(" AND ").append(changed);
        }
        sql.append(" THEN UPDATE SET ").append(setFrom(set, "s"));
        if (changed != null && updateWhere) {
          sql.append(" WHERE ").append(changed);
        }
      }
      List<DmlColumn> insert = spec.getInsertColumns();
      sql.append(" WHEN NOT MATCHED THEN INSERT (").append(names(insert)).append(") VALUES (");
      for (int i = 0; i < insert.size(); i++) {
        sql.append(i == 0 ? "" : ", ").append("s.").append(quote(insert.get(i)));
      }
      sql.append(')').append(upsert.terminator());
      return statement(sql.toString(), source);
    }

    DmlStatement onConflict() throws HopDatabaseException {
      checkNoUpdateOnlyColumns(UpsertForm.ON_CONFLICT);
      List<DmlColumn> insert = spec.getInsertColumns();
      List<DmlColumn> set = spec.getSetColumns();
      String changed =
          spec.isUpdateOnlyChanged() && !set.isEmpty() ? changedCondition(set, "EXCLUDED") : null;
      StringBuilder sql = new StringBuilder("INSERT INTO ").append(table);
      // The alias is only needed to compare with the existing row.
      sql.append(changed != null ? " AS t (" : " (").append(names(insert)).append(") VALUES ");
      sql.append(plainRow(insert));
      sql.append(" ON CONFLICT (").append(names(spec.getKeys())).append(") DO ");
      if (set.isEmpty()) {
        sql.append("NOTHING");
      } else {
        sql.append("UPDATE SET ").append(setFrom(set, "EXCLUDED"));
        if (changed != null) {
          sql.append(" WHERE ").append(changed);
        }
      }
      return statement(sql.toString(), insert);
    }

    /**
     * {@code INSERT ... ON DUPLICATE KEY UPDATE c = VALUES(c)}, or with a row alias {@code INSERT
     * ... VALUES (...) AS new ON DUPLICATE KEY UPDATE c = new.c}.
     */
    DmlStatement onDuplicateKey(boolean rowAlias) throws HopDatabaseException {
      checkNoUpdateOnlyColumns(UpsertForm.ON_DUPLICATE_KEY);
      List<DmlColumn> insert = spec.getInsertColumns();
      List<DmlColumn> set = spec.getSetColumns();
      StringBuilder sql = new StringBuilder("INSERT INTO ").append(table);
      sql.append(" (").append(names(insert)).append(") VALUES ").append(plainRow(insert));
      if (rowAlias) {
        sql.append(" AS ").append(ROW_ALIAS);
      }
      sql.append(" ON DUPLICATE KEY UPDATE ");
      if (set.isEmpty()) {
        // Insert-only: an assignment that changes nothing. INSERT IGNORE would also swallow every
        // other error of the row. With a row alias a bare column name is ambiguous ("Column 'ID'
        // in field list is ambiguous"): take the inserted row's key, which is the existing row's.
        String key = quote(spec.getKeys().get(0));
        sql.append(key).append(" = ").append(rowAlias ? ROW_ALIAS + "." + key : key);
      } else {
        // A row whose values don't change is not written: MySQL skips it on its own, which is what
        // "update only changed" asks for.
        for (int i = 0; i < set.size(); i++) {
          String c = quote(set.get(i));
          sql.append(i == 0 ? "" : ", ").append(c).append(" = ");
          if (rowAlias) {
            sql.append(ROW_ALIAS).append('.').append(c);
          } else {
            sql.append("VALUES(").append(c).append(')');
          }
        }
      }
      return statement(sql.toString(), insert);
    }

    private void checkNoUpdateOnlyColumns(UpsertForm form) throws HopDatabaseException {
      List<DmlColumn> updateOnly = spec.getUpdateOnlyColumns();
      if (!updateOnly.isEmpty()) {
        throw new HopDatabaseException(
            "An upsert with "
                + form
                + " can't update column "
                + updateOnly.get(0).column()
                + ", which it doesn't insert");
      }
    }

    /** The row as a derived table aliased {@code s}, with casts when the syntax asks for them. */
    private String source(List<DmlColumn> columns) throws HopDatabaseException {
      StringBuilder source = new StringBuilder("(");
      if (syntax.rowSource() == DmlSyntax.RowSource.VALUES) {
        source.append("VALUES (");
        for (int i = 0; i < columns.size(); i++) {
          source.append(i == 0 ? "" : ", ").append(parameter(columns.get(i)));
        }
        source.append(")) AS s (").append(names(columns)).append(')');
        return source.toString();
      }
      source.append("SELECT ");
      for (int i = 0; i < columns.size(); i++) {
        DmlColumn column = columns.get(i);
        source.append(i == 0 ? "" : ", ").append(parameter(column));
        source.append(" AS ").append(quote(column));
      }
      if (syntax.rowSource() == DmlSyntax.RowSource.SELECT_FROM_DUAL) {
        source.append(" FROM dual");
      }
      return source.append(") s").toString();
    }

    /** {@code (?, ?, ?)}: a row of an INSERT, whose types come from the target. */
    private static String plainRow(List<DmlColumn> columns) {
      StringBuilder row = new StringBuilder("(");
      for (int i = 0; i < columns.size(); i++) {
        row.append(i == 0 ? "?" : ", ?");
      }
      return row.append(')').toString();
    }

    private String parameter(DmlColumn column) throws HopDatabaseException {
      if (!syntax.castParameters()) {
        return "?";
      }
      if (column.nativeType() == null || column.nativeType().isBlank()) {
        throw new HopDatabaseException(
            "The type of column " + column.column() + " is needed to cast its parameter");
      }
      return "CAST(? AS " + column.nativeType() + ")";
    }

    /** {@code t.a = s.a AND t.b = s.b} */
    private String keyMatch(List<DmlColumn> keys) {
      StringBuilder match = new StringBuilder();
      for (int i = 0; i < keys.size(); i++) {
        String c = quote(keys.get(i));
        match.append(i == 0 ? "" : " AND ").append("t.").append(c).append(" = s.").append(c);
      }
      return match.toString();
    }

    /** {@code a = s.a, b = s.b} */
    private String setFrom(List<DmlColumn> set, String sourceAlias) {
      StringBuilder assignments = new StringBuilder();
      for (int i = 0; i < set.size(); i++) {
        String c = quote(set.get(i));
        assignments.append(i == 0 ? "" : ", ").append(c);
        assignments.append(" = ").append(sourceAlias).append('.').append(c);
      }
      return assignments.toString();
    }

    /**
     * Null-safe standard SQL, comparing the target {@code t} with the source: changed when the
     * values differ or exactly one of them is NULL.
     *
     * @return the condition, or null when a column can't be compared ({@link
     *     DmlColumn#comparable()}): whether the row changed can't be told then, so every matched
     *     row is updated
     */
    private String changedCondition(List<DmlColumn> set, String source) {
      if (!spec.getUncomparableSetColumns().isEmpty()) {
        return null;
      }
      StringBuilder condition = new StringBuilder("(");
      for (int i = 0; i < set.size(); i++) {
        String c = quote(set.get(i));
        String t = "t." + c;
        String s = source + "." + c;
        condition.append(i == 0 ? "" : " OR ");
        condition.append('(').append(t).append(" <> ").append(s);
        condition.append(" OR (").append(t).append(" IS NULL AND ").append(s);
        condition.append(" IS NOT NULL) OR (").append(t).append(" IS NOT NULL AND ").append(s);
        condition.append(" IS NULL))");
      }
      return condition.append(')').toString();
    }

    private String names(List<DmlColumn> columns) {
      StringBuilder names = new StringBuilder();
      for (int i = 0; i < columns.size(); i++) {
        names.append(i == 0 ? "" : ", ").append(quote(columns.get(i)));
      }
      return names.toString();
    }

    private String quote(DmlColumn column) {
      return databaseMeta.quoteField(column.column());
    }

    private DmlStatement statement(String sql, List<DmlColumn> rowColumns) {
      int[] layout = new int[rowColumns.size()];
      for (int i = 0; i < layout.length; i++) {
        layout[i] = spec.sourceIndex(rowColumns.get(i));
      }
      return new DmlStatement(sql, rowColumns, layout);
    }
  }
}

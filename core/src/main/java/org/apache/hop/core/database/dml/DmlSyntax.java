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
import lombok.Builder;
import lombok.Singular;
import lombok.With;

/**
 * How a database writes the DML statements Hop generates beyond a plain one-row INSERT, UPDATE or
 * DELETE: its upsert forms (MERGE, {@code INSERT ... ON CONFLICT}, {@code INSERT ... ON DUPLICATE
 * KEY UPDATE}), each one row per statement, and an INSERT that carries several rows. A dialect
 * returns it from {@link org.apache.hop.core.database.IDatabase#getDmlSyntax()}; {@link
 * DmlStatementBuilder} writes the statements and {@link DmlSupport} picks the form for a
 * connection.
 *
 * <p>Declare only what a test ran against the database. An upsert form left out means a transform
 * that needs it refuses the connection with a clear reason instead of failing on the first row; a
 * multi-row INSERT left out costs speed, never correctness: Hop then inserts one row per statement.
 *
 * <p>Built with {@code builder()}, so that later options can be added without breaking the dialects
 * that declare a syntax today.
 *
 * @param rowSource how the row of parameters is written as a derived table: the source of a MERGE
 * @param castParameters whether each parameter of the derived table is written as {@code CAST(? AS
 *     type)}, with the column types read by {@link DmlColumns#fromTable} and written by {@link
 *     org.apache.hop.core.database.IDatabase#getDmlCastType}, which must not cut or round the
 *     value. The {@code VALUES} list of an {@code ON CONFLICT} or {@code ON DUPLICATE KEY} is never
 *     cast: its types come from the target columns, unless {@code castValuesLists} says so for an
 *     INSERT of several rows.
 * @param upserts the upsert forms the database has, in order of preference; empty when it has none
 * @param castValuesLists whether the parameters of the {@code VALUES} list of an INSERT of several
 *     rows are cast as well. SQL Server types such a list as one derived table, from its
 *     parameters, before it assigns the rows to the columns: a {@code BigDecimal} of 2.25 next to
 *     one of 1 is rounded to 2. Takes effect only with {@code castParameters}, which is what makes
 *     a transform read the column types.
 * @param insertForm how several rows go into one INSERT, or null to insert one row per statement
 * @param maxParameters the most parameters one statement may have, 0 for no limit
 * @param maxRowsPerStatement the most rows one statement may carry, 0 for no limit
 */
@Builder(toBuilder = true)
@With
public record DmlSyntax(
    RowSource rowSource,
    boolean castParameters,
    @Singular List<Upsert> upserts,
    boolean castValuesLists,
    InsertForm insertForm,
    int maxParameters,
    int maxRowsPerStatement) {

  public DmlSyntax {
    rowSource = rowSource == null ? RowSource.VALUES : rowSource;
    upserts = upserts == null ? List.of() : List.copyOf(upserts);
    maxParameters = Math.max(0, maxParameters);
    maxRowsPerStatement = Math.max(0, maxRowsPerStatement);
  }

  /**
   * The standard MERGE with a {@code VALUES} source and no terminator, for every server version,
   * and nothing else.
   */
  public static final DmlSyntax STANDARD_MERGE =
      DmlSyntax.builder().upsert(Upsert.of(UpsertForm.MERGE)).build();

  /**
   * @return the first declared upsert of this form, or null
   */
  public Upsert upsert(UpsertForm form) {
    for (Upsert upsert : upserts) {
      if (upsert.form() == form) {
        return upsert;
      }
    }
    return null;
  }

  /**
   * @return every declared upsert of this form, in order of preference; a database can declare one
   *     form twice, for example for two ranges of server versions
   */
  public List<Upsert> upserts(UpsertForm form) {
    return upserts.stream().filter(upsert -> upsert.form() == form).toList();
  }

  /**
   * One upsert form of a database, with what Hop needs to write it.
   *
   * @param form the statement
   * @param minimumMajorVersion the first major server version that has it, 0 for every version
   * @param minimumMinorVersion the first minor version within that major version, 0 for any
   * @param changedCondition where a MERGE writes the condition of "update only changed rows"
   * @param terminator written after a MERGE, for example {@code ";"} on SQL Server; null or empty
   *     for none
   * @param rowAlias for {@code ON DUPLICATE KEY}: name the inserted row with a row alias, {@code
   *     INSERT ... VALUES (...) AS new ON DUPLICATE KEY UPDATE c = new.c} (MySQL 8.0.19 and later),
   *     instead of the {@code VALUES(c)} function, which MySQL deprecated in 8.0.20. Declare both
   *     as two entries, the alias first with its minimum version, and the server picks.
   */
  @With
  public record Upsert(
      UpsertForm form,
      int minimumMajorVersion,
      int minimumMinorVersion,
      ChangedCondition changedCondition,
      String terminator,
      boolean rowAlias) {

    public Upsert {
      changedCondition =
          changedCondition == null ? ChangedCondition.WHEN_MATCHED_AND : changedCondition;
      terminator = terminator == null ? "" : terminator;
    }

    /** The form for every server version, without a terminator or a row alias. */
    public static Upsert of(UpsertForm form) {
      return new Upsert(form, 0, 0, ChangedCondition.WHEN_MATCHED_AND, "", false);
    }

    /**
     * @return the minimum version as people write it: "15", or "9.5" with a minor version
     */
    public String minimumVersion() {
      return minimumMinorVersion > 0
          ? minimumMajorVersion + "." + minimumMinorVersion
          : Integer.toString(minimumMajorVersion);
    }
  }

  /** Where the condition of an update of changed rows only is written in a MERGE. */
  public enum ChangedCondition {
    /** {@code WHEN MATCHED AND (changed) THEN UPDATE SET ...}: standard SQL. */
    WHEN_MATCHED_AND,
    /** {@code WHEN MATCHED THEN UPDATE SET ... WHERE (changed)}: Oracle. */
    UPDATE_WHERE
  }

  /** How the row of parameters is written as a derived table, aliased {@code s}. */
  public enum RowSource {
    /** {@code (VALUES (?, ?)) AS s (a, b)}: SQL Server, PostgreSQL, DB2, H2, DuckDB. */
    VALUES,
    /** {@code (SELECT ? AS a, ? AS b) s}: databases that can't name the columns of VALUES. */
    SELECT,
    /** {@code (SELECT ? AS a, ? AS b FROM dual) s}: Oracle. */
    SELECT_FROM_DUAL
  }

  /** How several rows go into one INSERT. */
  public enum InsertForm {
    /** {@code INSERT INTO t (a, b) VALUES (?, ?), (?, ?)}. */
    VALUES_LIST
  }
}

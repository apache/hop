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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.MultiRowBatch;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IRowMeta;

/**
 * The INSERT statements of one {@link DmlSpec} on one connection, for every number of rows a {@link
 * MultiRowBatch} asks for: built once per size with {@link DmlStatementBuilder}, prepared on the
 * connection and bound from bind rows that hold one value per source column.
 */
public final class DmlStatements implements MultiRowBatch.IStatements {

  /** Sets one parameter of a statement. */
  @FunctionalInterface
  public interface IParameterSetter {
    /**
     * @param statement the statement
     * @param parameterIndex the JDBC parameter index, one-based
     * @param sourceIndex the position of the value's column in the spec's source columns
     * @param value the value
     */
    void set(PreparedStatement statement, int parameterIndex, int sourceIndex, Object value)
        throws HopDatabaseException;
  }

  private final DatabaseMeta databaseMeta;
  private final DmlPlan plan;
  private final DmlSpec spec;
  private final Connection connection;
  private final IParameterSetter setter;
  private final Map<Integer, DmlStatement> built = new HashMap<>();

  /**
   * @param databaseMeta the connection, for quoting
   * @param plan the chosen forms, with the rows per statement
   * @param spec what the statements do: an INSERT
   * @param connection the open connection to prepare the statements on
   * @param setter sets one parameter from one value of a bind row
   */
  public DmlStatements(
      DatabaseMeta databaseMeta,
      DmlPlan plan,
      DmlSpec spec,
      Connection connection,
      IParameterSetter setter) {
    if (spec.getOperation() != DmlOperation.INSERT) {
      throw new IllegalArgumentException("Only an INSERT carries several rows: " + spec);
    }
    this.databaseMeta = databaseMeta;
    this.plan = plan;
    this.spec = spec;
    this.connection = connection;
    this.setter = setter;
  }

  /**
   * The statements on an open {@link Database}, bound the way Hop binds every value: through the
   * dialect's value bindings.
   *
   * @param database the connected database
   * @param plan the chosen forms, with the rows per statement
   * @param spec what the statements do: an INSERT
   * @param bindRowMeta one value per source column of the spec, in that order
   */
  public static DmlStatements of(
      Database database, DmlPlan plan, DmlSpec spec, IRowMeta bindRowMeta) {
    // The bindings are resolved once for the layout, not for every value of every row.
    Database.RowValueWriter writer = database.rowValueWriter(bindRowMeta);
    return new DmlStatements(
        database.getDatabaseMeta(),
        plan,
        spec,
        database.getConnection(),
        (statement, parameterIndex, sourceIndex, value) ->
            writer.setValue(statement, sourceIndex, value, parameterIndex));
  }

  /**
   * A {@link MultiRowBatch} that writes these statements on an open {@link Database}.
   *
   * @param database the connected database, not in auto-commit mode nor in a connection group
   * @param support the resolved batch support of the connection
   */
  public MultiRowBatch newBatch(Database database, BatchSupport support)
      throws HopDatabaseException {
    return MultiRowBatch.of(database, this, support);
  }

  /**
   * @return the statement for this many rows, built once
   */
  public DmlStatement statement(int rows) throws HopDatabaseException {
    DmlStatement statement = built.get(rows);
    if (statement == null) {
      statement = DmlStatementBuilder.insert(databaseMeta, plan, spec, rows);
      built.put(rows, statement);
    }
    return statement;
  }

  @Override
  public int getRowsPerStatement() {
    return plan.getRowsPerStatement();
  }

  /**
   * The plan's maximum bytes per statement, less the statement's own text and room for the protocol
   * around it.
   */
  @Override
  public long getMaxStatementBytes() {
    long max = plan.getMaxStatementBytes();
    if (max <= 0) {
      return 0;
    }
    long head;
    try {
      head = statement(1).getSql().getBytes(StandardCharsets.UTF_8).length;
    } catch (HopDatabaseException e) {
      head = 0;
    }
    return Math.max(1, max - head - STATEMENT_RESERVE_BYTES);
  }

  /**
   * At least the bytes a row adds to a statement, whether the driver sends its values in the
   * statement's text, quoted and escaped (MySQL's Connector/J by default), or in a binary protocol
   * with a length and a type per parameter: every value, and the separators and per-parameter
   * overhead of either form.
   */
  @Override
  public long estimateBytes(Object[] bindRow) {
    long bytes = ROW_OVERHEAD_BYTES;
    for (Object value : bindRow) {
      bytes += PARAMETER_OVERHEAD_BYTES + valueBytes(value);
    }
    return bytes;
  }

  /** Packet headers, command bytes and the null bitmap of a binary execution. */
  private static final long STATEMENT_RESERVE_BYTES = 1024;

  /** The parentheses and comma of a row in a VALUES list. */
  private static final long ROW_OVERHEAD_BYTES = 4;

  /** A separator in the text, or the type and the longest length prefix of a binary parameter. */
  private static final long PARAMETER_OVERHEAD_BYTES = 12;

  /**
   * At least the bytes a value takes when it is written as an SQL literal in UTF-8, its quotes and
   * escapes included; written in a binary protocol it takes no more.
   *
   * @param value a value of a bind row
   * @return an upper bound of its size in bytes
   */
  static long valueBytes(Object value) {
    if (value == null) {
      return 4;
    }
    if (value instanceof String string) {
      return textBytes(string);
    }
    if (value instanceof byte[] binary) {
      // Every byte escaped, with a prefix such as _binary'...'.
      return 2L * binary.length + 16;
    }
    if (value instanceof BigDecimal decimal) {
      return decimal.precision() + Math.abs((long) decimal.scale()) + 8;
    }
    if (value instanceof Number || value instanceof Boolean) {
      return 32;
    }
    if (value instanceof java.util.Date) {
      return 40;
    }
    return textBytes(String.valueOf(value));
  }

  /**
   * The UTF-8 bytes of a quoted string literal, every character that may be escaped counted as 2.
   */
  private static long textBytes(String text) {
    long bytes = 2;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c < 0x80) {
        bytes += isEscaped(c) ? 2 : 1;
      } else if (c < 0x800 || Character.isSurrogate(c)) {
        // A surrogate pair is 4 bytes in UTF-8.
        bytes += 2;
      } else {
        bytes += 3;
      }
    }
    return bytes;
  }

  private static boolean isEscaped(char c) {
    return c == '\'' || c == '"' || c == '\\' || c == 0 || c == '\n' || c == '\r' || c == 0x1A;
  }

  @Override
  public PreparedStatement prepare(int rows) throws SQLException, HopDatabaseException {
    return connection.prepareStatement(statement(rows).getSql());
  }

  @Override
  public void bind(PreparedStatement statement, int rows, int row, Object[] bindRow)
      throws HopDatabaseException {
    DmlStatement dml = statement(rows);
    int perRow = dml.getParametersPerRow();
    for (int position = 0; position < perRow; position++) {
      int sourceIndex = dml.sourceIndex(position);
      setter.set(statement, dml.parameterIndex(row, position), sourceIndex, bindRow[sourceIndex]);
    }
  }

  public DmlPlan getPlan() {
    return plan;
  }

  public DmlSpec getSpec() {
    return spec;
  }
}

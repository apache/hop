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
package org.apache.hop.core.database.types;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * A database column described independently of which JDBC metadata API it came from.
 *
 * <p>JDBC exposes column metadata two different ways: {@link ResultSetMetaData}, from a query or a
 * prepared statement, and a row of {@link java.sql.DatabaseMetaData#getColumns}. Hop reads both,
 * and historically carried a separate copy of the type mapping for each, which is how they drifted
 * apart. Normalizing to this one shape is what lets a single mapper serve both.
 *
 * <p>Instances are immutable.
 */
public final class DatabaseColumn {

  private final String name;
  private final String tableName;
  private final int sqlType;
  private final String nativeTypeName;
  private final int precision;
  private final int scale;
  private final int displaySize;
  private final boolean signed;
  private final String comment;

  /**
   * The result set metadata this column came from, or null when it came from a getColumns() row.
   * Only kept so that {@code IDatabase.customizeValueFromSqlType} can still be handed the raw JDBC
   * metadata it takes today; nothing else should reach for it.
   */
  private final ResultSetMetaData resultSetMetaData;

  private final int columnIndex;

  private DatabaseColumn(
      String name,
      String tableName,
      int sqlType,
      String nativeTypeName,
      int precision,
      int scale,
      int displaySize,
      boolean signed,
      String comment,
      ResultSetMetaData resultSetMetaData,
      int columnIndex) {
    this.name = name;
    this.tableName = tableName;
    this.sqlType = sqlType;
    this.nativeTypeName = nativeTypeName;
    this.precision = precision;
    this.scale = scale;
    this.displaySize = displaySize;
    this.signed = signed;
    this.comment = comment;
    this.resultSetMetaData = resultSetMetaData;
    this.columnIndex = columnIndex;
  }

  /** Describes column {@code index} (1-based) of the given result set metadata. */
  public static DatabaseColumn of(ResultSetMetaData rm, int index) throws SQLException {
    return of(rm, index, rm.getColumnName(index));
  }

  /**
   * Describes column {@code index} (1-based) under a caller-supplied name. Callers that resolve the
   * name themselves (MySQL's legacy column naming, for example) use this overload.
   */
  public static DatabaseColumn of(ResultSetMetaData rm, int index, String name)
      throws SQLException {
    return new DatabaseColumn(
        name,
        readTableName(rm, index),
        rm.getColumnType(index),
        rm.getColumnTypeName(index),
        rm.getPrecision(index),
        rm.getScale(index),
        rm.getColumnDisplaySize(index),
        readSigned(rm, index),
        rm.getColumnLabel(index),
        rm,
        index);
  }

  /**
   * Describes the column on the current row of a {@link java.sql.DatabaseMetaData#getColumns}
   * result set.
   *
   * <p>That API reports neither display size nor signedness. Display size falls back to {@code
   * COLUMN_SIZE}; signedness falls back to true, because every SQL dialect's BIGINT is signed
   * unless explicitly declared otherwise (unsigned integers are a MySQL extension). Assuming
   * unsigned here would needlessly widen every BIGINT to a BigNumber.
   */
  public static DatabaseColumn ofColumnsRow(ResultSet columnsRow) throws SQLException {
    int columnSize = columnsRow.getInt("COLUMN_SIZE");
    Object decimalDigits = columnsRow.getObject("DECIMAL_DIGITS");
    return new DatabaseColumn(
        columnsRow.getString("COLUMN_NAME"),
        columnsRow.getString("TABLE_NAME"),
        columnsRow.getInt("DATA_TYPE"),
        columnsRow.getString("TYPE_NAME"),
        columnSize,
        decimalDigits == null ? 0 : columnsRow.getInt("DECIMAL_DIGITS"),
        columnSize,
        true,
        columnsRow.getString("REMARKS"),
        null,
        -1);
  }

  /** Not every JDBC driver implements getTableName(); those that don't report no table. */
  private static String readTableName(ResultSetMetaData rm, int index) {
    try {
      return rm.getTableName(index);
    } catch (Exception ignored) {
      // This JDBC driver doesn't support the getTableName method. Nothing more we can do here.
      return null;
    }
  }

  /** Not every JDBC driver implements isSigned(); those that don't are treated as unsigned. */
  private static boolean readSigned(ResultSetMetaData rm, int index) {
    try {
      return rm.isSigned(index);
    } catch (Exception ignored) {
      // This JDBC driver doesn't support the isSigned method. Nothing more we can do here.
      return false;
    }
  }

  public String getName() {
    return name;
  }

  /**
   * The table this column belongs to, empty or null when the column is an expression rather than a
   * column of a table. A dialect whose driver types a column by looking at its data needs this to
   * tell "the driver could not type this expression" from "this is a real column of that type".
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @return the {@link java.sql.Types} constant reported for this column.
   */
  public int getSqlType() {
    return sqlType;
  }

  /**
   * @return the database's own name for the type, e.g. NUMBER, JSONB, SDO_GEOMETRY, YEAR.
   */
  public String getNativeTypeName() {
    return nativeTypeName;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  public int getDisplaySize() {
    return displaySize;
  }

  public boolean isSigned() {
    return signed;
  }

  public String getComment() {
    return comment;
  }

  public ResultSetMetaData getResultSetMetaData() {
    return resultSetMetaData;
  }

  public int getColumnIndex() {
    return columnIndex;
  }
}

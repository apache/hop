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
import java.util.Locale;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

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
  private final boolean autoIncrement;

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
      boolean autoIncrement,
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
    this.autoIncrement = autoIncrement;
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
        readAutoIncrement(rm, index),
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
    boolean autoIncrement = false;
    try {
      autoIncrement = "YES".equalsIgnoreCase(columnsRow.getString("IS_AUTOINCREMENT"));
    } catch (Exception ignored) {
      // Driver-dependent.
    }
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
        autoIncrement,
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

  /**
   * Not every JDBC driver implements isAutoIncrement(); those that don't are treated as not auto
   * increment.
   */
  private static boolean readAutoIncrement(ResultSetMetaData rm, int index) {
    try {
      return rm.isAutoIncrement(index);
    } catch (Exception ignored) {
      // This JDBC driver doesn't support the isAutoIncrement method.
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

  public boolean isAutoIncrement() {
    return autoIncrement;
  }

  public ResultSetMetaData getResultSetMetaData() {
    return resultSetMetaData;
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  /**
   * Describes a column when the caller already has JDBC type information and is not holding a live
   * {@link ResultSet} or {@link ResultSetMetaData}. Used by value-constraint loading.
   */
  public static DatabaseColumn of(
      String name, int sqlType, String nativeTypeName, int precision, int scale, int displaySize) {
    return of(name, sqlType, nativeTypeName, precision, scale, displaySize, false);
  }

  /**
   * Describes a column with an explicit auto increment indicator when the caller already has JDBC
   * type information.
   */
  public static DatabaseColumn of(
      String name,
      int sqlType,
      String nativeTypeName,
      int precision,
      int scale,
      int displaySize,
      boolean autoIncrement) {
    return new DatabaseColumn(
        name,
        null,
        sqlType,
        nativeTypeName,
        precision,
        scale,
        displaySize,
        true,
        null,
        autoIncrement,
        null,
        -1);
  }

  /**
   * The definition of this column as defined in the database (for example: varchar(100), bigint,
   * int identity, bool, float, timestamp(6), numeric(10, 2)).
   */
  public String getDefinition() {
    return calculateDefinition(
        nativeTypeName, precision, scale, displaySize > 0 ? displaySize : precision);
  }

  /**
   * Calculates the database column definition for an {@link IValueMeta} using whatever original
   * database metadata was attached to it.
   */
  public static String calculateDefinition(IValueMeta valueMeta) {
    if (valueMeta == null) {
      return "";
    }
    String typeName = valueMeta.getOriginalColumnTypeName();
    if (Utils.isEmpty(typeName)) {
      return Const.NVL(valueMeta.getTypeDesc(), "");
    }
    int precision =
        valueMeta.getLength() > 0 ? valueMeta.getLength() : valueMeta.getOriginalPrecision();
    int scale;
    if (valueMeta.getType() == IValueMeta.TYPE_TIMESTAMP
        || valueMeta.getType() == IValueMeta.TYPE_DATE) {
      scale =
          valueMeta.getOriginalScale() > 0 ? valueMeta.getOriginalScale() : valueMeta.getLength();
    } else {
      scale =
          valueMeta.getPrecision() >= 0 ? valueMeta.getPrecision() : valueMeta.getOriginalScale();
    }
    int length = valueMeta.getLength();
    return calculateDefinition(typeName, precision, scale, length);
  }

  /**
   * Formats the database column data type definition given the native type name and column
   * dimensions.
   */
  public static String calculateDefinition(
      String nativeTypeName, int precision, int scale, int length) {
    if (Utils.isEmpty(nativeTypeName)) {
      return "";
    }
    String trimmed = nativeTypeName.trim();
    if (trimmed.contains("(")) {
      return trimmed;
    }

    String upper = trimmed.toUpperCase(Locale.ROOT);

    // 1. Types that do not take length/precision
    if (isIntegerType(upper)
        || isFloatingPointType(upper)
        || isBooleanType(upper)
        || isUnsizedType(upper)) {
      return trimmed;
    }

    // 2. String/Character, Binary, and Bit types: VARCHAR, CHAR, NVARCHAR, NCHAR, VARCHAR2,
    // NVARCHAR2,
    // BPCHAR, BINARY, VARBINARY, RAW, BIT
    if (isSizedStringType(upper) || isSizedBinaryType(upper) || isSizedBitType(upper)) {
      int size = precision > 0 ? precision : length;
      if (upper.equals("BIT") && size <= 1) {
        return trimmed;
      }
      if (size > 0 && size < DatabaseMeta.CLOB_LENGTH) {
        return trimmed + "(" + size + ")";
      }
      return trimmed;
    }

    // 3. Decimal/Numeric types: DECIMAL, NUMERIC, NUMBER, DEC
    if (isDecimalType(upper)) {
      int p = precision > 0 ? precision : length;
      if (p > 0 && p <= 1000) {
        if (scale > 0) {
          return trimmed + "(" + p + ", " + scale + ")";
        } else if (scale == 0) {
          return trimmed + "(" + p + ")";
        }
      }
      return trimmed;
    }

    // 4. Timestamp / Time / DateTime types: TIMESTAMP, TIMESTAMPTZ, TIME, TIMETZ, DATETIME2
    if (isDateTimeWithPrecision(upper)) {
      int fracSec =
          (scale >= 0 && scale <= 9) ? scale : ((length >= 0 && length <= 9) ? length : -1);
      if (fracSec > 0) {
        int tzIndex = -1;
        if (upper.contains(" WITHOUT TIME ZONE")) {
          tzIndex = upper.indexOf(" WITHOUT TIME ZONE");
        } else if (upper.contains(" WITH LOCAL TIME ZONE")) {
          tzIndex = upper.indexOf(" WITH LOCAL TIME ZONE");
        } else if (upper.contains(" WITH TIME ZONE")) {
          tzIndex = upper.indexOf(" WITH TIME ZONE");
        }
        if (tzIndex >= 0) {
          return trimmed.substring(0, tzIndex) + "(" + fracSec + ")" + trimmed.substring(tzIndex);
        } else {
          return trimmed + "(" + fracSec + ")";
        }
      }
      return trimmed;
    }

    return trimmed;
  }

  private static boolean isIntegerType(String upper) {
    return upper.equals("BIGINT")
        || upper.startsWith("BIGINT ")
        || upper.equals("INT")
        || upper.startsWith("INT ")
        || upper.equals("INTEGER")
        || upper.startsWith("INTEGER ")
        || upper.equals("SMALLINT")
        || upper.startsWith("SMALLINT ")
        || upper.equals("TINYINT")
        || upper.startsWith("TINYINT ")
        || upper.equals("MEDIUMINT")
        || upper.startsWith("MEDIUMINT ")
        || upper.equals("INT2")
        || upper.equals("INT4")
        || upper.equals("INT8")
        || upper.equals("INT16")
        || upper.equals("INT32")
        || upper.equals("INT64")
        || upper.equals("UINT")
        || upper.equals("SERIAL")
        || upper.equals("BIGSERIAL")
        || upper.equals("SMALLSERIAL");
  }

  private static boolean isFloatingPointType(String upper) {
    return upper.equals("FLOAT")
        || upper.startsWith("FLOAT ")
        || upper.equals("DOUBLE")
        || upper.startsWith("DOUBLE ")
        || upper.equals("REAL")
        || upper.equals("DOUBLE PRECISION")
        || upper.equals("FLOAT4")
        || upper.equals("FLOAT8")
        || upper.equals("BINARY_FLOAT")
        || upper.equals("BINARY_DOUBLE");
  }

  private static boolean isBooleanType(String upper) {
    return upper.equals("BOOL") || upper.equals("BOOLEAN");
  }

  private static boolean isSizedBitType(String upper) {
    return upper.equals("BIT") || upper.equals("BIT VARYING") || upper.equals("VARBIT");
  }

  private static boolean isUnsizedType(String upper) {
    return upper.equals("TEXT")
        || upper.equals("TINYTEXT")
        || upper.equals("MEDIUMTEXT")
        || upper.equals("LONGTEXT")
        || upper.equals("CLOB")
        || upper.equals("NCLOB")
        || upper.equals("BLOB")
        || upper.equals("BYTEA")
        || upper.equals("IMAGE")
        || upper.equals("JSON")
        || upper.equals("JSONB")
        || upper.equals("XML")
        || upper.equals("UUID")
        || upper.equals("UNIQUEIDENTIFIER")
        || upper.equals("MONEY")
        || upper.equals("SMALLMONEY")
        || upper.equals("DATETIME")
        || upper.equals("SMALLDATETIME")
        || upper.equals("ROWVERSION")
        || upper.equals("DATE")
        || upper.equals("GEOMETRY")
        || upper.equals("GEOGRAPHY")
        || upper.equals("HIERARCHYID")
        || upper.equals("SQL_VARIANT")
        || upper.equals("SYSNAME");
  }

  private static boolean isSizedStringType(String upper) {
    return upper.equals("VARCHAR")
        || upper.equals("CHAR")
        || upper.equals("CHARACTER")
        || upper.equals("CHARACTER VARYING")
        || upper.equals("VARCHAR2")
        || upper.equals("NVARCHAR")
        || upper.equals("NVARCHAR2")
        || upper.equals("NCHAR")
        || upper.equals("BPCHAR");
  }

  private static boolean isSizedBinaryType(String upper) {
    return upper.equals("BINARY") || upper.equals("VARBINARY") || upper.equals("RAW");
  }

  private static boolean isDecimalType(String upper) {
    return upper.equals("DECIMAL")
        || upper.equals("NUMERIC")
        || upper.equals("NUMBER")
        || upper.equals("DEC");
  }

  private static boolean isDateTimeWithPrecision(String upper) {
    return upper.equals("TIMESTAMP")
        || upper.startsWith("TIMESTAMP ")
        || upper.equals("TIMESTAMPTZ")
        || upper.startsWith("TIMESTAMPTZ ")
        || upper.equals("DATETIME2")
        || upper.startsWith("DATETIME2 ")
        || upper.equals("TIME")
        || upper.startsWith("TIME ")
        || upper.equals("TIMETZ")
        || upper.startsWith("TIMETZ ");
  }
}

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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.util.Utils;

/**
 * Reads the native types of target table columns from the JDBC metadata, for dialects that cast
 * their parameters ({@link DmlSyntax#castParameters()}). The dialect writes each type as a cast
 * target, see {@link IDatabase#getDmlCastType}.
 */
public final class DmlColumns {

  /** Longer than this, a character or binary length is "unbounded" and left out of the cast. */
  static final int MAX_CAST_LENGTH = 1_000_000;

  /** More digits than this is an unconstrained number, written without precision. */
  static final int MAX_CAST_PRECISION = 1000;

  /** Longer than this, a SQL Server character or binary column is a {@code (max)} column. */
  static final int SQL_SERVER_MAX_LENGTH = 8000;

  /**
   * Type names that never take a length or precision, although the driver reports one: SQL Server's
   * {@code money(19,4)} or {@code uniqueidentifier(36)} would be a syntax error.
   */
  private static final Set<String> UNSIZED_TYPES =
      Set.of("money", "smallmoney", "uniqueidentifier", "sysname", "datetimeoffset");

  private DmlColumns() {}

  /**
   * Looks up the given columns of a table on an open connection.
   *
   * @param database the open connection
   * @param schema the schema of the table, unquoted; null or empty for the default
   * @param table the table name, unquoted
   * @param columnNames the columns to look up, unquoted
   * @return a column with its native type for every name, in the order given, named as the table
   *     names it
   * @throws HopDatabaseException if the table or one of the columns isn't found
   */
  public static List<DmlColumn> fromTable(
      Database database, String schema, String table, List<String> columnNames)
      throws HopDatabaseException {
    try {
      return fromTable(
          database.getConnection().getMetaData(),
          database.getDatabaseMeta().getIDatabase(),
          schema,
          table,
          columnNames);
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to read the columns of table " + table, e);
    }
  }

  /**
   * Looks up the given columns of a table in the JDBC metadata. The table and schema are tried as
   * given, then in upper and in lower case, since the metadata lookup is case-sensitive and
   * databases store unquoted names differently; a table stored in another case is found too, and
   * column names match ignoring case when no column has the exact name. Only the one table the
   * statement writes to is read: without a schema, the one in the connection's current schema.
   *
   * @param metaData the metadata of an open connection
   * @param dialect writes each column type as a cast target, see {@link IDatabase#getDmlCastType},
   *     and tells which types can be compared, see {@link IDatabase#isDmlComparable}; null for
   *     {@link #nativeType(String, int, int, int)} and {@link #comparable(String, int)}
   * @param schema the schema of the table, unquoted; null or empty for the connection's current
   *     schema
   * @param table the table name, unquoted
   * @param columnNames the columns to look up, unquoted
   * @return a column with its native type for every name, in the order given, named as the table
   *     names it
   * @throws HopDatabaseException if the table or one of the columns isn't found, or if without a
   *     schema the table is in several schemas and not in the current one
   */
  public static List<DmlColumn> fromTable(
      DatabaseMetaData metaData,
      IDatabase dialect,
      String schema,
      String table,
      List<String> columnNames)
      throws HopDatabaseException {
    Map<String, DmlColumn> columns;
    try {
      columns = readColumns(metaData, dialect, Utils.isEmpty(schema) ? null : schema, table);
    } catch (DmlTable.AmbiguousTableException e) {
      throw new HopDatabaseException(e.getMessage(), e);
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to read the columns of table " + table, e);
    }
    if (columns.isEmpty()) {
      throw new HopDatabaseException(
          "Table "
              + (Utils.isEmpty(schema) ? "" : schema + ".")
              + table
              + " was not found in the database metadata");
    }
    List<DmlColumn> result = new ArrayList<>();
    for (String name : columnNames) {
      DmlColumn column = columns.get(name);
      if (column == null) {
        for (DmlColumn candidate : columns.values()) {
          if (candidate.column().equalsIgnoreCase(name)) {
            column = candidate;
            break;
          }
        }
      }
      if (column == null) {
        throw new HopDatabaseException("Column " + name + " was not found in table " + table);
      }
      result.add(column);
    }
    return result;
  }

  private static Map<String, DmlColumn> readColumns(
      DatabaseMetaData metaData, IDatabase dialect, String schema, String table)
      throws SQLException {
    Map<String, DmlColumn> columns = new LinkedHashMap<>();
    DmlTable.Found found = DmlTable.find(metaData, schema, table);
    if (found == null) {
      return columns;
    }
    for (DmlTable.ColumnRow row : found.columns()) {
      String castType =
          dialect == null
              ? nativeType(row.typeName(), row.dataType(), row.columnSize(), row.decimalDigits())
              : dialect.getDmlCastType(
                  row.typeName(), row.dataType(), row.columnSize(), row.decimalDigits());
      boolean comparable =
          dialect == null
              ? comparable(row.typeName(), row.dataType())
              : dialect.isDmlComparable(row.typeName(), row.dataType());
      columns.putIfAbsent(row.name(), new DmlColumn(row.name(), castType, comparable));
    }
    return columns;
  }

  /**
   * Whether standard SQL can compare two values of a column with {@code <>}, from what {@link
   * DatabaseMetaData#getColumns} reports: not for large objects, XML, structured and Java object
   * types. The default of {@link IDatabase#isDmlComparable}; dialects add the types of their own
   * that can't be compared.
   *
   * @param typeName TYPE_NAME
   * @param dataType DATA_TYPE, a {@link Types} constant
   * @return false when {@code a <> b} is an error on this type
   */
  public static boolean comparable(String typeName, int dataType) {
    return switch (dataType) {
      case Types.CLOB,
              Types.NCLOB,
              Types.BLOB,
              Types.SQLXML,
              Types.STRUCT,
              Types.JAVA_OBJECT,
              Types.REF,
              Types.DATALINK ->
          false;
      default -> true;
    };
  }

  /**
   * @return whether the type name, ignoring case and a schema in front of it ({@code SYS.XMLTYPE}),
   *     is one of the names given; for the dialects' {@link IDatabase#isDmlComparable}
   */
  public static boolean isOneOf(String typeName, Set<String> lowerCaseNames) {
    if (typeName == null) {
      return false;
    }
    String name = typeName.trim().toLowerCase(Locale.ROOT);
    int dot = name.lastIndexOf('.');
    return lowerCaseNames.contains(name)
        || (dot >= 0 && lowerCaseNames.contains(name.substring(dot + 1)));
  }

  /**
   * Writes a column type as a cast target, from what {@link DatabaseMetaData#getColumns} reports:
   * the type name with its length for character and binary types, and with its precision and scale
   * for exact numbers. A type without a length in a cast can mean a length of one (CHAR in most
   * databases) or a scale of zero (DECIMAL in DB2 and Derby), which would truncate the value before
   * it reaches the table.
   *
   * @param typeName TYPE_NAME
   * @param dataType DATA_TYPE, a {@link Types} constant
   * @param columnSize COLUMN_SIZE
   * @param decimalDigits DECIMAL_DIGITS, or -1 when the driver reports none
   * @return the type to cast a parameter to, for example {@code VARCHAR(50)} or {@code
   *     NUMERIC(10,2)}
   */
  public static String nativeType(
      String typeName, int dataType, int columnSize, int decimalDigits) {
    return nativeType(typeName, dataType, columnSize, decimalDigits, false);
  }

  /**
   * As {@link #nativeType(String, int, int, int)}, with SQL Server's spelling when asked for: an
   * unbounded {@code varchar}, {@code nvarchar} or {@code varbinary} is cast to {@code (max)},
   * since SQL Server reads a cast to {@code varchar} without a length as {@code varchar(30)}. The
   * SQL Server dialects build their cast types on it, see {@link IDatabase#getDmlCastType}.
   *
   * @param sqlServer true for a SQL Server connection
   */
  public static String nativeType(
      String typeName, int dataType, int columnSize, int decimalDigits, boolean sqlServer) {
    if (Utils.isEmpty(typeName)) {
      return null;
    }
    String type = typeName.trim();
    // SQL Server and Sybase report an identity column as "int identity".
    if (type.toLowerCase(Locale.ROOT).endsWith(" identity")) {
      type = type.substring(0, type.length() - " identity".length()).trim();
    }
    String lower = type.toLowerCase(Locale.ROOT);
    // Types that SQL Server reports with a length or precision they can't be written with.
    if (UNSIZED_TYPES.contains(lower)) {
      return type;
    }
    if (sqlServer
        && columnSize > SQL_SERVER_MAX_LENGTH
        && (lower.equals("varchar") || lower.equals("nvarchar") || lower.equals("varbinary"))) {
      return type + "(max)";
    }
    // PostgreSQL reports serial columns by their pseudo-type, which is not a type to cast to.
    switch (type.toLowerCase(Locale.ROOT)) {
      case "serial", "serial4" -> type = "int4";
      case "bigserial", "serial8" -> type = "int8";
      case "smallserial", "serial2" -> type = "int2";
      default -> {
        // a real type
      }
    }

    String modifier = null;
    switch (dataType) {
      case Types.CHAR,
          Types.VARCHAR,
          Types.NCHAR,
          Types.NVARCHAR,
          Types.BINARY,
          Types.VARBINARY -> {
        if (columnSize > 0 && columnSize <= MAX_CAST_LENGTH) {
          modifier = "(" + columnSize + ")";
        }
      }
      case Types.DECIMAL, Types.NUMERIC -> {
        if (columnSize > 0 && columnSize <= MAX_CAST_PRECISION) {
          modifier = "(" + columnSize + "," + Math.max(decimalDigits, 0) + ")";
        }
      }
      case Types.CLOB, Types.NCLOB, Types.BLOB -> {
        // A large object without a length has a default one: 1M in DB2, which would refuse a
        // larger value that the column takes. Keep the column's length, however large.
        if (columnSize > 0) {
          modifier = "(" + columnSize + ")";
        }
      }
      default -> {
        // DB2's DBCLOB, whatever type number the driver reports for it.
        if (columnSize > 0 && "dbclob".equals(lower)) {
          modifier = "(" + columnSize + ")";
        }
      }
    }
    if (modifier == null) {
      return type.replace("()", "").replaceAll("\\s+", " ").trim();
    }
    if (type.contains("()")) {
      // Derby: "VARCHAR () FOR BIT DATA"
      return type.replace(" ()", modifier).replace("()", modifier);
    }
    if (type.contains("(")) {
      return type;
    }
    return type + modifier;
  }
}

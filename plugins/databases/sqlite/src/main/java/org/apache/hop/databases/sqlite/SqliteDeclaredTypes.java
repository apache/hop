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

package org.apache.hop.databases.sqlite;

import java.sql.Types;
import java.util.Locale;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/**
 * The JDBC type of a SQLite table column, taken from its declared type alone.
 *
 * <p>The xerial driver answers {@code getColumnType()} from the declared type and the storage class
 * of the value on the current row together. Before a query runs there is no row and the declared
 * type decides; once it runs, a value the declared type does not expect wins: a column declared
 * NUMERIC reads as INTEGER when the first row holds 42, as NUMERIC when it holds null, and as
 * VARCHAR when it holds text. One column, three types, depending on the data.
 *
 * <p>Hop describes a query twice, once from the prepared statement to show the fields and once from
 * the result to read the rows, and the two have to agree. A transform that takes its fields from
 * the first and its values from the second otherwise gets a Long in a Number field. See issue
 * #3633.
 *
 * <p>So the type a table column is read as only depends on its declared type: see {@link
 * #jdbcType}. An expression column is left alone: it has no declared type to go by, only its data.
 */
final class SqliteDeclaredTypes {

  private SqliteDeclaredTypes() {
    // Utility class.
  }

  /** Puts the declared type's JDBC type back on a table column the driver typed from its data. */
  static final IDatabaseTypeRule RULE =
      new IDatabaseTypeRule() {
        @Override
        public DatabaseColumn correctColumn(
            IVariables variables, DatabaseMeta databaseMeta, DatabaseColumn column) {
          if (Utils.isEmpty(column.getTableName()) || Utils.isEmpty(column.getNativeTypeName())) {
            return null;
          }
          int declared = jdbcType(column.getNativeTypeName());
          return declared == column.getSqlType() ? null : column.withSqlType(declared);
        }
      };

  /**
   * The JDBC type of a column of this declared type.
   *
   * <p>For every name the driver knows, this is its answer when there is no row, which is what a
   * prepared statement's metadata says: {@code org.sqlite.jdbc3.JDBC3ResultSet.getColumnType} with
   * a null value. It matches the declared type up to its first parenthesis, upper-cased, and so
   * does this, except that it also ignores the space SQLite allows before the parenthesis: {@code
   * NUMERIC (20)} is a NUMERIC.
   *
   * <p>A name the driver does not know it calls NUMERIC. SQLite gives such a column numeric
   * affinity, which stores anything that does not look like a number as text, so a column declared
   * JSON or UUID holds text, and reading it as a number would lose every value. Those are read as
   * strings, which can hold whatever the column does. A column meant to hold numbers says so with
   * one of the numeric names.
   */
  static int jdbcType(String declaredTypeName) {
    String typeName = declaredTypeName.toUpperCase(Locale.ENGLISH);
    int parenthesis = typeName.indexOf('(');
    if (parenthesis >= 0) {
      typeName = typeName.substring(0, parenthesis);
    }
    return switch (typeName.strip()) {
      case "NUMERIC", "NUMBER" -> Types.NUMERIC;
      case "BOOLEAN" -> Types.BOOLEAN;
      case "TINYINT" -> Types.TINYINT;
      case "SMALLINT", "INT2" -> Types.SMALLINT;
      case "BIGINT", "INT8", "UNSIGNED BIG INT" -> Types.BIGINT;
      case "DATE", "DATETIME" -> Types.DATE;
      case "TIMESTAMP" -> Types.TIMESTAMP;
      case "INT", "INTEGER", "MEDIUMINT" -> Types.INTEGER;
      case "DECIMAL" -> Types.DECIMAL;
      case "DOUBLE", "DOUBLE PRECISION" -> Types.DOUBLE;
      case "REAL" -> Types.REAL;
      case "FLOAT" -> Types.FLOAT;
      case "CHARACTER", "NCHAR", "NATIVE CHARACTER", "CHAR" -> Types.CHAR;
      case "CLOB" -> Types.CLOB;
      case "VARCHAR", "VARYING CHARACTER", "NVARCHAR", "TEXT" -> Types.VARCHAR;
      case "BINARY" -> Types.BINARY;
      case "BLOB" -> Types.BLOB;
      default -> Types.VARCHAR;
    };
  }
}

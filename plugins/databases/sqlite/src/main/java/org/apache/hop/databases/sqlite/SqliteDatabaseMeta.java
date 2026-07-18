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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Locale;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;

/** Contains SQLite specific information through static final members */
@DatabaseMetaPlugin(
    type = "SQLITE",
    typeDescription = "SQLite",
    image = "sqlite.svg",
    documentationUrl = "/database/databases/sqlite.html")
@GuiPlugin(id = "GUI-SQLiteDatabaseMeta")
public class SqliteDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  /**
   * @see IDatabase#getNotFoundTK(boolean)
   */
  @Override
  public int getNotFoundTK(boolean useAutoinc) {
    if (isSupportsAutoInc() && useAutoinc) {
      return 1;
    }
    return super.getNotFoundTK(useAutoinc);
  }

  @Override
  public String getDriverClass() {
    return "org.sqlite.JDBC";
  }

  @Override
  public DriverDownload getDriverDownload() {
    return DriverDownload.builder()
        .mavenCoordinate("org.xerial:sqlite-jdbc")
        .defaultVersion("3.53.2.0")
        .licenseCategory("A")
        .licenseName("Apache-2.0")
        .licenseUrl("https://github.com/xerial/sqlite-jdbc/blob/master/LICENSE")
        .vendor("Xerial SQLite JDBC")
        .vendorUrl("https://github.com/xerial/sqlite-jdbc")
        .build();
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) {
    return "jdbc:sqlite:" + databaseName;
  }

  /**
   * Checks whether or not the command setFetchSize() is supported by the JDBC driver...
   *
   * @return true is setFetchSize() is supported!
   */
  @Override
  public boolean isFetchSizeSupported() {
    return false;
  }

  /**
   * @return true if the database supports bitmap indexes
   */
  @Override
  public boolean isSupportsBitmapIndex() {
    return false;
  }

  /**
   * @param tableName The table to be truncated.
   * @return The SQL statement to truncate a table: remove all rows from it without a transaction
   */
  @Override
  public String getTruncateTableStatement(String tableName) {
    return "DELETE FROM " + tableName;
  }

  /**
   * Generates the SQL statement to add a column to the specified table For this generic type, i set
   * it to the most common possibility.
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " ADD "
        + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " MODIFY "
        + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v, String tk, String pk, boolean useAutoinc, boolean addFieldName, boolean addCr) {
    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_DATE:
        retval += "DATETIME";
        break; // There is no Date or Timestamp data type in SQLite!!!
      case IValueMeta.TYPE_BOOLEAN:
        retval += "CHAR(1)";
        break;
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER:
        if (fieldname.equalsIgnoreCase(tk)
            || // Technical key
            fieldname.equalsIgnoreCase(pk) // Primary key
        ) {
          retval += "INTEGER PRIMARY KEY AUTOINCREMENT";
        } else {
          if (precision != 0 || length < 0 || length > 18) {
            retval += "NUMERIC";
          } else {
            retval += "INTEGER";
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if (length >= DatabaseMeta.CLOB_LENGTH) {
          retval += "BLOB";
        } else {
          retval += "TEXT";
        }
        break;
      case IValueMeta.TYPE_BINARY:
        retval += "BLOB";
        break;
      default:
        retval += "UNKNOWN";
        break;
    }

    if (addCr) {
      retval += Const.CR;
    }

    return retval;
  }

  /**
   * @return true if the database supports error handling (the default). Returns false for certain
   *     databases (SQLite) that invalidate a prepared statement or even the complete connection
   *     when an error occurs.
   */
  @Override
  public boolean isSupportsErrorHandling() {
    return false;
  }

  @Override
  public boolean isSqliteVariant() {
    return true;
  }

  /**
   * SQLite uses dynamic typing with name-based type affinity (see
   * https://www.sqlite.org/datatype3.html section 3.1). The JDBC driver reports columns whose
   * declared type carries a size preceded by a space (e.g. {@code "TEXT (50)"}) as {@link
   * java.sql.Types#NUMERIC}, which makes the generic mapper turn them into BigNumber/Integer. Since
   * the declared size is meaningless in SQLite, re-derive the Hop type from the affinity of the
   * declared type name (which the driver still reports correctly), correcting only the columns the
   * generic mapper got wrong. See issue #6472.
   */
  @Override
  public IValueMeta customizeValueFromSqlType(IValueMeta v, ResultSetMetaData rm, int index)
      throws SQLException {
    if (v == null || rm == null) {
      return super.customizeValueFromSqlType(v, rm, index);
    }
    String typeName = rm.getColumnTypeName(index);
    if (typeName == null) {
      return super.customizeValueFromSqlType(v, rm, index);
    }
    typeName = typeName.toUpperCase(Locale.ROOT);

    int targetType;
    if (typeName.contains("INT")) {
      // INTEGER affinity: SQLite integers are 64-bit, which fit a Hop Integer (Long).
      targetType = IValueMeta.TYPE_INTEGER;
    } else if (typeName.contains("CHAR")
        || typeName.contains("CLOB")
        || typeName.contains("TEXT")) {
      // TEXT affinity.
      targetType = IValueMeta.TYPE_STRING;
    } else if (typeName.contains("REAL")
        || typeName.contains("FLOA")
        || typeName.contains("DOUB")) {
      // REAL affinity.
      targetType = IValueMeta.TYPE_NUMBER;
    } else {
      // NUMERIC and BLOB affinity: keep whatever the generic mapper produced.
      return super.customizeValueFromSqlType(v, rm, index);
    }

    if (v.getType() == targetType) {
      return super.customizeValueFromSqlType(v, rm, index);
    }

    try {
      IValueMeta corrected = ValueMetaFactory.cloneValueMeta(v, targetType);
      corrected.setPrecision(-1);
      return corrected;
    } catch (HopPluginException e) {
      throw new SQLException(e);
    }
  }
}

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
 *
 */

package org.apache.hop.databases.clickhouse;

import org.apache.commons.lang.Validate;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

/**
 * Contains Clickhouse specific information through static final members
 *
 * <p>https://clickhouse.tech/docs/en/sql-reference/
 */
@DatabaseMetaPlugin(
    type = "CLICKHOUSE",
    typeDescription = "ClickHouse",
    image = "clikhouse.svg",
    documentationUrl = "/database/databases/clickhouse.html")
@GuiPlugin(id = "GUI-ClickhouseDatabaseMeta")
public class ClickhouseDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  public static final String CONST_ALTER_TABLE = "ALTER TABLE ";

  // TODO: Manage all attributes in plugin when HOP-67 is fixed
  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 8123;
    }
    return -1;
  }

  @Override
  public String getDriverClass() {
    return "com.clickhouse.jdbc.ClickHouseDriver";
  }

  @Override
  public boolean isSupportsCustomDeleteStmt() {
    return true;
  }

  @Override
  public boolean isSupportsCustomUpdateStmt() {
    return true;
  }

  /**
   * Get the DELETE statement for the current database given the table name
   *
   * @param tableName
   * @return
   */
  @Override
  public String getSqlDeleteStmt(String tableName) {
    return CONST_ALTER_TABLE + tableName + " DELETE ";
  }

  /**
   * Get the UPDATE statement for the current database given the table name
   *
   * @param tableName
   * @return
   */
  @Override
  public String getSqlUpdateStmt(String tableName) {
    return CONST_ALTER_TABLE + tableName + " UPDATE ";
  }

  @Override
  public String getURL(String hostName, String port, String databaseName) {

    Validate.notEmpty(hostName, "Host name is empty");

    String url = "jdbc:clickhouse://" + hostName.toLowerCase();

    if (!Utils.isEmpty(port)) {
      url = url + ":" + port;
    }

    if (!Utils.isEmpty(databaseName)) {
      url = url + "/" + databaseName;
    }

    return url;
  }

  @Override
  public String getAddColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return CONST_ALTER_TABLE
        + tableName
        + " ADD COLUMN "
        + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
  }

  @Override
  public String getDropColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return CONST_ALTER_TABLE + tableName + " DROP COLUMN " + v.getName() + Const.CR;
  }

  @Override
  public String getModifyColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return CONST_ALTER_TABLE
        + tableName
        + " MODIFY COLUMN "
        + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v,
      String surrogateKey,
      String primaryKey,
      boolean useAutoinc,
      boolean addFieldName,
      boolean addCr) {
    String retval = "";

    String newline = addCr ? Const.CR : "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();
    int type = v.getType();

    boolean isKeyField =
        fieldname.equalsIgnoreCase(surrogateKey) || fieldname.equalsIgnoreCase(primaryKey);

    if (addFieldName) {
      retval += fieldname + " ";
    }
    if (isKeyField) {
      Validate.isTrue(
          type == IValueMeta.TYPE_NUMBER
              || type == IValueMeta.TYPE_INTEGER
              || type == IValueMeta.TYPE_BIGNUMBER);
      return ddlForPrimaryKey() + newline;
    }
    switch (type) {
      case IValueMeta.TYPE_TIMESTAMP:
        // timestamp w/ local timezone
        retval += "DATETIME";
        break;
      case IValueMeta.TYPE_DATE:
        retval += "DATE";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        retval += "UINT8";
        break;
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER:
        if (type == IValueMeta.TYPE_INTEGER) {
          // Integer values...
          if (length > 18) {
            retval += "INT128";
          } else if (length > 9) {
            retval += "INT64";
          } else {
            retval += "INT32";
          }
        } else if (type == IValueMeta.TYPE_BIGNUMBER) {
          // Fixed point value...
          if (length < 1) {
            // user configured no value for length. Use 16 digits, which is comparable to
            // mantissa 2^53 of IEEE 754 binary64 "double".
            length = 16;
          }
          if (precision < 1) {
            // user configured no value for precision. Use 16 digits, which is comparable
            // to IEEE 754 binary64 "double".
            precision = 16;
          }
          retval += "DECIMAL(" + length + "," + precision + ")";
        } else {
          // Floating point value with double precision...
          retval += "FLOAT64";
        }
        break;
      case IValueMeta.TYPE_STRING:
        retval += "STRING";
        break;
      case IValueMeta.TYPE_BINARY:
        retval += "UNSUPPORTED";
        break;
      default:
        retval += "UNKNOWN";
        break;
    }
    return retval + newline;
  }

  private String ddlForPrimaryKey() {
    return "UUID NOT NULL PRIMARY KEY";
  }

  @Override
  public String getLimitClause(int nrRows) {
    return " LIMIT " + nrRows;
  }

  /**
   * Returns the minimal SQL to launch in order to determine the layout of the resultset for a given
   * database table
   *
   * @param tableName The name of the table to determine the layout for
   * @return The SQL to launch.
   */
  @Override
  public String getSqlQueryFields(String tableName) {
    return "SELECT * FROM " + tableName + " LIMIT 0";
  }

  @Override
  public String getSqlTableExists(String tableName) {
    return getSqlQueryFields(tableName);
  }

  @Override
  public String getSqlColumnExists(String columnname, String tableName) {
    return getSqlQueryColumnFields(columnname, tableName);
  }

  public String getSqlQueryColumnFields(String columnname, String tableName) {
    return "SELECT " + columnname + " FROM " + tableName + " LIMIT 0";
  }

  /**
   * @return The extra option separator in database URL for this platform (usually this is semicolon
   *     ; )
   */
  @Override
  public String getExtraOptionSeparator() {
    return "&";
  }

  /**
   * @return true if all fields should always be quoted in db
   */
  @Override
  public boolean isQuoteAllFields() {
    return false;
  }

  /**
   * @return This indicator separates the normal URL from the options
   */
  @Override
  public String getExtraOptionIndicator() {
    return "?";
  }

  /**
   * @return true if the database supports schemas
   */
  @Override
  public boolean isSupportsSchemas() {
    return false;
  }

  /**
   * @return true if the database supports transactions.
   */
  @Override
  public boolean isSupportsTransactions() {
    return false;
  }

  /**
   * @return true if the database supports views
   */
  @Override
  public boolean isSupportsViews() {
    return true;
  }

  @Override
  public boolean isSupportsSequences() {
    return false;
  }

  @Override
  public boolean isSupportsSynonyms() {
    return true;
  }

  @Override
  public boolean isSupportsBooleanDataType() {
    return false;
  }

  @Override
  public boolean IsSupportsErrorHandlingOnBatchUpdates() {
    return true;
  }

  @Override
  public String[] getReservedWords() {
    return new String[] {
      "ALL",
      "ALTER",
      "AND",
      "ANY",
      "AS",
      "ASC",
      "BETWEEN",
      "BY",
      "CASE",
      "CAST",
      "CHECK",
      "CLUSTER",
      "COLUMN",
      "CONNECT",
      "CREATE",
      "CROSS",
      "CURRENT",
      "DELETE",
      "DESC",
      "DISTINCT",
      "DROP",
      "ELSE",
      "EXCLUSIVE",
      "EXISTS",
      "FALSE",
      "FOR",
      "FROM",
      "FULL",
      "GRANT",
      "GROUP",
      "HAVING",
      "IDENTIFIED",
      "IMMEDIATE",
      "IN",
      "INCREMENT",
      "INNER",
      "INSERT",
      "INTERSECT",
      "INTO",
      "IS",
      "JOIN",
      "LATERAL",
      "LEFT",
      "LIKE",
      "LOCK",
      "LONG",
      "MAXEXTENTS",
      "MINUS",
      "MODIFY",
      "NATURAL",
      "NOT",
      "NULL",
      "OF",
      "ON",
      "OPTION",
      "OR",
      "ORDER",
      "REGEXP",
      "RENAME",
      "REVOKE",
      "RIGHT",
      "RLIKE",
      "ROW",
      "ROWS",
      "SELECT",
      "SET",
      "SOME",
      "START",
      "TABLE",
      "THEN",
      "TO",
      "TRIGGER",
      "TRUE",
      "UNION",
      "UNIQUE",
      "UPDATE",
      "USING",
      "VALUES",
      "WHEN",
      "WHENEVER",
      "WHERE",
      "WITH"
    };
  }

  @Override
  public String getExtraOptionsHelpText() {
    return "https://github.com/ClickHouse/clickhouse-jdbc";
  }

  @Override
  public String getSqlInsertAutoIncUnknownDimensionRow(
      String schemaTable, String keyField, String versionField) {
    return "insert into " + schemaTable + "(" + keyField + ", " + versionField + ") values (1, 1)";
  }

  @Override
  public String quoteSqlString(String string) {
    string = string.replace("'", "\\\\'");
    string = string.replace("\\n", "\\\\n");
    string = string.replace("\\r", "\\\\r");
    return "'" + string + "'";
  }

  @Override
  public boolean isReleaseSavepoint() {
    return false;
  }

  @Override
  public boolean isRequiringTransactionsOnQueries() {
    return false;
  }

  @Override
  public boolean isRequiresName() {
    return false;
  }

  /**
   * @return true if we need to supply the schema-name to getTables in order to get a correct list
   *     of items.
   */
  @Override
  public boolean useSchemaNameForTableList() {
    return false;
  }

  /**
   * @return true if the database resultsets support getTimeStamp() to retrieve date-time. (Date)
   */
  @Override
  public boolean isSupportsTimeStampToDateConversion() {
    return false;
  }
}

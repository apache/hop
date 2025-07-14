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

package org.apache.hop.databases.firebird;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

/** Contains Firebird specific information through static final members */
@DatabaseMetaPlugin(
    type = "FIREBIRD",
    typeDescription = "Firebird SQL",
    image = "firebird.svg",
    documentationUrl = "/database/databases/firebird.html")
@GuiPlugin(id = "GUI-FirebirdDatabaseMeta")
public class FirebirdDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  public static final String CONST_TIMESTAMP = "TIMESTAMP";
  public static final String CONST_SMALLINT = "SMALLINT";
  public static final String CONST_INTEGER = "INTEGER";
  public static final String CONST_DOUBLE = "DOUBLE";
  public static final String CONST_VARCHAR = "VARCHAR";
  public static final String CONST_UNKNOWN = "UNKNOWN";

  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 3050;
    }
    return -1;
  }

  /**
   * @return Whether or not the database can use auto increment type of fields (pk)
   */
  @Override
  public boolean isSupportsAutoInc() {
    return false;
  }

  @Override
  public String getDriverClass() {
    return "org.firebirdsql.jdbc.FBDriver";
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) {
    return "jdbc:firebirdsql://" + hostname + ":" + port + "/" + databaseName;
  }

  /**
   * @return true if the database supports bitmap indexes
   */
  @Override
  public boolean isSupportsBitmapIndex() {
    return false;
  }

  /**
   * @return true if the database supports synonyms
   */
  @Override
  public boolean isSupportsSynonyms() {
    return false;
  }

  /**
   * @param tableName The table to be truncated.
   * @return The SQL statement to truncate a table: remove all rows from it without a transaction No
   *     such luck for firebird, I'm afraid.
   */
  @Override
  public String getTruncateTableStatement(String tableName) {
    return "DELETE FROM " + tableName;
  }

  /**
   * Generates the SQL statement to add a column to the specified table
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
        + " ALTER COLUMN "
        + v.getName()
        + " TYPE "
        + getFieldDefinition(v, tk, pk, useAutoinc, false, false);
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v, String tk, String pk, boolean useAutoinc, boolean addFieldName, boolean addCr) {
    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      if (Const.indexOfString(fieldname, getReservedWords()) >= 0) {
        retval += getStartQuote() + fieldname + getEndQuote();
      } else {
        retval += fieldname + " ";
      }
    }

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_DATE:
        retval += CONST_TIMESTAMP;
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if (isSupportsBooleanDataType()) {
          retval += "BIT";
        } else {
          retval += "CHAR(1)";
        }
        break;
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER:
        if (fieldname.equalsIgnoreCase(tk)
            || // Technical key
            fieldname.equalsIgnoreCase(pk) // Primary key
        ) {
          retval += "BIGINT NOT NULL PRIMARY KEY";
        } else {
          if (type == IValueMeta.TYPE_INTEGER) {
            // Integer values...
            if (length < 5) {
              retval += CONST_SMALLINT;
            } else if (length < 10) {
              retval += CONST_INTEGER;
            } else {
              retval += "BIGINT";
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
            retval += "DOUBLE";
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if (length < 32720) {
          retval += CONST_VARCHAR;
          if (length > 0) {
            retval += "(" + length + ")";
          } else {
            retval += "(8000)"; // Maybe use some default DB String length?
          }
        } else {
          retval += "BLOB SUB_TYPE TEXT";
        }
        break;
      case IValueMeta.TYPE_BINARY:
        retval += "BLOB";
        break;
      default:
        retval += CONST_UNKNOWN;
        break;
    }

    if (addCr) {
      retval += Const.CR;
    }

    return retval;
  }

  @Override
  public String[] getReservedWords() {
    return new String[] {
      "ABSOLUTE",
      "ACTION",
      "ACTIVE",
      "ADD",
      "ADMIN",
      "AFTER",
      "ALL",
      "ALLOCATE",
      "ALTER",
      "AND",
      "ANY",
      "ARE",
      "AS",
      "ASC",
      "ASCENDING",
      "ASSERTION",
      "AT",
      "AUTHORIZATION",
      "AUTO",
      "AUTODDL",
      "AVG",
      "BASED",
      "BASENAME",
      "BASE_NAME",
      "BEFORE",
      "BEGIN",
      "BETWEEN",
      "BIT",
      "BIT_LENGTH",
      "BLOB",
      "BLOBEDIT",
      "BOTH",
      "BUFFER",
      "BY",
      "CACHE",
      "CASCADE",
      "CASCADED",
      "CASE",
      "CAST",
      "CATALOG",
      "CHAR",
      "CHARACTER",
      "CHAR_LENGTH",
      "CHARACTER_LENGTH",
      "CHECK",
      "CHECK_POINT_LEN",
      "CHECK_POINT_LENGTH",
      "CLOSE",
      "COALESCE",
      "COLLATE",
      "COLLATION",
      "COLUMN",
      "COMMIT",
      "COMMITTED",
      "COMPILETIME",
      "COMPUTED",
      "CONDITIONAL",
      "CONNECT",
      "CONNECTION",
      "CONSTRAINT",
      "CONSTRAINTS",
      "CONTAINING",
      "CONTINUE",
      "CONVERT",
      "CORRESPONDING",
      "COUNT",
      "CREATE",
      "CROSS",
      "CSTRING",
      "CURRENT",
      "CURRENT_DATE",
      "CURRENT_TIME",
      "CURRENT_TIMESTAMP",
      "CURRENT_USER",
      "DATABASE",
      "DATE",
      "DAY",
      "DB_KEY",
      "DEALLOCATE",
      "DEBUG",
      "DEC",
      "DECIMAL",
      "DECLARE",
      "DEFAULT",
      "DEFERRABLE",
      "DEFERRED",
      "DELETE",
      "DESC",
      "DESCENDING",
      "DESCRIBE",
      "DESCRIPTOR",
      "DIAGNOSTICS",
      "DISCONNECT",
      "DISPLAY",
      "DISTINCT",
      "DO",
      "DOMAIN",
      CONST_DOUBLE,
      "DROP",
      "ECHO",
      "EDIT",
      "ELSE",
      "END",
      "END-EXEC",
      "ENTRY_POINT",
      "ESCAPE",
      "EVENT",
      "EXCEPT",
      "EXCEPTION",
      "EXEC",
      "EXECUTE",
      "EXISTS",
      "EXIT",
      "EXTERN",
      "EXTERNAL",
      "EXTRACT",
      "FALSE",
      "FETCH",
      "FILE",
      "FILTER",
      "FLOAT",
      "FOR",
      "FOREIGN",
      "FOUND",
      "FREE_IT",
      "FROM",
      "FULL",
      "FUNCTION",
      "GDSCODE",
      "GENERATOR",
      "GEN_ID",
      "GET",
      "GLOBAL",
      "GO",
      "GOTO",
      "GRANT",
      "GROUP",
      "GROUP_COMMIT_WAIT",
      "GROUP_COMMIT_WAIT_TIME",
      "HAVING",
      "HELP",
      "HOUR",
      "IDENTITY",
      "IF",
      "IMMEDIATE",
      "IN",
      "INACTIVE",
      "INDEX",
      "INDICATOR",
      "INIT",
      "INITIALLY",
      "INNER",
      "INPUT",
      "INPUT_TYPE",
      "INSENSITIVE",
      "INSERT",
      "INT",
      CONST_INTEGER,
      "INTERSECT",
      "INTERVAL",
      "INTO",
      "IS",
      "ISOLATION",
      "ISQL",
      "JOIN",
      "KEY",
      "LANGUAGE",
      "LAST",
      "LC_MESSAGES",
      "LC_TYPE",
      "LEADING",
      "LEFT",
      "LENGTH",
      "LEV",
      "LEVEL",
      "LIKE",
      "LOCAL",
      "LOGFILE",
      "LOG_BUFFER_SIZE",
      "LOG_BUF_SIZE",
      "LONG",
      "LOWER",
      "MANUAL",
      "MATCH",
      "MAX",
      "MAXIMUM",
      "MAXIMUM_SEGMENT",
      "MAX_SEGMENT",
      "MERGE",
      "MESSAGE",
      "MIN",
      "MINIMUM",
      "MINUTE",
      "MODULE",
      "MODULE_NAME",
      "MONTH",
      "NAMES",
      "NATIONAL",
      "NATURAL",
      "NCHAR",
      "NEXT",
      "NO",
      "NOAUTO",
      "NOT",
      "NULL",
      "NULLIF",
      "NUM_LOG_BUFS",
      "NUM_LOG_BUFFERS",
      "NUMERIC",
      "OCTET_LENGTH",
      "OF",
      "ON",
      "ONLY",
      "OPEN",
      "OPTION",
      "OR",
      "ORDER",
      "OUTER",
      "OUTPUT",
      "OUTPUT_TYPE",
      "OVERFLOW",
      "OVERLAPS",
      "PAD",
      "PAGE",
      "PAGELENGTH",
      "PAGES",
      "PAGE_SIZE",
      "PARAMETER",
      "PARTIAL",
      "PASSWORD",
      "PLAN",
      "POSITION",
      "POST_EVENT",
      "PRECISION",
      "PREPARE",
      "PRESERVE",
      "PRIMARY",
      "PRIOR",
      "PRIVILEGES",
      "PROCEDURE",
      "PUBLIC",
      "QUIT",
      "RAW_PARTITIONS",
      "RDB$DB_KEY",
      "READ",
      "REAL",
      "RECORD_VERSION",
      "REFERENCES",
      "RELATIVE",
      "RELEASE",
      "RESERV",
      "RESERVING",
      "RESTRICT",
      "RETAIN",
      "RETURN",
      "RETURNING_VALUES",
      "RETURNS",
      "REVOKE",
      "RIGHT",
      "ROLE",
      "ROLLBACK",
      "ROWS",
      "RUNTIME",
      "SCHEMA",
      "SCROLL",
      "SECOND",
      "SECTION",
      "SELECT",
      "SESSION",
      "SESSION_USER",
      "SET",
      "SHADOW",
      "SHARED",
      "SHELL",
      "SHOW",
      "SINGULAR",
      "SIZE",
      CONST_SMALLINT,
      "SNAPSHOT",
      "SOME",
      "SORT",
      "SPACE",
      "SQL",
      "SQLCODE",
      "SQLERROR",
      "SQLSTATE",
      "SQLWARNING",
      "STABILITY",
      "STARTING",
      "STARTS",
      "STATEMENT",
      "STATIC",
      "STATISTICS",
      "SUB_TYPE",
      "SUBSTRING",
      "SUM",
      "SUSPEND",
      "SYSTEM_USER",
      "TABLE",
      "TEMPORARY",
      "TERMINATOR",
      "THEN",
      "TIME",
      CONST_TIMESTAMP,
      "TIMEZONE_HOUR",
      "TIMEZONE_MINUTE",
      "TO",
      "TRAILING",
      "TRANSACTION",
      "TRANSLATE",
      "TRANSLATION",
      "TRIGGER",
      "TRIM",
      "TRUE",
      "TYPE",
      "UNCOMMITTED",
      "UNION",
      "UNIQUE",
      CONST_UNKNOWN,
      "UPDATE",
      "UPPER",
      "USAGE",
      "USER",
      "USING",
      "VALUE",
      "VALUES",
      CONST_VARCHAR,
      "VARIABLE",
      "VARYING",
      "VERSION",
      "VIEW",
      "WAIT",
      "WEEKDAY",
      "WHEN",
      "WHENEVER",
      "WHERE",
      "WHILE",
      "WITH",
      "WORK",
      "WRITE",
      "YEAR",
      "YEARDAY",
      "ZONE",
      "ABSOLUTE",
      "ACTION",
      "ACTIVE",
      "ADD",
      "ADMIN",
      "AFTER",
      "ALL",
      "ALLOCATE",
      "ALTER",
      "AND",
      "ANY",
      "ARE",
      "AS",
      "ASC",
      "ASCENDING",
      "ASSERTION",
      "AT",
      "AUTHORIZATION",
      "AUTO",
      "AUTODDL",
      "AVG",
      "BASED",
      "BASENAME",
      "BASE_NAME",
      "BEFORE",
      "BEGIN",
      "BETWEEN",
      "BIT",
      "BIT_LENGTH",
      "BLOB",
      "BLOBEDIT",
      "BOTH",
      "BUFFER",
      "BY",
      "CACHE",
      "CASCADE",
      "CASCADED",
      "CASE",
      "CAST",
      "CATALOG",
      "CHAR",
      "CHARACTER",
      "CHAR_LENGTH",
      "CHARACTER_LENGTH",
      "CHECK",
      "CHECK_POINT_LEN",
      "CHECK_POINT_LENGTH",
      "CLOSE",
      "COALESCE",
      "COLLATE",
      "COLLATION",
      "COLUMN",
      "COMMIT",
      "COMMITTED",
      "COMPILETIME",
      "COMPUTED",
      "CONDITIONAL",
      "CONNECT",
      "CONNECTION",
      "CONSTRAINT",
      "CONSTRAINTS",
      "CONTAINING",
      "CONTINUE",
      "CONVERT",
      "CORRESPONDING",
      "COUNT",
      "CREATE",
      "CROSS",
      "CSTRING",
      "CURRENT",
      "CURRENT_DATE",
      "CURRENT_TIME",
      "CURRENT_TIMESTAMP",
      "CURRENT_USER",
      "DATABASE",
      "DATE",
      "DAY",
      "DB_KEY",
      "DEALLOCATE",
      "DEBUG",
      "DEC",
      "DECIMAL",
      "DECLARE",
      "DEFAULT",
      "DEFERRABLE",
      "DEFERRED",
      "DELETE",
      "DESC",
      "DESCENDING",
      "DESCRIBE",
      "DESCRIPTOR",
      "DIAGNOSTICS",
      "DISCONNECT",
      "DISPLAY",
      "DISTINCT",
      "DO",
      "DOMAIN",
      CONST_DOUBLE,
      "DROP",
      "ECHO",
      "EDIT",
      "ELSE",
      "END",
      "END-EXEC",
      "ENTRY_POINT",
      "ESCAPE",
      "EVENT",
      "EXCEPT",
      "EXCEPTION",
      "EXEC",
      "EXECUTE",
      "EXISTS",
      "EXIT",
      "EXTERN",
      "EXTERNAL",
      "EXTRACT",
      "FALSE",
      "FETCH",
      "FILE",
      "FILTER",
      "FLOAT",
      "FOR",
      "FOREIGN",
      "FOUND",
      "FREE_IT",
      "FROM",
      "FULL",
      "FUNCTION",
      "GDSCODE",
      "GENERATOR",
      "GEN_ID",
      "GET",
      "GLOBAL",
      "GO",
      "GOTO",
      "GRANT",
      "GROUP",
      "GROUP_COMMIT_WAIT",
      "GROUP_COMMIT_WAIT_TIME",
      "HAVING",
      "HELP",
      "HOUR",
      "IDENTITY",
      "IF",
      "IMMEDIATE",
      "IN",
      "INACTIVE",
      "INDEX",
      "INDICATOR",
      "INIT",
      "INITIALLY",
      "INNER",
      "INPUT",
      "INPUT_TYPE",
      "INSENSITIVE",
      "INSERT",
      "INT",
      CONST_INTEGER,
      "INTERSECT",
      "INTERVAL",
      "INTO",
      "IS",
      "ISOLATION",
      "ISQL",
      "JOIN",
      "KEY",
      "LANGUAGE",
      "LAST",
      "LC_MESSAGES",
      "LC_TYPE",
      "LEADING",
      "LEFT",
      "LENGTH",
      "LEV",
      "LEVEL",
      "LIKE",
      "LOCAL",
      "LOGFILE",
      "LOG_BUFFER_SIZE",
      "LOG_BUF_SIZE",
      "LONG",
      "LOWER",
      "MANUAL",
      "MATCH",
      "MAX",
      "MAXIMUM",
      "MAXIMUM_SEGMENT",
      "MAX_SEGMENT",
      "MERGE",
      "MESSAGE",
      "MIN",
      "MINIMUM",
      "MINUTE",
      "MODULE",
      "MODULE_NAME",
      "MONTH",
      "NAMES",
      "NATIONAL",
      "NATURAL",
      "NCHAR",
      "NEXT",
      "NO",
      "NOAUTO",
      "NOT",
      "NULL",
      "NULLIF",
      "NUM_LOG_BUFS",
      "NUM_LOG_BUFFERS",
      "NUMERIC",
      "OCTET_LENGTH",
      "OF",
      "ON",
      "ONLY",
      "OPEN",
      "OPTION",
      "OR",
      "ORDER",
      "OUTER",
      "OUTPUT",
      "OUTPUT_TYPE",
      "OVERFLOW",
      "OVERLAPS",
      "PAD",
      "PAGE",
      "PAGELENGTH",
      "PAGES",
      "PAGE_SIZE",
      "PARAMETER",
      "PARTIAL",
      "PASSWORD",
      "PLAN",
      "POSITION",
      "POST_EVENT",
      "PRECISION",
      "PREPARE",
      "PRESERVE",
      "PRIMARY",
      "PRIOR",
      "PRIVILEGES",
      "PROCEDURE",
      "PUBLIC",
      "QUIT",
      "RAW_PARTITIONS",
      "RDB$DB_KEY",
      "READ",
      "REAL",
      "RECORD_VERSION",
      "REFERENCES",
      "RELATIVE",
      "RELEASE",
      "RESERV",
      "RESERVING",
      "RESTRICT",
      "RETAIN",
      "RETURN",
      "RETURNING_VALUES",
      "RETURNS",
      "REVOKE",
      "RIGHT",
      "ROLE",
      "ROLLBACK",
      "ROWS",
      "RUNTIME",
      "SCHEMA",
      "SCROLL",
      "SECOND",
      "SECTION",
      "SELECT",
      "SESSION",
      "SESSION_USER",
      "SET",
      "SHADOW",
      "SHARED",
      "SHELL",
      "SHOW",
      "SINGULAR",
      "SIZE",
      CONST_SMALLINT,
      "SNAPSHOT",
      "SOME",
      "SORT",
      "SPACE",
      "SQL",
      "SQLCODE",
      "SQLERROR",
      "SQLSTATE",
      "SQLWARNING",
      "STABILITY",
      "STARTING",
      "STARTS",
      "STATEMENT",
      "STATIC",
      "STATISTICS",
      "SUB_TYPE",
      "SUBSTRING",
      "SUM",
      "SUSPEND",
      "SYSTEM_USER",
      "TABLE",
      "TEMPORARY",
      "TERMINATOR",
      "THEN",
      "TIME",
      CONST_TIMESTAMP,
      "TIMEZONE_HOUR",
      "TIMEZONE_MINUTE",
      "TO",
      "TRAILING",
      "TRANSACTION",
      "TRANSLATE",
      "TRANSLATION",
      "TRIGGER",
      "TRIM",
      "TRUE",
      "TYPE",
      "UNCOMMITTED",
      "UNION",
      "UNIQUE",
      CONST_UNKNOWN,
      "UPDATE",
      "UPPER",
      "USAGE",
      "USER",
      "USING",
      "VALUE",
      "VALUES",
      CONST_VARCHAR,
      "VARIABLE",
      "VARYING",
      "VERSION",
      "VIEW",
      "WAIT",
      "WEEKDAY",
      "WHEN",
      "WHENEVER",
      "WHERE",
      "WHILE",
      "WITH",
      "WORK",
      "WRITE",
      "YEAR",
      "YEARDAY",
      "ZONE"
    };
  }

  /**
   * @return The SQL on this database to get a list of stored procedures.
   */
  @Override
  public String getSqlListOfProcedures() {
    return "SELECT RDB$PROCEDURE_NAME "
        + "FROM RDB$PROCEDURES "
        + "WHERE RDB$OWNER_NAME = '"
        + getUsername().toUpperCase()
        + "' ";
  }

  /**
   * @return The extra option separator in database URL for this platform.
   */
  @Override
  public String getExtraOptionSeparator() {
    return "&";
  }

  /**
   * @return This indicator separates the normal URL from the options
   */
  @Override
  public String getExtraOptionIndicator() {
    return "?";
  }
}

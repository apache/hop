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

package org.apache.hop.databases.singlestore;

import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Contains SingleStore (MemSQL) specific information through static final members */
@Setter
@Getter
@DatabaseMetaPlugin(
    type = "SINGLESTORE",
    typeDescription = "SingleStore (MemSQL)",
    documentationUrl = "/database/databases/singlestore.html")
@GuiPlugin(id = "GUI-SingleStoreDatabaseMeta")
public class SingleStoreDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  public static final String CONST_ALTER_TABLE = "ALTER TABLE ";
  private static final int VARCHAR_LIMIT = 21_844;

  @GuiWidgetElement(
      id = "singleStoreConnectionMode",
      order = "10",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SingleStoreDatabaseMeta.label.ConnectionMode",
      toolTip = "i18n::SingleStoreDatabaseMeta.toolTip.ConnectionMode")
  @HopMetadataProperty(key = "connectionMode")
  private String connectionMode;

  /**
   * @return The extra option separator in database URL for this platform
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

  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 3306;
    }
    return -1;
  }

  @Override
  public String getDriverClass() {
    return "com.singlestore.jdbc.Driver";
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) {
    String url = "jdbc:singlestore:";
    if (StringUtils.isNotEmpty(connectionMode)) {
      url = url + connectionMode;
    }
    url += "//" + hostname + ":" + port + "/" + databaseName;

    return url;
  }

  /**
   * Checks whether the command setFetchSize() is supported by the JDBC driver...
   *
   * @return true is setFetchSize() is supported!
   */
  @Override
  public boolean isFetchSizeSupported() {
    return true;
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

  @Override
  public boolean isSupportsSequences() {
    return false;
  }

  @Override
  public boolean isSupportsSequenceNoMaxValueOption() {
    return false;
  }

  /**
   * SingleStore doesn't do cursor emulation but just supports setFetchSize
   *
   * @return false to indicate that Impala doesn't support auto increment primary key columns.
   */
  @Override
  public boolean isStreamingResults() {
    return false;
  }

  @Override
  public boolean isSupportsAutoInc() {
    return true;
  }

  @Override
  public String getLimitClause(int nrRows) {
    return " limit " + nrRows;
  }

  @Override
  public String getSqlQueryFields(String tableName) {
    return "SELECT * FROM " + tableName + getLimitClause(1);
  }

  @Override
  public String getSqlTableExists(String tableName) {
    return getSqlQueryFields(tableName);
  }

  @Override
  public String getSqlColumnExists(String columnName, String tableName) {
    return getSqlQueryColumnFields(columnName, tableName);
  }

  public String getSqlQueryColumnFields(String columnName, String tableName) {
    return "SELECT " + columnName + " FROM " + tableName + getLimitClause(1);
  }

  /**
   * Generates the SQL statement to add a column to the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoIncrement whether this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether to add a semicolon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return CONST_ALTER_TABLE
        + tableName
        + " ADD COLUMN "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, true, false);
  }

  /**
   * Generates the SQL statement to drop a column from the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoIncrement whether this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether to add a semicolon behind the statement.
   * @return the SQL statement to drop a column from the specified table
   */
  @Override
  public String getDropColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return CONST_ALTER_TABLE + tableName + " DROP COLUMN " + v.getName();
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoIncrement whether this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether to add a semicolon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " MODIFY "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, true, false);
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v,
      String tk,
      String pk,
      boolean useAutoIncrement,
      boolean addFieldName,
      boolean addCR) {
    String fieldClause = "";

    String fieldName = v.getName();
    if (v.getLength() == DatabaseMeta.CLOB_LENGTH) {
      v.setLength(getMaxTextFieldLength());
    }
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      fieldClause += fieldName + " ";
    }

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_DATE:
      case IValueMeta.TYPE_TIMESTAMP:
        fieldClause += "DATETIME(6)";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if (isSupportsBooleanDataType()) {
          fieldClause += "BOOLEAN";
        } else {
          fieldClause += "CHAR(1)";
        }
        break;

      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER:
        if (fieldName.equalsIgnoreCase(tk)
            || // Technical key
            fieldName.equalsIgnoreCase(pk) // Primary key
        ) {
          if (useAutoIncrement) {
            fieldClause += "BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY";
          } else {
            fieldClause += "BIGINT NOT NULL PRIMARY KEY";
          }
        } else {
          // Integer values...
          if (precision == 0) {
            if (length > 9) {
              if (length < 19) {
                // can hold signed values between -9223372036854775808 and 9223372036854775807
                // 18 significant digits
                fieldClause += "BIGINT";
              } else {
                fieldClause += "DECIMAL(" + length + ")";
              }
            } else {
              fieldClause += "INT";
            }
          } else {
            // Floating point values...
            if (length > 15) {
              fieldClause += "DECIMAL(" + length;
              if (precision > 0) {
                fieldClause += ", " + precision;
              }
              fieldClause += ")";
            } else {
              // A double-precision floating-point number is accurate to approximately 15 decimal
              // places.
              fieldClause += "DOUBLE";
            }
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if (length > 0) {
          if (length == 1) {
            fieldClause += "CHAR(1)";
          } else if (length < VARCHAR_LIMIT) {
            fieldClause += "VARCHAR(" + length + ")";
          } else if (length < 65536) {
            fieldClause += "TEXT";
          } else if (length < 16777216) {
            fieldClause += "MEDIUMTEXT";
          } else {
            fieldClause += "LONGTEXT";
          }
        } else {
          fieldClause += "TINYTEXT";
        }
        break;
      case IValueMeta.TYPE_BINARY:
        if (length > 0) {
          fieldClause += "BINARY(" + length + ")";
        } else {
          fieldClause += "VARBINARY";
        }
        break;
      default:
        fieldClause += " UNKNOWN";
        break;
    }

    if (addCR) {
      fieldClause += Const.CR;
    }

    return fieldClause;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.database.IDatabase#getReservedWords()
   */
  @Override
  public String[] getReservedWords() {
    return new String[] {
      "ADD",
      "ALL",
      "ALTER",
      "ANALYZE",
      "AND",
      "AS",
      "ASC",
      "ASENSITIVE",
      "BEFORE",
      "BETWEEN",
      "BIGINT",
      "BINARY",
      "BLOB",
      "BOTH",
      "BY",
      "CALL",
      "CASCADE",
      "CASE",
      "CHANGE",
      "CHAR",
      "CHARACTER",
      "CHECK",
      "COLLATE",
      "COLUMN",
      "CONDITION",
      "CONNECTION",
      "CONSTRAINT",
      "CONTINUE",
      "CONVERT",
      "CREATE",
      "CROSS",
      "CURRENT_DATE",
      "CURRENT_TIME",
      "CURRENT_TIMESTAMP",
      "CURRENT_USER",
      "CURSOR",
      "DATABASE",
      "DATABASES",
      "DAY_HOUR",
      "DAY_MICROSECOND",
      "DAY_MINUTE",
      "DAY_SECOND",
      "DEC",
      "DECIMAL",
      "DECLARE",
      "DEFAULT",
      "DELAYED",
      "DELETE",
      "DESC",
      "DESCRIBE",
      "DETERMINISTIC",
      "DISTINCT",
      "DISTINCTROW",
      "DIV",
      "DOUBLE",
      "DROP",
      "DUAL",
      "EACH",
      "ELSE",
      "ELSEIF",
      "ENCLOSED",
      "ESCAPED",
      "EXISTS",
      "EXIT",
      "EXPLAIN",
      "FALSE",
      "FETCH",
      "FLOAT",
      "FOR",
      "FORCE",
      "FOREIGN",
      "FROM",
      "FULLTEXT",
      "GOTO",
      "GRANT",
      "GROUP",
      "HAVING",
      "HIGH_PRIORITY",
      "HOUR_MICROSECOND",
      "HOUR_MINUTE",
      "HOUR_SECOND",
      "IF",
      "IGNORE",
      "IN",
      "INDEX",
      "INFILE",
      "INNER",
      "INOUT",
      "INSENSITIVE",
      "INSERT",
      "INT",
      "INTEGER",
      "INTERVAL",
      "INTO",
      "IS",
      "ITERATE",
      "JOIN",
      "KEY",
      "KEYS",
      "KILL",
      "LEADING",
      "LEAVE",
      "LEFT",
      "LIKE",
      "LIMIT",
      "LINES",
      "LOAD",
      "LOCALTIME",
      "LOCALTIMESTAMP",
      "LOCATE",
      "LOCK",
      "LONG",
      "LONGBLOB",
      "LONGTEXT",
      "LOOP",
      "LOW_PRIORITY",
      "MATCH",
      "MEDIUMBLOB",
      "MEDIUMINT",
      "MEDIUMTEXT",
      "MIDDLEINT",
      "MINUTE_MICROSECOND",
      "MINUTE_SECOND",
      "MOD",
      "MODIFIES",
      "NATURAL",
      "NOT",
      "NO_WRITE_TO_BINLOG",
      "NULL",
      "NUMERIC",
      "ON",
      "OPTIMIZE",
      "OPTION",
      "OPTIONALLY",
      "OR",
      "ORDER",
      "OUT",
      "OUTER",
      "OUTFILE",
      "POSITION",
      "PRECISION",
      "PRIMARY",
      "PROCEDURE",
      "PURGE",
      "READ",
      "READS",
      "REAL",
      "REFERENCES",
      "REGEXP",
      "RENAME",
      "REPEAT",
      "REPLACE",
      "REQUIRE",
      "RESTRICT",
      "RETURN",
      "REVOKE",
      "RIGHT",
      "RLIKE",
      "SCHEMA",
      "SCHEMAS",
      "SECOND_MICROSECOND",
      "SELECT",
      "SENSITIVE",
      "SEPARATOR",
      "SET",
      "SHOW",
      "SMALLINT",
      "SONAME",
      "SPATIAL",
      "SPECIFIC",
      "SQL",
      "SQLEXCEPTION",
      "SQLSTATE",
      "SQLWARNING",
      "SQL_BIG_RESULT",
      "SQL_CALC_FOUND_ROWS",
      "SQL_SMALL_RESULT",
      "SSL",
      "STARTING",
      "STRAIGHT_JOIN",
      "TABLE",
      "TERMINATED",
      "THEN",
      "TINYBLOB",
      "TINYINT",
      "TINYTEXT",
      "TO",
      "TRAILING",
      "TRIGGER",
      "TRUE",
      "UNDO",
      "UNION",
      "UNIQUE",
      "UNLOCK",
      "UNSIGNED",
      "UPDATE",
      "USAGE",
      "USE",
      "USING",
      "UTC_DATE",
      "UTC_TIME",
      "UTC_TIMESTAMP",
      "VALUES",
      "VARBINARY",
      "VARCHAR",
      "VARCHARACTER",
      "VARYING",
      "WHEN",
      "WHERE",
      "WHILE",
      "WITH",
      "WRITE",
      "XOR",
      "YEAR_MONTH",
      "ZEROFILL"
    };
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.database.IDatabase#getStartQuote()
   */
  @Override
  public String getStartQuote() {
    return "`";
  }

  /**
   * Simply add an underscore in the case of MySQL!
   *
   * @see IDatabase#getEndQuote()
   */
  @Override
  public String getEndQuote() {
    return "`";
  }

  /**
   * @param tableNames The names of the tables to lock
   * @return The SQL command to lock database tables for write purposes.
   */
  @Override
  public String getSqlLockTables(String[] tableNames) {
    StringBuilder sql = new StringBuilder("LOCK TABLES ");
    for (int i = 0; i < tableNames.length; i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append(tableNames[i]).append(" WRITE");
    }
    sql.append(";").append(Const.CR);

    return sql.toString();
  }

  /**
   * @param tableName The name of the table to unlock
   * @return The SQL command to unlock a database table.
   */
  @Override
  public String getSqlUnlockTables(String[] tableName) {
    return "UNLOCK TABLES"; // This unlocks all tables
  }

  /**
   * @return extra help text on the supported options on the selected database platform.
   */
  @Override
  public String getExtraOptionsHelpText() {
    return "https://docs.singlestore.com/cloud/developer-resources/"
        + "connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/";
  }

  /**
   * @param tableName The table name for which we want to check if it's a system table.
   * @return true if the specified table is a system table
   */
  @Override
  public boolean isSystemTable(String tableName) {
    return (tableName.startsWith("sys") || tableName.equals("dtproperties"));
  }

  /**
   * Get the SQL to insert a new empty unknown record in a dimension.
   *
   * @param schemaTable the schema-table name to insert into
   * @param keyField The key field
   * @param versionField the version field
   * @return the SQL to insert the unknown record into the SCD.
   */
  @Override
  public String getSqlInsertAutoIncUnknownDimensionRow(
      String schemaTable, String keyField, String versionField) {
    return "insert into " + schemaTable + "(" + keyField + ", " + versionField + ") values (1, 1)";
  }

  /**
   * @param string The String to quote
   * @return A string that is properly quoted for use in a SQL statement (insert, update, delete,
   *     etc)
   */
  @Override
  public String quoteSqlString(String string) {
    string = string.replace("'", "\\\\'");
    string = string.replace("\\n", "\\\\n");
    string = string.replace("\\r", "\\\\r");
    return "'" + string + "'";
  }

  /**
   * @return true if the database is a MySQL variant, like MySQL 5.1, InfiniDB, InfoBright, and so
   *     on.
   */
  @Override
  public boolean isMySqlVariant() {
    return true;
  }

  /**
   * SingleStore is a MySQL variant. Things like LOAD DATA work exactly like MySQL. However, the
   * SingleStore JDBC driver is smarter and doesn't always need the same work-around.
   *
   * @param dbMetaData The database metadata
   * @param rsMetaData The result set metadata as returned by the JDBC driver
   * @param index The column index to get the column name for
   * @return The column name
   * @throws HopDatabaseException In case something goes wrong
   */
  @Override
  public String getLegacyColumnName(
      DatabaseMetaData dbMetaData, ResultSetMetaData rsMetaData, int index)
      throws HopDatabaseException {
    try {
      return rsMetaData.getColumnName(index);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          "Error retrieving column index " + index + " from SingleStore result set", e);
    }
  }

  @Override
  public int getNotFoundTK(boolean useAutoIncrement) {
    if (useAutoIncrement) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public boolean IsSupportsErrorHandlingOnBatchUpdates() {
    return true;
  }

  @Override
  public boolean isRequiringTransactionsOnQueries() {
    return false;
  }

  @Override
  public void addDefaultOptions() {
    addExtraOption(getPluginId(), "defaultFetchSize", "500");
    setSupportsTimestampDataType(true);
    setSupportsBooleanDataType(true);
  }

  @Override
  public int getMaxVARCHARLength() {
    return VARCHAR_LIMIT;
  }

  @Override
  public int getMaxTextFieldLength() {
    return Integer.MAX_VALUE;
  }
}

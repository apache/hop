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

package org.apache.hop.databases.hive;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Contains MySQL specific information through static final members */
@DatabaseMetaPlugin(
    type = "HIVE",
    typeDescription = "Apache Hive",
    image = "hive.svg",
    documentationUrl = "/database/databases/apache-hive.html")
@GuiPlugin(id = "GUI-HiveDatabaseMeta")
public class HiveDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  private static final Class<?> PKG = HiveDatabaseMeta.class;

  @GuiWidgetElement(
      id = "tablePartitions",
      order = "10",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::DatabaseDialog.label.TablePartitions",
      toolTip = "i18n::DatabaseDialog.tooltip.TablePartitions")
  @HopMetadataProperty
  private String tablePartitions;

  /**
   * We can generate the PARTITION clause here. We take the tablePartitions field and look for the
   * table. In there we'll TABLE1(field1) which we'll turn into PARTITION(field1).
   *
   * @param schemaTable The schema-table name combination (Fully qualified table name) to generate
   *     the clause for.
   * @return
   */
  @Override
  public String getSqlInsertClauseBeforeFields(IVariables variables, String schemaTable) {
    if (StringUtils.isEmpty(tablePartitions)) {
      return null;
    }

    String[] tableParts = variables.resolve(tablePartitions).split(";");
    for (String tablePart : tableParts) {
      String prefix = schemaTable + "(";
      if (tablePart.startsWith(prefix)) {
        // This is the part we want: PARTITION(field)
        //
        return "PARTITION" + tablePart.substring(prefix.length() - 1);
      }
    }
    return null;
  }

  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 10000;
    }
    return -1;
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
    return "org.apache.hive.jdbc.HiveDriver";
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) {
    // Split the hostnames and ports up using commas.
    List<String> hostnames = new ArrayList<>();
    List<String> ports = new ArrayList<>();
    for (String hostnamePart : hostname.split(",")) {
      hostnames.add(StringUtils.strip(hostnamePart));
    }
    for (String portPart : port.split(",")) {
      ports.add(StringUtils.strip(portPart));
    }

    StringBuilder jdbc = new StringBuilder().append("jdbc:hive2://");
    for (int i = 0; i < hostnames.size(); i++) {
      if (i > 0) {
        jdbc.append(',');
      }
      String hostnamePart = hostnames.get(i);
      jdbc.append(hostnamePart);
      if (i < ports.size()) {
        String portPart = ports.get(i);
        if (StringUtils.isNotEmpty(portPart)) {
          jdbc.append(":").append(portPart);
        }
      }
    }
    return jdbc.append("/").append(databaseName).toString();
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
   * @return This indicator separates the normal URL from the options
   */
  @Override
  public String getExtraOptionIndicator() {
    return "?";
  }

  /**
   * @return true if the database supports transactions.
   */
  @Override
  public boolean isSupportsTransactions() {
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
   * @return true if the database supports views
   */
  @Override
  public boolean isSupportsViews() {
    return true;
  }

  /**
   * @return true if the database supports synonyms
   */
  @Override
  public boolean isSupportsSynonyms() {
    return false;
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
        + " MODIFY "
        + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v, String tk, String pk, boolean useAutoinc, boolean addFieldName, boolean addCR) {
    String retval = "";

    String fieldname = v.getName();
    if (v.getLength() == DatabaseMeta.CLOB_LENGTH) {
      v.setLength(getMaxTextFieldLength());
    }
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_DATE:
        retval += "TIMESTAMP";
        break;

      case IValueMeta.TYPE_BOOLEAN:
        if (isSupportsBooleanDataType()) {
          retval += "BOOLEAN";
        } else {
          retval += "CHAR(1)";
        }
        break;

      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_INTEGER:
        if (fieldname.equalsIgnoreCase(tk)
            || // Technical key
            fieldname.equalsIgnoreCase(pk) // Primary key
        ) {
          if (useAutoinc) {
            retval += "BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY";
          } else {
            retval += "BIGINT NOT NULL PRIMARY KEY";
          }
        } else {
          if (type == IValueMeta.TYPE_INTEGER) {
            // Integer values...
            if (length < 3) {
              retval += "TINYINT";
            } else if (length < 5) {
              retval += "SMALLINT";
            } else if (length < 10) {
              retval += "INT";
            } else if (length < 20) {
              retval += "BIGINT";
            } else {
              retval += "DECIMAL(" + length + ")";
            }
          } else if (type == IValueMeta.TYPE_BIGNUMBER) {
            // Fixed point value...
            if (length
                < 1) { // user configured no value for length. Use 16 digits, which is comparable to
              // mantissa 2^53 of IEEE 754 binary64 "double".
              length = 16;
            }
            if (precision
                < 1) { // user configured no value for precision. Use 16 digits, which is comparable
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
        retval += "STRING";
        break;
      case IValueMeta.TYPE_BINARY:
        retval += "BINARY";
        break;
      default:
        retval += " UNKNOWN";
        break;
    }

    if (addCR) {
      retval += Const.CR;
    }

    return retval;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.database.IDatabase#getReservedWords()
   */
  @Override
  public String[] getReservedWords() {
    return new String[] {
      "ALL",
      "ALTER",
      "AND",
      "ARRAY",
      "AS",
      "AUTHORIZATION",
      "BETWEEN",
      "BIGINT",
      "BINARY",
      "BOOLEAN",
      "BOTH",
      "BY",
      "CACHE",
      "CASE",
      "CAST",
      "CHAR",
      "COLUMN",
      "COMMIT",
      "CONF",
      "CONSTRAINT",
      "CREATE",
      "CROSS",
      "CUBE",
      "CURRENT",
      "CURRENT_DATE",
      "CURRENT_TIMESTAMP",
      "CURSOR",
      "DATABASE",
      "DATE",
      "DAYOFWEEK",
      "DECIMAL",
      "DELETE",
      "DESCRIBE",
      "DISTINCT",
      "DOUBLE",
      "DROP",
      "ELSE",
      "END",
      "EXCHANGE",
      "EXISTS",
      "EXTENDED",
      "EXTERNAL",
      "EXTRACT",
      "FALSE",
      "FETCH",
      "FLOAT",
      "FLOOR",
      "FOLLOWING",
      "FOR",
      "FOREIGN",
      "FROM",
      "FULL",
      "FUNCTION",
      "GRANT",
      "GROUP",
      "GROUPING",
      "HAVING",
      "IF",
      "IMPORT",
      "IN",
      "INNER",
      "INSERT",
      "INT",
      "INTEGER",
      "INTERSECT",
      "INTERVAL",
      "INTO",
      "IS",
      "JOIN",
      "LATERAL",
      "LEFT",
      "LESS",
      "LIKE",
      "LOCAL",
      "MACRO",
      "MAP",
      "MORE",
      "NONE",
      "NOT",
      "NULL",
      "OF",
      "ON",
      "ONLY",
      "OR",
      "ORDER",
      "OUT",
      "OUTER",
      "OVER",
      "PARTIALSCAN",
      "PARTITION",
      "PERCENT",
      "PRECEDING",
      "PRECISION",
      "PRESERVE",
      "PRIMARY",
      "PROCEDURE",
      "RANGE",
      "READS",
      "REDUCE",
      "REFERENCES",
      "REGEXP",
      "REVOKE",
      "RIGHT",
      "RLIKE",
      "ROLLBACK",
      "ROLLUP",
      "ROW",
      "ROWS",
      "SELECT",
      "SET",
      "SMALLINT",
      "START",
      "TABLE",
      "TABLESAMPLE",
      "THEN",
      "TIMESTAMP",
      "TO",
      "TRANSFORM",
      "TRIGGER",
      "TRUE",
      "TRUNCATE",
      "UNBOUNDED",
      "UNION",
      "UNIQUEJOIN",
      "UPDATE",
      "USER",
      "USING",
      "UTC_TMESTAMP",
      "VALUES",
      "VARCHAR",
      "VIEWS",
      "WHEN",
      "WHERE",
      "WINDOW",
      "WITH",
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
    String sql = "LOCK TABLES ";
    for (int i = 0; i < tableNames.length; i++) {
      if (i > 0) {
        sql += ", ";
      }
      sql += tableNames[i] + " WRITE";
    }
    sql += ";" + Const.CR;

    return sql;
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
    return "https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.5/integrating-hive/content/hive_connection_string_url_syntax.html";
  }

  /**
   * @param tableName
   * @return true if the specified table is a system table
   */
  @Override
  public boolean isSystemTable(String tableName) {
    return false;
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
   * @param string
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
    return false;
  }

  /** Returns a false as Oracle does not allow for the releasing of savepoints. */
  @Override
  public boolean isReleaseSavepoint() {
    return false;
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
    setForcingIdentifiersToLowerCase(true);
    setSupportsTimestampDataType(true);
    setSupportsBooleanDataType(true);
  }

  @Override
  public int getMaxVARCHARLength() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxTextFieldLength() {
    return Integer.MAX_VALUE;
  }

  /**
   * Gets tablePartitions
   *
   * @return value of tablePartitions
   */
  public String getTablePartitions() {
    return tablePartitions;
  }

  /**
   * Sets tablePartitions
   *
   * @param tablePartitions value of tablePartitions
   */
  public void setTablePartitions(String tablePartitions) {
    this.tablePartitions = tablePartitions;
  }
}

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

package org.apache.hop.databases.googlebigquery;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;

@DatabaseMetaPlugin(
  type = "GOOGLEBIGQUERY",
  typeDescription = "Google BigQuery"
)
@GuiPlugin( id = "GUI-GoogleBigQueryDatabaseMeta" )
public class GoogleBigQueryDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  private static final Class<?> PKG = GoogleBigQueryDatabaseMeta.class; // For Translator

  @Override public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  @Override public String getDriverClass() {
    return "com.simba.googlebigquery.jdbc42.Driver";
  }

  @Override public String getURL( String hostname, String port, String databaseName ) {
    return "jdbc:bigquery://" + hostname + ":"
      + ( StringUtil.isEmpty( port ) ? "443" : port ) + ";"
      + ( StringUtil.isEmpty( databaseName ) ? "" : "ProjectId=" + databaseName ) + ";";
  }

  @Override public String getExtraOptionsHelpText() {
    return "https://cloud.google.com/bigquery/partners/simba-drivers/";
  }

  @Override public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoinc,
                                              boolean addFieldName, boolean addCr ) {
    String retval = "";

    String fieldname = v.getName();
    int precision = v.getPrecision();

    if ( addFieldName ) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch ( type ) {
      case IValueMeta.TYPE_TIMESTAMP:
        retval += "TIMESTAMP";
        break;

      case IValueMeta.TYPE_DATE:
        retval += "DATE";
        break;

      case IValueMeta.TYPE_BOOLEAN:
        retval += "BOOL";
        break;

      case IValueMeta.TYPE_NUMBER:
      case IValueMeta.TYPE_INTEGER:
      case IValueMeta.TYPE_BIGNUMBER:
        if ( precision == 0 ) {
          retval += "INT64";
        } else {
          retval += "FLOAT64";
        }
        if ( fieldname.equalsIgnoreCase( tk )
          || fieldname.equalsIgnoreCase( pk ) ) {
          retval += " NOT NULL";
        }
        break;

      case IValueMeta.TYPE_STRING:
        retval += "STRING";
        break;

      case IValueMeta.TYPE_BINARY:
        retval += "BYTES";
        break;

      default:
        retval += " UNKNOWN";
        break;
    }

    if ( addCr ) {
      retval += Const.CR;
    }

    return retval;
  }

  @Override public String getAddColumnStatement(
    String tableName, IValueMeta v, String tk,
    boolean useAutoinc, String pk, boolean semicolon ) {
    // BigQuery does not support DDL through JDBC.
    // https://cloud.google.com/bigquery/partners/simba-drivers/#do_the_drivers_provide_the_ability_to_manage_tables_create_table
    return null;
  }

  @Override public String getModifyColumnStatement(
    String tableName, IValueMeta v, String tk, boolean useAutoinc,
    String pk, boolean semicolon ) {
    // BigQuery does not support DDL through JDBC.
    // https://cloud.google.com/bigquery/partners/simba-drivers/#do_the_drivers_provide_the_ability_to_manage_tables_create_table
    return null;
  }

  @Override public String getLimitClause( int nrRows ) {
    return " LIMIT " + nrRows;
  }

  @Override public String getSqlQueryFields( String tableName ) {
    return "SELECT * FROM " + tableName + " LIMIT 0";
  }

  @Override public String getSqlTableExists( String tableName ) {
    return getSqlQueryFields( tableName );
  }

  @Override public String getSqlColumnExists( String columnname, String tableName ) {
    return getSqlQueryColumnFields( columnname, tableName );
  }

  public String getSqlQueryColumnFields( String columnname, String tableName ) {
    return "SELECT " + columnname + " FROM " + tableName + " LIMIT 0";
  }

  @Override public boolean supportsAutoInc() {
    return false;
  }

  @Override public boolean supportsAutoGeneratedKeys() {
    return false;
  }

  @Override public boolean supportsTimeStampToDateConversion() {
    return false;
  }

  @Override public boolean supportsBooleanDataType() {
    return true;
  }

  @Override public boolean isRequiringTransactionsOnQueries() {
    return false;
  }

  @Override public String getExtraOptionSeparator() {
    return ";";
  }

  @Override public String getExtraOptionIndicator() {
    return "";
  }

  @Override public String getStartQuote() {
    return "`";
  }

  @Override public String getEndQuote() {
    return "`";
  }

  @Override public boolean supportsBitmapIndex() {
    return false;
  }

  @Override public boolean supportsViews() {
    return true;
  }

  @Override public boolean supportsSynonyms() {
    return false;
  }

  @Override public String[] getReservedWords() {
    return new String[] { "ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "ASSERT_ROWS_MODIFIED", "AT", "BETWEEN",
      "COLLATE", "CONTAINS", "CREATE", "CROSS", "CUBE", "CURRENT", "DEFAULT", "DEFINE", "DESC", "DISTINCT",
      "ELSE", "END", "ENUM", "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT", "FALSE", "FETCH", "FOLLOWING",
      "FOR", "FROM", "FULL", "GROUP", "GROUPING", "GROUPS", "HASH", "HAVING", "IF", "IGNORE", "IN", "INNER",
      "INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEFT", "LIKE", "LIMIT", "LOOKUP", "MERGE",
      "NATURAL", "NEW", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "OR", "ORDER", "OUTER", "OVER", "PARTITION",
      "PRECEDING", "PROTO", "RANGE", "RECURSIVE", "RESPECT", "RIGHT", "ROLLUP", "ROWS", "SELECT", "SET", "SOME",
      "STRUCT", "TABLESAMPLE", "THEN", "TO", "TREAT", "TRUE", "UNBOUNDED", "UNION", "UNNEST", "USING", "WHEN",
      "WHERE", "WINDOW", "WITH", "WITHIN", "BY", "CASE", "CAST" };
  }

  @Override public boolean supportsStandardTableOutput() {
    return false;
  }

  @Override public String getUnsupportedTableOutputMessage() {
    return BaseMessages.getString( PKG, "GoogleBigQueryDatabaseMeta.UnsupportedTableOutputMessage" );
  }
}

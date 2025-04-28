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

package org.apache.hop.databases.access;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

/** Microsoft Access connection */
@DatabaseMetaPlugin(
    type = "MSACCESS",
    typeDescription = "Microsoft Access database",
    documentationUrl = "/database/databases/access.html",
    classLoaderGroup = "access-db")
@GuiPlugin(id = "GUI-MSAccessDatabaseMeta")
public class AccessDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public String getDriverClass() {
    return "net.ucanaccess.jdbc.UcanaccessDriver";
  }

  @Override
  public String getURL(String hostname, String port, String database) {
    return "jdbc:ucanaccess://" + database + ";showSchema=true";
  }

  @Override
  public String getExtraOptionsHelpText() {
    return "http://ucanaccess.sourceforge.net/site.html#examples";
  }

  @Override
  public void addDefaultOptions() {
    addExtraOption(getPluginId(), "newDatabaseVersion", "V2010");
  }

  @Override
  public int getNotFoundTK(boolean useAutoinc) {
    if (isSupportsAutoInc() && useAutoinc) {
      return 0;
    }
    return super.getNotFoundTK(useAutoinc);
  }

  @Override
  public String getSchemaTableCombination(String schemaName, String tablePart) {
    // Ignore schema
    return tablePart;
  }

  @Override
  public String getTruncateTableStatement(String tableName) {
    return "DELETE FROM " + tableName;
  }

  @Override
  public String getDropColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return "-- Drop not supported\n-- ALTER TABLE " + tableName + " DROP " + v.getName();
  }

  @Override
  public String getAddColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " ADD "
        + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
  }

  @Override
  public String getModifyColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " ALTER COLUMN "
        + v.getName()
        + " SET "
        + getFieldDefinition(v, tk, pk, useAutoinc, false, false);
  }

  @Override
  public String getFieldDefinition(
      IValueMeta valueMeta,
      String tk,
      String pk,
      boolean useAutoinc,
      boolean addFieldName,
      boolean addCr) {
    String retval = "";

    if (addFieldName) {
      retval += valueMeta.getName() + " ";
    }

    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_DATE:
        retval += "DATETIME";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        retval += "YESNO";
        break;
      case IValueMeta.TYPE_INTEGER:
        if (tk.equals(valueMeta.getName())) {
          retval += "AUTOINCREMENT";
        } else retval += "LONG";
        break;
      case IValueMeta.TYPE_NUMBER:
        retval += "DOUBLE";
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        retval += "NUMERIC";
        break;
      case IValueMeta.TYPE_STRING:
        if (valueMeta.getLength() > getMaxVARCHARLength()) {
          retval += "MEMO";
        } else {
          retval += "TEXT";
        }
        break;
      default:
        retval += " UNKNOWN";
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
      "AND",
      "ANY",
      "AS",
      "ALL",
      "AT",
      "AVG",
      "BETWEEN",
      "BOTH",
      "BY",
      "CALL",
      "CASE",
      "CAST",
      "COALESCE",
      "CONSTRAINT",
      "CORRESPONDING",
      "CONVERT",
      "COUNT",
      "CREATE",
      "CROSS",
      "DEFAULT",
      "DISTINCT",
      "DO",
      "DROP",
      "ELSE",
      "EVERY",
      "EXISTS",
      "EXCEPT",
      "FOR",
      "FROM",
      "FULL",
      "GRANT",
      "GROUP",
      "HAVING",
      "IN",
      "INNER",
      "INTERSECT",
      "INTO",
      "IS",
      "JOIN",
      "LEFT",
      "LEADING",
      "LIKE",
      "MAX",
      "MIN",
      "NATURAL",
      "NOT",
      "NULLIF",
      "ON",
      "ORDER",
      "OR",
      "OUTER",
      "PRIMARY",
      "REFERENCES",
      "RIGHT",
      "SELECT",
      "SET",
      "SOME",
      "STDDEV_POP",
      "STDDEV_SAMP",
      "SUM",
      "TABLE",
      "THEN",
      "TO",
      "TRAILING",
      "TRIGGER",
      "UNION",
      "UNIQUE",
      "USING",
      "USER",
      "VALUES",
      "VAR_POP",
      "VAR_SAMP",
      "WHEN",
      "WHERE",
      "WITH",
      "END"
    };
  }

  /**
   * Get the maximum length of a TEXT field for this database connection. If this size is exceeded
   * use a MEMO.
   *
   * @return The maximum TEXT field length for this database type.
   */
  @Override
  public int getMaxVARCHARLength() {
    return 255;
  }

  /**
   * Get the maximum length of a MEMO field for this database connection.
   *
   * @return The maximum MEMO field length for this database type.
   */
  @Override
  public int getMaxTextFieldLength() {
    return 8388607;
  }

  @Override
  public String getSqlListOfSchemas() {
    return "SELECT SCHEMA_NAME AS \"name\" FROM INFORMATION_SCHEMA.SCHEMATA";
  }

  @Override
  public boolean isFetchSizeSupported() {
    return false;
  }

  @Override
  public boolean isSupportsBitmapIndex() {
    return false;
  }

  @Override
  public boolean isSupportsBatchUpdates() {
    return false;
  }

  @Override
  public boolean isSupportsCatalogs() {
    return false;
  }

  @Override
  public boolean isSupportsSchemas() {
    return false;
  }

  @Override
  public boolean isSupportsSynonyms() {
    return false;
  }

  @Override
  public boolean isSupportsSequences() {
    return false;
  }

  @Override
  public boolean isSupportsBooleanDataType() {
    return true;
  }

  @Override
  public boolean isSupportsErrorHandling() {
    return false;
  }

  @Override
  public boolean isSupportsFloatRoundingOnUpdate() {
    return false;
  }
}

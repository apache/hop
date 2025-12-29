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

package org.apache.hop.databases.duckdb;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

@DatabaseMetaPlugin(
    type = "DuckDB",
    typeDescription = "DuckDB",
    image = "duckdb.svg",
    documentationUrl = "/database/databases/duckdb.html")
@GuiPlugin(id = "GUI-DuckDBDatabaseMeta")
public class DuckDBDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  private static final Class<?> PKG = DuckDBDatabaseMeta.class;

  @Override
  public String getCreateTableStatement() {
    return super.getCreateTableStatement();
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v,
      String tk,
      String pk,
      boolean useAutoIncrement,
      boolean addFieldName,
      boolean addCr) {
    // https://duckdb.org/docs/sql/data_types/overview.html
    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      retval += fieldname + " ";
    } else {
      retval += fieldname + " TYPE ";
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
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER:
        if (fieldname.equalsIgnoreCase(tk)
            || // Technical key
            fieldname.equalsIgnoreCase(pk) // Primary key
        ) {
          retval += "IDENTITY";
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
        if (length >= DatabaseMeta.CLOB_LENGTH) {
          retval += "TEXT";
        } else {
          retval += "VARCHAR";
          if (length > 0) {
            retval += "(" + length;
          } else {
            retval += "(" + Integer.MAX_VALUE;
          }
          retval += ")";
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

  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public String getDriverClass() {
    return "org.duckdb.DuckDBDriver";
  }

  @Override
  public boolean isDuckDbVariant() {
    return true;
  }

  @Override
  public String getURL(String hostname, String port, String databaseName)
      throws HopDatabaseException {
    return "jdbc:duckdb:" + (databaseName.equals("memory") ? "" : databaseName);
  }

  @Override
  public String getAddColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " ADD COLUMN "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, true, false);
  }

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
        + " ALTER COLUMN "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, false, false);
  }

  @Override
  public boolean isSupportsOptionsInURL() {
    return false;
  }

  @Override
  public String[] getTableTypes() {
    return new String[] {"BASE TABLE", "LOCAL TEMPORARY"};
  }

  @Override
  public void addDefaultOptions() {
    setSupportsBooleanDataType(true);
    setSupportsTimestampDataType(true);
  }
}

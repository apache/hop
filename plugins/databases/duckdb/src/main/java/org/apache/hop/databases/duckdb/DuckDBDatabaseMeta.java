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

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.ColumnContext;
import org.apache.hop.core.database.types.DatabaseTypes;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.database.types.JdbcDateValues;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

@DatabaseMetaPlugin(
    type = "DuckDB",
    typeDescription = "DuckDB",
    image = "duckdb.svg",
    documentationUrl = "/database/databases/duckdb.html",
    classLoaderGroup = "duckdb-db")
@GuiPlugin(id = "GUI-DuckDBDatabaseMeta")
public class DuckDBDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  /** DuckDB limits rows at the end of the statement. */
  @Override
  public String getLimitClause(int nrRows) {
    return " LIMIT " + nrRows;
  }

  private static final List<IDatabaseTypeRule> TYPE_RULES =
      DatabaseTypes.rules()
          // As of DuckDB JDBC 0.10.0 the Calendar overloads of setDate and setTimestamp are not
          // implemented, so a configured time zone cannot be passed to the driver.
          .bind(IValueMeta.TYPE_DATE, JdbcDateValues.WITHOUT_CALENDAR_OVERLOADS)
          .build();

  @Override
  public List<IDatabaseTypeRule> getTypeRules() {
    return TYPE_RULES;
  }

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
          switch (type) {
            case IValueMeta.TYPE_INTEGER -> {
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
            }
            case IValueMeta.TYPE_BIGNUMBER -> {
              int p = (precision < 1) ? 16 : precision;
              int len = (length < 1) ? 16 : length;
              retval += "DECIMAL(" + len + "," + p + ")";
            }
            default -> retval += "DOUBLE";
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
  @SuppressWarnings("java:S1313") // the driver version is not an IP address
  public DriverDownload getDriverDownload() {
    return DriverDownload.builder()
        .mavenCoordinate("org.duckdb:duckdb_jdbc")
        .defaultVersion("1.5.4.0")
        .licenseCategory("A")
        .licenseName("MIT")
        .licenseUrl("https://github.com/duckdb/duckdb-java/blob/main/LICENSE")
        .vendor("DuckDB")
        .vendorUrl("https://duckdb.org/docs/stable/clients/java")
        .build();
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
        + getColumnDefinition(
            v, tk, pk, useAutoIncrement, true, false, ColumnContext.Purpose.ADD_COLUMN);
  }

  @Override
  public String getModifyColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    // The column name and the TYPE keyword belong to the ALTER syntax, not to the column
    // definition. Asking for a definition without a field name has to return the type on its own,
    // the way it does for every other dialect. See issue #3738, which was fixed the other way
    // around, inside getFieldDefinition.
    return "ALTER TABLE "
        + tableName
        + " ALTER COLUMN "
        + v.getName()
        + " TYPE "
        + getColumnDefinition(
            v, tk, pk, useAutoIncrement, false, false, ColumnContext.Purpose.MODIFY_COLUMN);
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

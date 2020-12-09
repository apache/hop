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

package org.apache.hop.databases.cache;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

/**
 * Contains DB2 specific information through static final members
 *
 * @author Matt
 * @since 11-mrt-2005
 */
@DatabaseMetaPlugin(
  type = "CACHE",
  typeDescription = "Intersystems Cache"
)
@GuiPlugin( id = "GUI-CacheDatabaseMeta" )
public class CacheDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  @Override
  public int[] getAccessTypeList() {
    return new int[] {
      DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public int getDefaultDatabasePort() {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE ) {
      return 1972;
    }
    return -1;
  }

  @Override
  public boolean supportsSetCharacterStream() {
    return false;
  }

  @Override
  public boolean isFetchSizeSupported() {
    return false;
  }

  /**
   * @return Whether or not the database can use auto increment type of fields (pk)
   */
  @Override
  public boolean supportsAutoInc() {
    return false;
  }

  @Override
  public String getDriverClass() {
    return "com.intersys.jdbc.CacheDriver";
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
      return "jdbc:Cache://" + hostname + ":" + port + "/" + databaseName;
  }

  /**
   * Generates the SQL statement to add a column to the specified table
   *
   * @param tableName   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                       String pk, boolean semicolon ) {
    return "ALTER TABLE "
      + tableName + " ADD COLUMN ( " + getFieldDefinition( v, tk, pk, useAutoinc, true, false ) + " ) ";
  }

  /**
   * Generates the SQL statement to drop a column from the specified table
   *
   * @param tableName   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to drop a column from the specified table
   */
  @Override
  public String getDropColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                        String pk, boolean semicolon ) {
    return "ALTER TABLE " + tableName + " DROP COLUMN " + v.getName() + Const.CR;
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tableName   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                          String pk, boolean semicolon ) {
    return "ALTER TABLE "
      + tableName + " ALTER COLUMN " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
  }

  @Override
  public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoinc,
                                    boolean addFieldName, boolean addCr ) {
    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if ( addFieldName ) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch ( type ) {
      case IValueMeta.TYPE_TIMESTAMP:
      case IValueMeta.TYPE_DATE:
        retval += "TIMESTAMP";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        retval += "CHAR(1)";
        break;
      case IValueMeta.TYPE_NUMBER:
      case IValueMeta.TYPE_INTEGER:
      case IValueMeta.TYPE_BIGNUMBER:
        if ( fieldname.equalsIgnoreCase( tk ) ) { // Technical & primary key : see at bottom
          retval += "DECIMAL";
        } else {
          if ( length < 0 || precision < 0 ) {
            retval += "DOUBLE";
          } else if ( precision > 0 || length > 9 ) {
            retval += "DECIMAL(" + length;
            if ( precision > 0 ) {
              retval += ", " + precision;
            }
            retval += ")";
          } else {
            // Precision == 0 && length<=9
            retval += "INT";
          }
        }
        break;
      case IValueMeta.TYPE_STRING: // CLOBs are just VARCHAR in the Cache database: can be very large!
        retval += "VARCHAR";
        if ( length > 0 ) {
          retval += "(" + length + ")";
        }
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

  /**
   * @return true if we need to append the PRIMARY KEY block in the create table block after the fields, required for
   * Cache.
   */
  @Override
  public boolean requiresCreateTablePrimaryKeyAppend() {
    return true;
  }

  /**
   * @return true if the database supports newlines in a SQL statements.
   */
  @Override
  public boolean supportsNewLinesInSql() {
    return false;
  }

}

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

package org.apache.hop.databases.informix;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;

/**
 * Contains Informix specific information through static final members
 *
 * @author Matt
 * @since 11-mrt-2005
 */
@DatabaseMetaPlugin(
  type = "INFORMIX",
  typeDescription = "Informix"
)
@GuiPlugin( id = "GUI-InformixDatabaseMeta" )
public class InformixDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  @GuiWidgetElement(
    id = "servername",
    order = "10",
    parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    variables = true,    
    label = "i18n:org.apache.hop.ui.core.database:DatabaseDialog.label.InformixServername"
  )
  protected boolean servername;

  @Override
  public int[] getAccessTypeList() {
    return new int[] {
      DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  @Override
  public int getDefaultDatabasePort() {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE ) {
      return 1526;
    }
    return -1;
  }

  /**
   * @see IDatabase#getNotFoundTK(boolean)
   */
  @Override
  public int getNotFoundTK( boolean useAutoinc ) {
    if ( supportsAutoInc() && useAutoinc ) {
      return 1;
    }
    return super.getNotFoundTK( useAutoinc );
  }

  @Override
  public String getDriverClass() {
    return "com.informix.jdbc.IfxDriver";
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) {
    return "jdbc:informix-sqli://"
            + hostname + ":" + port + "/" + databaseName + ":INFORMIXSERVER=" + getServername() + ";DELIMIDENT=Y";
  }

  /**
   * Indicates the need to insert a placeholder (0) for auto increment fields.
   *
   * @return true if we need a placeholder for auto increment fields in insert statements.
   */
  @Override
  public boolean needsPlaceHolder() {
    return true;
  }

  @Override
  public String getSqlQueryFields( String tableName ) {
    return "SELECT FIRST 1 * FROM " + tableName;
  }

  @Override
  public String getSqlTableExists( String tableName ) {
    return getSqlQueryFields( tableName );
  }

  @Override
  public String getSqlColumnExists( String columnname, String tableName ) {
    return getSqlQueryColumnFields( columnname, tableName );
  }

  public String getSqlQueryColumnFields( String columnname, String tableName ) {
    return "SELECT FIRST 1 " + columnname + " FROM " + tableName;
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
    return "ALTER TABLE " + tableName + " ADD " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
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
    return "ALTER TABLE " + tableName + " MODIFY " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
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
        retval += "DATETIME";
        break;
      case IValueMeta.TYPE_DATE:
        retval += "DATETIME YEAR to FRACTION";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if ( supportsBooleanDataType() ) {
          retval += "BOOLEAN";
        } else {
          retval += "CHAR(1)";
        }
        break;
      case IValueMeta.TYPE_NUMBER:
      case IValueMeta.TYPE_INTEGER:
      case IValueMeta.TYPE_BIGNUMBER:
        if ( fieldname.equalsIgnoreCase( tk ) || // Technical key
          fieldname.equalsIgnoreCase( pk ) // Primary key
        ) {
          if ( useAutoinc ) {
            retval += "SERIAL8";
          } else {
            retval += "INTEGER PRIMARY KEY";
          }
        } else {
          if ( ( length < 0 && precision < 0 ) || precision > 0 || length > 9 ) {
            retval += "FLOAT";
          } else { // Precision == 0 && length<=9
            retval += "INTEGER";
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if ( length >= DatabaseMeta.CLOB_LENGTH ) {
          retval += "CLOB";
        } else {
          if ( length < 256 ) {
            retval += "VARCHAR";
            if ( length > 0 ) {
              retval += "(" + length + ")";
            }
          } else {
            if ( length < 32768 ) {
              retval += "LVARCHAR";
            } else {
              retval += "TEXT";
            }
          }
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

  @Override
  public String getSqlLockTables( String[] tableNames ) {
    StringBuilder sql = new StringBuilder( 128 );
    for ( int i = 0; i < tableNames.length; i++ ) {
      sql.append( "LOCK TABLE " + tableNames[ i ] + " IN EXCLUSIVE MODE;" + Const.CR);
    }
    return sql.toString();
  }

  @Override
  public String getSqlUnlockTables( String[] tableNames ) {
    return null;
  }

  /**
   * Get the SQL to insert a new empty unknown record in a dimension.
   *
   * @param schemaTable  the schema-table name to insert into
   * @param keyField     The key field
   * @param versionField the version field
   * @return the SQL to insert the unknown record into the SCD.
   */
  @Override
  public String getSqlInsertAutoIncUnknownDimensionRow( String schemaTable, String keyField, String versionField ) {
    return "insert into " + schemaTable + "(" + keyField + ", " + versionField + ") values (1, 1)";
  }

  @Override
  public boolean isInformixVariant() {
    return true;
  }

}

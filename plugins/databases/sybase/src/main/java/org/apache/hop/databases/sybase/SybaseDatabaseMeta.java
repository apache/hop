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

package org.apache.hop.databases.sybase;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

/**
 * Contains Sybase specific information through static final members
 *
 * @author Matt
 * @since 11-mrt-2005
 */

@DatabaseMetaPlugin(
  type = "SYBASE",
  typeDescription = "Sybase"
)
@GuiPlugin( id = "GUI-SybaseDatabaseMeta" )
public class SybaseDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  @Override
  public int[] getAccessTypeList() {
    return new int[] {
      DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  @Override
  public int getDefaultDatabasePort() {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE ) {
      return 5001;
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
    return "net.sourceforge.jtds.jdbc.Driver";
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
    // jdbc:jtds:<server_type>://<server>[:<port>][/<database>][;<property>=<value>[;...]]
    return "jdbc:jtds:sybase://" + hostname + ":" + port + "/" + databaseName;
  }

  /**
   * @see IDatabase#getSchemaTableCombination(String, String)
   */
  @Override
  public String getSchemaTableCombination( String schemaName, String tablePart ) {
    return tablePart;
  }

  /**
   * @return true if this database needs a transaction to perform a query (auto-commit turned off).
   */
  @Override
  public boolean isRequiringTransactionsOnQueries() {
    return false;
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
      case IValueMeta.TYPE_DATE:
        retval += "DATETIME NULL";
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
        if ( fieldname.equalsIgnoreCase( tk ) || // Technical key: auto increment field!
          fieldname.equalsIgnoreCase( pk ) // Primary key
        ) {
          if ( useAutoinc ) {
            retval += "INTEGER IDENTITY NOT NULL";
          } else {
            retval += "INTEGER NOT NULL PRIMARY KEY";
          }
        } else {
          if ( precision != 0 || ( precision == 0 && length > 9 ) ) {
            if ( precision > 0 && length > 0 ) {
              retval += "DECIMAL(" + length + ", " + precision + ") NULL";
            } else {
              retval += "DOUBLE PRECISION NULL";
            }
          } else {
            // Precision == 0 && length<=9
            if ( length < 3 ) {
              retval += "TINYINT NULL";
            } else if ( length < 5 ) {
              retval += "SMALLINT NULL";
            } else {
              retval += "INTEGER NULL";
            }
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if ( length >= 2048 ) {
          retval += "TEXT NULL";
        } else {
          retval += "VARCHAR";
          if ( length > 0 ) {
            retval += "(" + length + ")";
          }
          retval += " NULL";
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
  public String getExtraOptionsHelpText() {
    return "http://jtds.sourceforge.net/faq.html#urlFormat";
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
    return "insert into " + schemaTable + "(" + versionField + ") values (1)";
  }

  /**
   * @param string
   * @return A string that is properly quoted for use in a SQL statement (insert, update, delete, etc)
   */
  @Override
  public String quoteSqlString(String string ) {
    string = string.replace( "'", "''" );
    string = string.replace( "\\n", "\\0xd" );
    string = string.replace( "\\r", "\\0xa" );
    return "'" + string + "'";
  }

  @Override
  public boolean isSybaseVariant() {
    return true;
  }
}

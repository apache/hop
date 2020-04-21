/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.databases.sybaseiq;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

/**
 * Contains Sybase IQ specific information through static final members
 *
 * @author fumigateAnt, liuhuaiyong <liuhuaiyong@gmail.com>
 * @since 18-09-2007
 */
@DatabaseMetaPlugin(
  type = "SYBASEIQ",
  typeDescription = "Sybase IQ"
)
@GuiPlugin( id = "GUI-SybaseIQDatabaseMeta" )
public class SybaseIQDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  @Override
  public int[] getAccessTypeList() {
    return new int[] {
      DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_ODBC };
  }

  @Override
  public int getDefaultDatabasePort() {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE ) {
      return 2638;
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
    return "com.sybase.jdbc3.jdbc.SybDriver";
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC ) {
      return "jdbc:odbc:" + databaseName;
    } else {
      return "jdbc:sybase:Tds:" + hostname + ":" + port + "/" + databaseName;
    }
  }

  /**
   * @see IDatabase#getSchemaTableCombination(java.lang.String, java.lang.String)
   */
  @Override
  public String getSchemaTableCombination( String schemaName, String tablePart ) {
    return schemaName + "." + tablePart;
  }

  /**
   * Generates the SQL statement to add a column to the specified table
   *
   * @param tablename   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement( String tablename, IValueMeta v, String tk, boolean useAutoinc,
                                       String pk, boolean semicolon ) {
    return "ALTER TABLE " + tablename + " ADD " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tablename   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement( String tablename, IValueMeta v, String tk, boolean useAutoinc,
                                          String pk, boolean semicolon ) {
    return "ALTER TABLE " + tablename + " MODIFY " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
  }

  @Override
  public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoinc,
                                    boolean addFieldname, boolean addCr ) {
    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if ( addFieldname ) {
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
   * @return true if we need to supply the schema-name to getTables in order to get a correct list of items.
   */
  @Override
  public boolean useSchemaNameForTableList() {
    return true;
  }

  /**
   * Returns the minimal SQL to launch in order to determine the layout of the resultset for a given database table
   * Note: added WHERE clause in SQL (just to make sure in case the sql is exec'd it will not clatter the db)
   *
   * @param tableName The name of the table to determine the layout for
   * @return The SQL to launch.
   */
  //
  @Override
  public String getSqlQueryFields( String tableName ) {
    return "SELECT * FROM " + tableName + " WHERE 1=2";
  }

  /**
   * Most databases allow you to retrieve result metadata by preparing a SELECT statement.
   *
   * @return true if the database supports retrieval of query metadata from a prepared statement. False if the query
   * needs to be executed first.
   */
  @Override
  public boolean supportsPreparedStatementMetadataRetrieval() {
    return false;
  }

}

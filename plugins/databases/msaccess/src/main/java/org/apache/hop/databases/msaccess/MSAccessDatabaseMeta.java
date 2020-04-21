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

package org.apache.hop.databases.msaccess;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;

import java.sql.ResultSet;

/**
 * Contains MySQL specific information through static final members
 *
 * @author Matt
 * @since 11-mrt-2005
 */
@DatabaseMetaPlugin(
  type = "MSACCESS",
  typeDescription = "MS Access"
)
@GuiPlugin( id = "GUI-MSAccessDatabaseMeta" )
public class MSAccessDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  @GuiWidgetElement( id = "hostname", type = GuiElementType.NONE, parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, ignored = true )
  protected String hostname;
  @GuiWidgetElement( id = "port", type = GuiElementType.NONE, parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, ignored = true )
  protected String port;
  @GuiWidgetElement( id = "databaseName", type = GuiElementType.NONE, parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, ignored = true )
  protected String databaseName;


  @Override
  public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_ODBC };
  }

  @Override
  public boolean supportsSetCharacterStream() {
    return false;
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
    return "sun.jdbc.odbc.JdbcOdbcDriver"; // always ODBC!
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
    return "jdbc:odbc:" + databaseName;
  }

  /**
   * Checks whether or not the command setFetchSize() is supported by the JDBC driver...
   *
   * @return true is setFetchSize() is supported!
   */
  @Override
  public boolean isFetchSizeSupported() {
    return false;
  }


  /**
   * Get the maximum length of a text field for this database connection. This includes optional CLOB, Memo and Text
   * fields. (the maximum!)
   *
   * @return The maximum text field length for this database type. (mostly CLOB_LENGTH)
   */
  @Override
  public int getMaxTextFieldLength() {
    return 65536;
  }

  /**
   * @return true if the database supports transactions.
   */
  @Override
  public boolean supportsTransactions() {
    return false;
  }

  /**
   * @return true if the database supports bitmap indexes
   */
  @Override
  public boolean supportsBitmapIndex() {
    return false;
  }

  /**
   * @return true if the database JDBC driver supports the setLong command
   */
  @Override
  public boolean supportsSetLong() {
    return false;
  }

  /**
   * @return true if the database supports views
   */
  @Override
  public boolean supportsViews() {
    return false;
  }

  /**
   * @return true if the database supports synonyms
   */
  @Override
  public boolean supportsSynonyms() {
    return false;
  }

  /**
   * @param tableName The table to be truncated.
   * @return The SQL statement to truncate a table: remove all rows from it without a transaction
   */
  @Override
  public String getTruncateTableStatement( String tableName ) {
    return "DELETE FROM " + tableName;
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
    return "ALTER TABLE " + tablename + " ADD COLUMN " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
  }

  /**
   * Generates the SQL statement to drop a column from the specified table
   *
   * @param tablename   The table to add
   * @param v           The column defined as a value
   * @param tk          the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk          the name of the primary key field
   * @param semicolon   whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to drop a column from the specified table
   */
  @Override
  public String getDropColumnStatement( String tablename, IValueMeta v, String tk, boolean useAutoinc,
                                        String pk, boolean semicolon ) {
    return "ALTER TABLE " + tablename + " DROP COLUMN " + v.getName() + Const.CR;
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
    return "ALTER TABLE "
      + tablename + " ALTER COLUMN " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
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
        retval += "DATETIME";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if ( supportsBooleanDataType() ) {
          retval += "BIT";
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
            retval += "COUNTER PRIMARY KEY";
          } else {
            retval += "LONG PRIMARY KEY";
          }
        } else {
          if ( precision == 0 ) {
            if ( length > 9 ) {
              retval += "DOUBLE";
            } else {
              if ( length > 5 ) {
                retval += "LONG";
              } else {
                retval += "INTEGER";
              }
            }
          } else {
            retval += "DOUBLE";
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if ( length > 0 ) {
          if ( length < 256 ) {
            retval += "TEXT(" + length + ")";
          } else {
            retval += "MEMO";
          }
        } else {
          retval += "TEXT";
        }
        break;
      case IValueMeta.TYPE_BINARY:
        retval += " LONGBINARY";
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

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.database.IDatabase#getReservedWords()
   */
  @Override
  public String[] getReservedWords() {
    return new String[] {
      /*
       * http://support.microsoft.com/kb/q109312 Note that if you set a reference to a type library, an object library,
       * or an ActiveX control, that library's reserved words are also reserved words in your database. For example, if
       * you add an ActiveX control to a form, a reference is set and the names of the objects, methods, and properties
       * of that control become reserved words in your database. For existing objects with names that contain reserved
       * words, you can avoid errors by surrounding the object name with brackets [ ], see
       * getStartQuote(),getEndQuote().
       */
      "ADD", "ALL", "ALPHANUMERIC", "ALTER", "AND", "ANY", "APPLICATION", "AS", "ASC", "ASSISTANT",
      "AUTOINCREMENT", "AVG", "BETWEEN", "BINARY", "BIT", "BOOLEAN", "BY", "BYTE", "CHAR", "CHARACTER",
      "COLUMN", "COMPACTDATABASE", "CONSTRAINT", "CONTAINER", "COUNT", "COUNTER", "CREATE", "CREATEDATABASE",
      "CREATEFIELD", "CREATEGROUP", "CREATEINDEX", "CREATEOBJECT", "CREATEPROPERTY", "CREATERELATION",
      "CREATETABLEDEF", "CREATEUSER", "CREATEWORKSPACE", "CURRENCY", "CURRENTUSER", "DATABASE", "DATE",
      "DATETIME", "DELETE", "DESC", "DESCRIPTION", "DISALLOW", "DISTINCT", "DISTINCTROW", "DOCUMENT", "DOUBLE",
      "DROP", "ECHO", "ELSE", "END", "EQV", "ERROR", "EXISTS", "EXIT", "FALSE", "FIELD", "FIELDS", "FILLCACHE",
      "FLOAT", "FLOAT4", "FLOAT8", "FOREIGN", "FORM", "FORMS", "FROM", "FULL", "FUNCTION", "GENERAL",
      "GETOBJECT", "GETOPTION", "GOTOPAGE", "GROUP", "GUID", "HAVING", "IDLE", "IEEEDOUBLE", "IEEESINGLE", "IF",
      "IGNORE", "IMP", "IN", "INDEX", "INDEX", "INDEXES", "INNER", "INSERT", "INSERTTEXT", "INT", "INTEGER",
      "INTEGER1", "INTEGER2", "INTEGER4", "INTO", "IS", "JOIN", "KEY", "LASTMODIFIED", "LEFT", "LEVEL", "LIKE",
      "LOGICAL", "LOGICAL1", "LONG", "LONGBINARY", "LONGTEXT", "MACRO", "MATCH", "MAX", "MIN", "MOD", "MEMO",
      "MODULE", "MONEY", "MOVE", "NAME", "NEWPASSWORD", "NO", "NOT", "NULL", "NUMBER", "NUMERIC", "OBJECT",
      "OLEOBJECT", "OFF", "ON", "OPENRECORDSET", "OPTION", "OR", "ORDER", "OUTER", "OWNERACCESS", "PARAMETER",
      "PARAMETERS", "PARTIAL", "PERCENT", "PIVOT", "PRIMARY", "PROCEDURE", "PROPERTY", "QUERIES", "QUERY",
      "QUIT", "REAL", "RECALC", "RECORDSET", "REFERENCES", "REFRESH", "REFRESHLINK", "REGISTERDATABASE",
      "RELATION", "REPAINT", "REPAIRDATABASE", "REPORT", "REPORTS", "REQUERY", "RIGHT", "SCREEN", "SECTION",
      "SELECT", "SET", "SETFOCUS", "SETOPTION", "SHORT", "SINGLE", "SMALLINT", "SOME", "SQL", "STDEV", "STDEVP",
      "STRING", "SUM", "TABLE", "TABLEDEF", "TABLEDEFS", "TABLEID", "TEXT", "TIME", "TIMESTAMP", "TOP",
      "TRANSFORM", "TRUE", "TYPE", "UNION", "UNIQUE", "UPDATE", "USER", "VALUE", "VALUES", "VAR", "VARP",
      "VARBINARY", "VARCHAR", "WHERE", "WITH", "WORKSPACE", "XOR", "YEAR", "YES", "YESNO" };
  }

  /**
   * @return The start quote sequence, mostly just double quote, but sometimes [, ...
   */
  @Override
  public String getStartQuote() {
    return "[";
  }

  /**
   * @return The end quote sequence, mostly just double quote, but sometimes ], ...
   */
  @Override
  public String getEndQuote() {
    return "]";
  }

  @Override
  public boolean supportsGetBlob() {
    return false;
  }

  /**
   * Verifies on the specified database connection if an index exists on the fields with the specified name.
   *
   * @param database   a connected database
   * @param schemaName
   * @param tableName
   * @param idxFields
   * @return true if the index exists, false if it doesn't.
   * @throws HopDatabaseException
   */
  @Override
  public boolean checkIndexExists( Database database, String schemaName, String tableName, String[] idxFields ) throws HopDatabaseException {

    String tablename = database.getDatabaseMeta().getQuotedSchemaTableCombination( schemaName, tableName );

    boolean[] exists = new boolean[ idxFields.length ];
    for ( int i = 0; i < exists.length; i++ ) {
      exists[ i ] = false;
    }

    try {
      // Get a list of all the indexes for this table
      //
      ResultSet indexList = null;
      try {
        indexList = database.getDatabaseMetaData().getIndexInfo( null, null, tablename, false, true );
        while ( indexList.next() ) {
          String column = indexList.getString( "COLUMN_NAME" );

          int idx = Const.indexOfString( column, idxFields );
          if ( idx >= 0 ) {
            exists[ idx ] = true;
          }
        }
      } finally {
        if ( indexList != null ) {
          indexList.close();
        }
      }

      // See if all the fields are indexed...
      boolean all = true;
      for ( int i = 0; i < exists.length && all; i++ ) {
        if ( !exists[ i ] ) {
          all = false;
        }
      }

      return all;
    } catch ( Exception e ) {
      throw new HopDatabaseException( "Unable to determine if indexes exists on table [" + tablename + "]", e );
    }
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
   * Get the schema-table combination to query the right table. Usually that is SCHEMA.TABLENAME, however there are
   * exceptions to this rule...
   *
   * @param schema_name The schema name
   * @param table_part  The tablename
   * @return the schema-table combination to query the right table.
   */
  @Override
  public String getSchemaTableCombination( String schema_name, String table_part ) {
    return getStartQuote() + schema_name + getEndQuote() + "." + getStartQuote() + table_part + getEndQuote() ;
  }

}

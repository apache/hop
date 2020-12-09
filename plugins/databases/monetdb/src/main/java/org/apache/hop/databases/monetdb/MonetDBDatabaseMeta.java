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

package org.apache.hop.databases.monetdb;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

/**
 * Contains Generic Database Connection information through static final members
 *
 * @author Matt
 * @since 11-mrt-2005
 */
@DatabaseMetaPlugin(
  type = "MONETDB",
  typeDescription = "MonetDB"
)
@GuiPlugin( id = "GUI-MonetDBDatabaseMeta" )
public class MonetDBDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  public static ThreadLocal<Boolean> safeModeLocal = new ThreadLocal<>();

  public static final int DEFAULT_VARCHAR_LENGTH = 100;

  protected static final String FIELDNAME_PROTECTOR = "_";

  private static final int MAX_VARCHAR_LENGTH = Integer.MAX_VALUE;

  @Override
  public int[] getAccessTypeList() {
    return new int[] {
      DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  /**
   * @see IDatabase#getDefaultDatabasePort()
   */
  @Override
  public int getDefaultDatabasePort() {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE ) {
      return 50000; // According to https://www.monetdb.org/Documentation/monetdb-man-page
    } else {
      return -1;
    }
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
    return "nl.cwi.monetdb.jdbc.MonetDriver";
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
      if ( Utils.isEmpty( port ) ) {
        return "jdbc:monetdb://" + hostname + "/" + databaseName;
      } else {
        return "jdbc:monetdb://" + hostname + ":" + port + "/" + databaseName;
      }
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
   * @return true if the database supports bitmap indexes
   */
  @Override
  public boolean supportsBitmapIndex() {
    return true;
  }

  @Override
  public boolean supportsAutoInc() {
    return true;
  }

  @Override
  public boolean supportsBatchUpdates() {
    return true;
  }

  @Override
  public boolean supportsSetMaxRows() {
    return true;
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
   * Generates the SQL statement to add a column to the specified table For this generic type, i set it to the most
   * common possibility.
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
  public String[] getReservedWords() {
    return new String[] {
      "IS", "ISNULL", "NOTNULL", "IN", "BETWEEN", "OVERLAPS", "LIKE", "ILIKE", "NOT", "AND", "OR", "CHAR", "VARCHAR",
      "CLOB", "BLOB", "DECIMAL", "DEC", "NUMERIC", "TINYINT", "SMALLINT", "INT", "BIGINT", "REAL", "DOUBLE", "BOOLEAN",
      "DATE", "TIME", "TIMESTAMP", "INTERVAL", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "TIMEZONE", "EXTRACT",
      "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP", "CURRENT_TIME", "SERIAL", "START",
      "WITH", "INCREMENT", "CACHE", "CYCLE", "SEQUENCE", "GETANCHOR", "GETBASENAME", "GETCONTENT", "GETCONTEXT", "GETDOMAIN",
      "GETEXTENSION", "GETFILE", "GETHOST", "GETPORT", "GETPROTOCOL", "GETQUERY", "GETUSER", "GETROBOTURL", "ISURL", "NEWURL",
      "BROADCAST", "MASKLEN", "SETMASKLEN", "NETMASK", "HOSTMASK", "NETWORK", "TEXT", "ABBREV", "CREATE", "TYPE", "NAME", "DROP",
      "USER" };
  }

  @Override
  public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoinc,
                                    boolean addFieldName, boolean addCr ) {
    StringBuilder retval = new StringBuilder();

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    Boolean mode = MonetDBDatabaseMeta.safeModeLocal.get();
    boolean safeMode = mode != null && mode.booleanValue();

    if ( addFieldName ) {
      // protect the fieldname
      if ( safeMode ) {
        fieldname = getSafeFieldname( fieldname );
      }

      retval.append( fieldname + " " );
    }

    int type = v.getType();
    switch ( type ) {
      case IValueMeta.TYPE_TIMESTAMP:
      case IValueMeta.TYPE_DATE:
        retval.append( "TIMESTAMP" );
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if ( supportsBooleanDataType() ) {
          retval.append( "BOOLEAN" );
        } else {
          retval.append( "CHAR(1)" );
        }
        break;
      case IValueMeta.TYPE_NUMBER:
      case IValueMeta.TYPE_INTEGER:
      case IValueMeta.TYPE_BIGNUMBER:
        if ( fieldname.equalsIgnoreCase( tk ) || // Technical key
          fieldname.equalsIgnoreCase( pk ) // Primary key
        ) {
          if ( useAutoinc ) {
            retval.append( "SERIAL" );
          } else {
            retval.append( "BIGINT" );
          }
        } else {
          // Integer values...
          if ( precision == 0 ) {
            if ( length > 9 ) {
              if ( length < 19 ) {
                // can hold signed values between -9223372036854775808 and 9223372036854775807
                // 18 significant digits
                retval.append( "BIGINT" );
              } else {
                retval.append( "DECIMAL(" ).append( length ).append( ")" );
              }
            } else if ( type == IValueMeta.TYPE_NUMBER ) {
              retval.append( "DOUBLE" );
            } else {
              retval.append( "BIGINT" );
            }
          } else {
            // Floating point values...
            if ( length > 15 ) {
              retval.append( "DECIMAL(" ).append( length );
              if ( precision > 0 ) {
                retval.append( ", " ).append( precision );
              }
              retval.append( ")" );
            } else {
              // A double-precision floating-point number is accurate to approximately 15 decimal places.
              // http://mysql.mirrors-r-us.net/doc/refman/5.1/en/numeric-type-overview.html
              retval.append( "DOUBLE" );
            }
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if ( length > getMaxVARCHARLength() ) {
          retval.append( "CLOB" );
        } else {
          retval.append( "VARCHAR(" );
          if ( length > 0 ) {
            retval.append( length );
          } else {
            if ( safeMode ) {
              retval.append( DEFAULT_VARCHAR_LENGTH );
            }
          }
          retval.append( ")" );
        }
        break;
      default:
        retval.append( " UNKNOWN" );
        break;
    }

    if ( addCr ) {
      retval.append( Const.CR );
    }

    return retval.toString();
  }

  /**
   * Returns the minimal SQL to launch in order to determine the layout of the resultset for a given database table
   *
   * @param tableName The name of the table to determine the layout for
   * @return The SQL to launch.
   */
  @Override
  public String getSqlQueryFields( String tableName ) {
    return "SELECT * FROM " + tableName + ";";
  }

  @Override
  public boolean supportsResultSetMetadataRetrievalOnly() {
    return true;
  }

  @Override
  public int getMaxVARCHARLength() {
    return MAX_VARCHAR_LENGTH;
  }

  @Override
  public boolean supportsSequences() {
    return true;
  }

  @Override
  public String getSqlListOfSequences() {
    return "SELECT name FROM sys.sequences";
  }

  @Override
  public String getSqlSequenceExists( String sequenceName ) {
    return String.format( "SELECT * FROM sys.sequences WHERE name = '%s'", sequenceName );
  }

  @Override
  public String getSqlCurrentSequenceValue( String sequenceName ) {
    String realSequenceName = sequenceName.replace( getStartQuote(), "" ).replace( getEndQuote(), "" );
    return String.format( "SELECT get_value_for( 'sys', '%s' )", realSequenceName );
  }

  @Override
  public String getSqlNextSequenceValue( String sequenceName ) {
    String realSequenceName = sequenceName.replace( getStartQuote(), "" ).replace( getEndQuote(), "" );
    return String.format( "SELECT next_value_for( 'sys', '%s' )", realSequenceName );
  }
}

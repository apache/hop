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

package org.apache.hop.databases.universe;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

/**
 * Contains IBM UniVerse database specific information through static final members
 *
 * @author Matt
 * @since 16-nov-2006
 */
@DatabaseMetaPlugin(
  type = "UNIVERSE",
  typeDescription = "UniVerse database"
)
@GuiPlugin( id = "GUI-UniVerseDatabaseMeta" )
public class UniVerseDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  private static final int MAX_VARCHAR_LENGTH = 65535;

  @Override
  public int[] getAccessTypeList() {
    return new int[] {
      DatabaseMeta.TYPE_ACCESS_NATIVE };
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
    return "com.ibm.u2.jdbc.UniJDBCDriver";
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
    return "jdbc:ibm-u2://" + hostname + "/" + databaseName;
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
   * UniVerse doesn't even support timestamps.
   */
  @Override
  public boolean supportsTimeStampToDateConversion() {
    return false;
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
      case IValueMeta.TYPE_DATE:
        retval += "DATE";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        retval += "CHAR(1)";
        break;
      case IValueMeta.TYPE_NUMBER:
      case IValueMeta.TYPE_INTEGER:
      case IValueMeta.TYPE_BIGNUMBER:
        if ( fieldname.equalsIgnoreCase( tk ) || // Technical key
          fieldname.equalsIgnoreCase( pk ) // Primary key
        ) {
          retval += "INTEGER";
        } else {
          if ( length > 0 ) {
            if ( precision > 0 || length > 18 ) {
              retval += "DECIMAL(" + length + ", " + precision + ")";
            } else {
              retval += "INTEGER";
            }

          } else {
            retval += "DOUBLE PRECISION";
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if ( length >= MAX_VARCHAR_LENGTH || length <= 0 ) {
          retval += "VARCHAR(" + MAX_VARCHAR_LENGTH + ")";
        } else {
          retval += "VARCHAR(" + length + ")";
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
  public String[] getReservedWords() {
    return new String[] {
      "@NEW", "@OLD", "ACTION", "ADD", "AL", "ALL", "ALTER", "AND", "AR", "AS", "ASC", "ASSOC", "ASSOCIATED",
      "ASSOCIATION", "AUTHORIZATION", "AVERAGE", "AVG", "BEFORE", "BETWEEN", "BIT", "BOTH", "BY", "CALC",
      "CASCADE", "CASCADED", "CAST", "CHAR", "CHAR_LENGTH", "CHARACTER", "CHARACTER_LENGTH", "CHECK", "COL.HDG",
      "COL.SPACES", "COL.SPCS", "COL.SUP", "COLUMN", "COMPILED", "CONNECT", "CONSTRAINT", "CONV", "CONVERSION",
      "COUNT", "COUNT.SUP", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "DATA", "DATE", "DBA", "DBL.SPC",
      "DEC", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DET.SUP", "DICT", "DISPLAY.NAME", "DISPLAYLIKE",
      "DISPLAYNAME", "DISTINCT", "DL", "DOUBLE", "DR", "DROP", "DYNAMIC", "E.EXIST", "EMPTY", "EQ", "EQUAL",
      "ESCAPE", "EVAL", "EVERY", "EXISTING", "EXISTS", "EXPLAIN", "EXPLICIT", "FAILURE", "FIRST", "FLOAT",
      "FMT", "FOOTER", "FOOTING", "FOR", "FOREIGN", "FORMAT", "FROM", "FULL", "GE", "GENERAL", "GRAND",
      "GRAND.TOTAL", "GRANT", "GREATER", "GROUP", "GROUP.SIZE", "GT", "HAVING", "HEADER", "HEADING", "HOME",
      "IMPLICIT", "IN", "INDEX", "INNER", "INQUIRING", "INSERT", "INT", "INTEGER", "INTO", "IS", "JOIN", "KEY",
      "LARGE.RECORD", "LAST", "LE", "LEADING", "LEFT", "LESS", "LIKE", "LOCAL", "LOWER", "LPTR", "MARGIN",
      "MATCHES", "MATCHING", "MAX", "MERGE.LOAD", "MIN", "MINIMIZE.SPACE", "MINIMUM.MODULUS", "MODULO",
      "MULTI.VALUE", "MULTIVALUED", "NATIONAL", "NCHAR", "NE", "NO", "NO.INDEX", "NO.OPTIMIZE", "NO.PAGE",
      "NOPAGE", "NOT", "NRKEY", "NULL", "NUMERIC", "NVARCHAR", "ON", "OPTION", "OR", "ORDER", "OUTER", "PCT",
      "PRECISION", "PRESERVING", "PRIMARY", "PRIVILEGES", "PUBLIC", "REAL", "RECORD.SIZE", "REFERENCES",
      "REPORTING", "RESOURCE", "RESTORE", "RESTRICT", "REVOKE", "RIGHT", "ROWUNIQUE", "SAID", "SAMPLE",
      "SAMPLED", "SCHEMA", "SELECT", "SEPARATION", "SEQ.NUM", "SET", "SINGLE.VALUE", "SINGLEVALUED", "SLIST",
      "SMALLINT", "SOME", "SPLIT.LOAD", "SPOKEN", "SUBSTRING", "SUCCESS", "SUM", "SUPPRESS", "SYNONYM", "TABLE",
      "TIME", "TO", "TOTAL", "TRAILING", "TRIM", "TYPE", "UNION", "UNIQUE", "UNNEST", "UNORDERED", "UPDATE",
      "UPPER", "USER", "USING", "VALUES", "VARBIT", "VARCHAR", "VARYING", "VERT", "VERTICALLY", "VIEW", "WHEN",
      "WHERE", "WITH", };
  }

  /**
   * @return true if the database supports newlines in a SQL statements.
   */
  @Override
  public boolean supportsNewLinesInSql() {
    return true;
  }


  @Override
  public int getMaxVARCHARLength() {
    return UniVerseDatabaseMeta.MAX_VARCHAR_LENGTH;
  }

}

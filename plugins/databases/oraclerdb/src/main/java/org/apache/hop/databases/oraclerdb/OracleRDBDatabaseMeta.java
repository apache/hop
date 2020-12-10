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

package org.apache.hop.databases.oraclerdb;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;

/**
 * Contains Oracle RDB specific information through static final members
 *
 * @author Matt
 * @since 27-jul-2006
 */
@DatabaseMetaPlugin(
  type = "ORACLERDB",
  typeDescription = "Oracle RDB"
)
@GuiPlugin( id = "GUI-OracleRDBDatabaseMeta" )
public class OracleRDBDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  @Override
  public int[] getAccessTypeList() {
    return new int[] {
      DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  /**
   * @return Whether or not the database can use auto increment type of fields (pk)
   */
  @Override
  public boolean supportsAutoInc() {
    return false;
  }

  /**
   * @see IDatabase#getLimitClause(int)
   */
  @Override
  public String getLimitClause( int nrRows ) {
    return " WHERE ROWNUM <= " + nrRows;
  }

  /**
   * Returns the minimal SQL to launch in order to determine the layout of the resultset for a given database table
   *
   * @param tableName The name of the table to determine the layout for
   * @return The SQL to launch.
   */
  @Override
  public String getSqlQueryFields( String tableName ) {
    return "SELECT * FROM " + tableName + " WHERE 1=0";
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
    return "SELECT " + columnname + " FROM " + tableName + " WHERE 1=0";
  }

  @Override
  public String getDriverClass() {
    return "oracle.rdb.jdbc.rdbThin.Driver";
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) throws HopDatabaseException {
      return "jdbc:rdbThin://" + hostname + ":" + port + "/" + databaseName;
  }

  /**
   * Oracle doesn't support options in the URL, we need to put these in a Properties object at connection time...
   */
  @Override
  public boolean supportsOptionsInURL() {
    return false;
  }

  /**
   * @return true if the database supports sequences
   */
  @Override
  public boolean supportsSequences() {
    return true;
  }

  @Override
  public String getSqlListOfSequences() {
    return "SELECT SEQUENCE_NAME FROM USER_SEQUENCES";
  }

  /**
   * Check if a sequence exists.
   *
   * @param sequenceName The sequence to check
   * @return The SQL to get the name of the sequence back from the databases data dictionary
   */
  @Override
  public String getSqlSequenceExists( String sequenceName ) {
    return "SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = '" + sequenceName.toUpperCase() + "'";
  }

  /**
   * Get the current value of a database sequence
   *
   * @param sequenceName The sequence to check
   * @return The current value of a database sequence
   */
  @Override
  public String getSqlCurrentSequenceValue( String sequenceName ) {
    return "SELECT " + sequenceName + ".currval FROM DUAL";
  }

  /**
   * Get the SQL to get the next value of a sequence. (Oracle only)
   *
   * @param sequenceName The sequence name
   * @return the SQL to get the next value of a sequence. (Oracle only)
   */
  @Override
  public String getSqlNextSequenceValue( String sequenceName ) {
    return "SELECT " + sequenceName + ".nextval FROM dual";
  }

  /**
   * @return true if we need to supply the schema-name to getTables in order to get a correct list of items.
   */
  @Override
  public boolean useSchemaNameForTableList() {
    return true;
  }

  /**
   * @return true if the database supports synonyms
   */
  @Override
  public boolean supportsSynonyms() {
    return true;
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
      + tableName + " ADD ( " + getFieldDefinition( v, tk, pk, useAutoinc, true, false ) + " ) ";
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
    return "ALTER TABLE " + tableName + " DROP ( " + v.getName() + " ) " + Const.CR;
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
      + tableName + " MODIFY (" + getFieldDefinition( v, tk, pk, useAutoinc, true, false ) + " )";
  }

  @Override
  public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoinc,
                                    boolean addFieldName, boolean addCr ) {
    StringBuilder retval = new StringBuilder( 128 );

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if ( addFieldName ) {
      retval.append( fieldname ).append( ' ' );
    }

    int type = v.getType();
    switch ( type ) {
      case IValueMeta.TYPE_DATE:
        retval.append( "DATE" );
        break;
      case IValueMeta.TYPE_BOOLEAN:
        retval.append( "CHAR(1)" );
        break;
      case IValueMeta.TYPE_NUMBER:
      case IValueMeta.TYPE_INTEGER:
      case IValueMeta.TYPE_BIGNUMBER:
        retval.append( "NUMBER" );
        if ( length > 0 ) {
          retval.append( '(' ).append( length );
          if ( precision > 0 ) {
            retval.append( ", " ).append( precision );
          }
          retval.append( ')' );
        }
        break;
      case IValueMeta.TYPE_STRING:
        if ( length >= DatabaseMeta.CLOB_LENGTH ) {
          retval.append( "CLOB" );
        } else {
          if ( length > 0 && length <= 2000 ) {
            retval.append( "VARCHAR2(" ).append( length ).append( ')' );
          } else {
            if ( length <= 0 ) {
              retval.append( "VARCHAR2(2000)" ); // We don't know, so we just use the maximum...
            } else {
              retval.append( "CLOB" );
            }
          }
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

  /*
   * (non-Javadoc)
   *
   * @see com.ibridge.hop.core.database.IDatabase#getReservedWords()
   */
  @Override
  public String[] getReservedWords() {
    return new String[] {
      "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "ARRAYLEN", "AS", "ASC", "AUDIT", "BETWEEN", "BY", "CHAR",
      "CHECK", "CLUSTER", "COLUMN", "COMMENT", "COMPRESS", "CONNECT", "CREATE", "CURRENT", "DATE", "DECIMAL",
      "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR",
      "FROM", "GRANT", "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INITIAL",
      "INSERT", "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS",
      "MODE", "MODIFY", "NOAUDIT", "NOCOMPRESS", "NOT", "NOTFOUND", "NOWAIT", "NULL", "NUMBER", "OF", "OFFLINE",
      "ON", "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "RAW", "RENAME",
      "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWLABEL", "ROWNUM", "ROWS", "SELECT", "SESSION", "SET", "SHARE",
      "SIZE", "SMALLINT", "SQLBUF", "START", "SUCCESSFUL", "SYNONYM", "SYSDATE", "TABLE", "THEN", "TO",
      "TRIGGER", "UID", "UNION", "UNIQUE", "UPDATE", "USER", "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2",
      "VIEW", "WHENEVER", "WHERE", "WITH" };
  }

  @Override
  public String getSqlLockTables( String[] tableNames ) {
    StringBuilder sql = new StringBuilder( 128 );
    for ( int i = 0; i < tableNames.length; i++ ) {
      sql.append( "LOCK TABLE " ).append( tableNames[ i ] ).append( " IN EXCLUSIVE MODE;" ).append( Const.CR );
    }
    return sql.toString();
  }

  @Override
  public String getSqlUnlockTables( String[] tableNames ) {
    return null; // commit handles the unlocking!
  }

  @Override
  public String getStartQuote(){ return ""; }

  @Override
  public String getEndQuote(){ return ""; }

}

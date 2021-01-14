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

package org.apache.hop.databases.mssql;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.*;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.ResultSet;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class MsSqlServerDatabaseMetaTest {
  MsSqlServerDatabaseMeta nativeMeta;
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  private DatabaseMeta databaseMeta;
  private IDatabase iDatabase;
  private IVariables variables;

  @BeforeClass
  public static void setUpOnce() throws HopPluginException, HopException {
    // Register Natives to create a default DatabaseMeta
    DatabasePluginType.getInstance().searchPlugins();
    ValueMetaPluginType.getInstance().searchPlugins();
    HopClientEnvironment.init();
  }

  @Before
  public void setupOnce() throws Exception {
    nativeMeta = new MsSqlServerDatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
    databaseMeta = new DatabaseMeta();
    iDatabase = mock( IDatabase.class );
    databaseMeta.setIDatabase( iDatabase );
    variables = spy( new Variables() );
  }

  @Test
  public void testSettings() throws Exception {
    assertFalse( nativeMeta.supportsCatalogs() );
    assertArrayEquals( new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE },
      nativeMeta.getAccessTypeList() );
    assertEquals( 1433, nativeMeta.getDefaultDatabasePort() );
    assertEquals( "net.sourceforge.jtds.jdbc.Driver", nativeMeta.getDriverClass() );

    assertEquals( "jdbc:jtds:sqlserver://FOO/WIBBLE", nativeMeta.getURL( "FOO", "", "WIBBLE" ) );
    assertEquals( "jdbc:jtds:sqlserver://FOO:1234/WIBBLE", nativeMeta.getURL( "FOO", "1234", "WIBBLE" ) );

    assertEquals( "FOO.BAR", nativeMeta.getSchemaTableCombination( "FOO", "BAR" ) );
    assertFalse( nativeMeta.supportsBitmapIndex() );

    assertArrayEquals( new String[] {
      /*
       * Transact-SQL Reference: Reserved Keywords Includes future keywords: could be reserved in future releases of SQL
       * Server as new features are implemented. REMARK: When SET QUOTED_IDENTIFIER is ON (default), identifiers can be
       * delimited by double quotation marks, and literals must be delimited by single quotation marks. When SET
       * QUOTED_IDENTIFIER is OFF, identifiers cannot be quoted and must follow all Transact-SQL rules for identifiers.
       */
      "ABSOLUTE", "ACTION", "ADD", "ADMIN", "AFTER", "AGGREGATE", "ALIAS", "ALL", "ALLOCATE", "ALTER", "AND",
      "ANY", "ARE", "ARRAY", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION", "BACKUP", "BEFORE", "BEGIN",
      "BETWEEN", "BINARY", "BIT", "BLOB", "BOOLEAN", "BOTH", "BREADTH", "BREAK", "BROWSE", "BULK", "BY", "CALL",
      "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR", "CHARACTER", "CHECK", "CHECKPOINT", "CLASS",
      "CLOB", "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLLATION", "COLUMN", "COMMIT", "COMPLETION",
      "COMPUTE", "CONNECT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONSTRUCTOR", "CONTAINS",
      "CONTAINSTABLE", "CONTINUE", "CONVERT", "CORRESPONDING", "CREATE", "CROSS", "CUBE", "CURRENT",
      "CURRENT_DATE", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
      "CURSOR", "CYCLE", "DATA", "DATABASE", "DATE", "DAY", "DBCC", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE",
      "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DENY", "DEPTH", "DEREF", "DESC", "DESCRIBE", "DESCRIPTOR",
      "DESTROY", "DESTRUCTOR", "DETERMINISTIC", "DIAGNOSTICS", "DICTIONARY", "DISCONNECT", "DISK", "DISTINCT",
      "DISTRIBUTED", "DOMAIN", "DOUBLE", "DROP", "DUMMY", "DUMP", "DYNAMIC", "EACH", "ELSE", "END", "END-EXEC",
      "EQUALS", "ERRLVL", "ESCAPE", "EVERY", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXIT",
      "EXTERNAL", "FALSE", "FETCH", "FILE", "FILLFACTOR", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND", "FREE",
      "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GENERAL", "GET", "GLOBAL", "GO", "GOTO",
      "GRANT", "GROUP", "GROUPING", "HAVING", "HOLDLOCK", "HOST", "HOUR", "IDENTITY", "IDENTITY_INSERT",
      "IDENTITYCOL", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDICATOR", "INITIALIZE", "INITIALLY",
      "INNER", "INOUT", "INPUT", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION",
      "ITERATE", "JOIN", "KEY", "KILL", "LANGUAGE", "LARGE", "LAST", "LATERAL", "LEADING", "LEFT", "LESS",
      "LEVEL", "LIKE", "LIMIT", "LINENO", "LOAD", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOCATOR", "MAP",
      "MATCH", "MINUTE", "MODIFIES", "MODIFY", "MODULE", "MONTH", "NAMES", "NATIONAL", "NATURAL", "NCHAR",
      "NCLOB", "NEW", "NEXT", "NO", "NOCHECK", "NONCLUSTERED", "NONE", "NOT", "NULL", "NULLIF", "NUMERIC",
      "OBJECT", "OF", "OFF", "OFFSETS", "OLD", "ON", "ONLY", "OPEN", "OPENDATASOURCE", "OPENQUERY",
      "OPENROWSET", "OPENXML", "OPERATION", "OPTION", "OR", "ORDER", "ORDINALITY", "OUT", "OUTER", "OUTPUT",
      "OVER", "PAD", "PARAMETER", "PARAMETERS", "PARTIAL", "PATH", "PERCENT", "PLAN", "POSTFIX", "PRECISION",
      "PREFIX", "PREORDER", "PREPARE", "PRESERVE", "PRIMARY", "PRINT", "PRIOR", "PRIVILEGES", "PROC",
      "PROCEDURE", "PUBLIC", "RAISERROR", "READ", "READS", "READTEXT", "REAL", "RECONFIGURE", "RECURSIVE",
      "REF", "REFERENCES", "REFERENCING", "RELATIVE", "REPLICATION", "RESTORE", "RESTRICT", "RESULT", "RETURN",
      "RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE", "ROW", "ROWCOUNT", "ROWGUIDCOL",
      "ROWS", "RULE", "SAVE", "SAVEPOINT", "SCHEMA", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SECTION", "SELECT",
      "SEQUENCE", "SESSION", "SESSION_USER", "SET", "SETS", "SETUSER", "SHUTDOWN", "SIZE", "SMALLINT", "SOME",
      "SPACE", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "START", "STATE",
      "STATEMENT", "STATIC", "STATISTICS", "STRUCTURE", "SYSTEM_USER", "TABLE", "TEMPORARY", "TERMINATE",
      "TEXTSIZE", "THAN", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TOP",
      "TRAILING", "TRAN", "TRANSACTION", "TRANSLATION", "TREAT", "TRIGGER", "TRUE", "TRUNCATE", "TSEQUAL",
      "UNDER", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPDATETEXT", "USAGE", "USE", "USER", "USING",
      "VALUE", "VALUES", "VARCHAR", "VARIABLE", "VARYING", "VIEW", "WAITFOR", "WHEN", "WHENEVER", "WHERE",
      "WHILE", "WITH", "WITHOUT", "WORK", "WRITE", "WRITETEXT", "YEAR", "ZONE" }, nativeMeta.getReservedWords() );

    assertEquals( "http://jtds.sourceforge.net/faq.html#urlFormat", nativeMeta.getExtraOptionsHelpText() );
    assertTrue( nativeMeta.supportsSchemas() );
    assertTrue( nativeMeta.supportsSequences() );
    assertTrue( nativeMeta.supportsSequenceNoMaxValueOption() );
    assertFalse( nativeMeta.useSafePoints() );
    assertTrue( nativeMeta.supportsErrorHandlingOnBatchUpdates() );
    assertEquals( 8000, nativeMeta.getMaxVARCHARLength() );
  }


  @Test
  public void testSqlStatements() {
    assertEquals( "SELECT TOP 1 * FROM FOO", nativeMeta.getSqlQueryFields( "FOO" ) );
    String lineSep = System.getProperty( "line.separator" );
    assertEquals( "SELECT top 0 * FROM FOO WITH (UPDLOCK, HOLDLOCK);"
        + lineSep + "SELECT top 0 * FROM BAR WITH (UPDLOCK, HOLDLOCK);" + lineSep,
      nativeMeta.getSqlLockTables( new String[] { "FOO", "BAR" } ) );

    assertEquals( "ALTER TABLE FOO ADD BAR DATETIME",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaDate( "BAR" ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR DATETIME",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaTimestamp( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO DROP COLUMN BAR" + lineSep,
      nativeMeta.getDropColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", true ) );

    assertEquals( "ALTER TABLE FOO ALTER COLUMN BAR VARCHAR(15)",
      nativeMeta.getModifyColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", true ) );

    assertEquals( "ALTER TABLE FOO ALTER COLUMN BAR VARCHAR(100)",
      nativeMeta.getModifyColumnStatement( "FOO", new ValueMetaString( "BAR" ), "", false, "", true ) );

    assertEquals( "select o.name from sysobjects o, sysusers u where  xtype in ( 'FN', 'P' ) and o.uid = u.uid order by o.name",
      nativeMeta.getSqlListOfProcedures() );

    assertEquals( "select name from sys.schemas", nativeMeta.getSqlListOfSchemas() );
    assertEquals( "insert into FOO(FOOVERSION) values (1)", nativeMeta.getSqlInsertAutoIncUnknownDimensionRow( "FOO", "FOOKEY", "FOOVERSION" ) );
    assertEquals( "SELECT NEXT VALUE FOR FOO", nativeMeta.getSqlNextSequenceValue( "FOO" ) );
    assertEquals( "SELECT current_value FROM sys.sequences WHERE name = 'FOO'", nativeMeta.getSqlCurrentSequenceValue( "FOO" ) );
    assertEquals( "SELECT 1 FROM sys.sequences WHERE name = 'FOO'", nativeMeta.getSqlSequenceExists( "FOO" ) );
    assertEquals( "SELECT name FROM sys.sequences", nativeMeta.getSqlListOfSequences() );
  }

  @Test
  public void testGetFieldDefinition() throws Exception {
    assertEquals( "CHAR(1)",
      nativeMeta.getFieldDefinition( new ValueMetaBoolean( "BAR" ), "", "", false, false, false ) );

    assertEquals( "BIGINT",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "BAR", 10, 0 ), "", "", false, false, false ) );

    assertEquals( "BIGINT",
      nativeMeta.getFieldDefinition( new ValueMetaBigNumber( "BAR", 10, 0 ), "", "", false, false, false ) );

    assertEquals( "BIGINT",
      nativeMeta.getFieldDefinition( new ValueMetaInteger( "BAR", 10, 0 ), "", "", false, false, false ) );

    assertEquals( "INT",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "BAR", 0, 0 ), "", "", false, false, false ) );

    assertEquals( "INT",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "BAR", 5, 0 ), "", "", false, false, false ) );

    assertEquals( "DECIMAL(10,3)",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "BAR", 10, 3 ), "", "", false, false, false ) );

    assertEquals( "DECIMAL(10,3)",
      nativeMeta.getFieldDefinition( new ValueMetaBigNumber( "BAR", 10, 3 ), "", "", false, false, false ) );

    assertEquals( "DECIMAL(21,4)",
      nativeMeta.getFieldDefinition( new ValueMetaBigNumber( "BAR", 21, 4 ), "", "", false, false, false ) );

    assertEquals( "TEXT",
      nativeMeta.getFieldDefinition( new ValueMetaString( "BAR", nativeMeta.getMaxVARCHARLength() + 2, 0 ), "", "", false, false, false ) );

    assertEquals( "VARCHAR(15)",
      nativeMeta.getFieldDefinition( new ValueMetaString( "BAR", 15, 0 ), "", "", false, false, false ) );

    assertEquals( "FLOAT(53)",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "BAR", 10, -7 ), "", "", false, false, false ) ); // Bug here - invalid SQL

    assertEquals( "DECIMAL(22,7)",
      nativeMeta.getFieldDefinition( new ValueMetaBigNumber( "BAR", 22, 7 ), "", "", false, false, false ) );
    assertEquals( "FLOAT(53)",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "BAR", -10, 7 ), "", "", false, false, false ) );
    assertEquals( "DECIMAL(5,7)",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "BAR", 5, 7 ), "", "", false, false, false ) );
    assertEquals( " UNKNOWN",
      nativeMeta.getFieldDefinition( new ValueMetaInternetAddress( "BAR" ), "", "", false, false, false ) );

    assertEquals( "BIGINT PRIMARY KEY IDENTITY(0,1)",
      nativeMeta.getFieldDefinition( new ValueMetaInteger( "BAR" ), "BAR", "", true, false, false ) );

    assertEquals( "BIGINT PRIMARY KEY",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "BAR" ), "BAR", "", false, false, false ) );

    assertEquals( "BIGINT PRIMARY KEY IDENTITY(0,1)",
      nativeMeta.getFieldDefinition( new ValueMetaInteger( "BAR" ), "", "BAR", true, false, false ) );
    assertEquals( "BIGINT PRIMARY KEY",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "BAR" ), "", "BAR", false, false, false ) );
    assertEquals( "VARBINARY(MAX)",
      nativeMeta.getFieldDefinition( new ValueMetaBinary(), "", "BAR", false, false, false ) );
    assertEquals( "VARBINARY(MAX)",
      nativeMeta.getFieldDefinition( new ValueMetaBinary( "BAR" ), "", "BAR", false, false, false ) );
  }

  private int rowCnt = 0;
  private String[] row1 = new String[] { "ROW1COL1", "ROW1COL2" };
  private String[] row2 = new String[] { "ROW2COL1", "ROW2COL2" };

  @Test
  public void testCheckIndexExists() throws Exception {
    String expectedSQL =
      "select i.name table_name, c.name column_name from     sysindexes i, sysindexkeys k, syscolumns c where    i.name = 'FOO' AND      i.id = k.id AND      i.id = c.id AND      k.colid = c.colid "
      ; // yes, variables at the end like in the dbmeta
    Database db = Mockito.mock( Database.class );
    IRowMeta rm = Mockito.mock( IRowMeta.class );
    ResultSet rs = Mockito.mock( ResultSet.class );
    DatabaseMeta dm = Mockito.mock( DatabaseMeta.class );
    Mockito.when( dm.getQuotedSchemaTableCombination( any(IVariables.class), eq(""), eq("FOO") ) ).thenReturn( "FOO" );
    Mockito.when( rs.next() ).thenReturn( rowCnt < 2 );
    Mockito.when( db.openQuery( expectedSQL ) ).thenReturn( rs );
    Mockito.when( db.getReturnRowMeta() ).thenReturn( rm );
    Mockito.when( rm.getString( row1, "column_name", "" ) ).thenReturn( "ROW1COL2" );
    Mockito.when( rm.getString( row2, "column_name", "" ) ).thenReturn( "ROW2COL2" );
    Mockito.when( db.getRow( rs ) ).thenAnswer( (Answer<Object[]>) invocation -> {
      rowCnt++;
      if ( rowCnt == 1 ) {
        return row1;
      } else if ( rowCnt == 2 ) {
        return row2;
      } else {
        return null;
      }
    } );
    Mockito.when( db.getDatabaseMeta() ).thenReturn( dm );
    assertTrue( nativeMeta.checkIndexExists( db, "", "FOO", new String[] { "ROW1COL2", "ROW2COL2" } ) );
    assertFalse( nativeMeta.checkIndexExists( db, "", "FOO", new String[] { "ROW2COL2", "NOTTHERE" } ) );
    assertFalse( nativeMeta.checkIndexExists( db, "", "FOO", new String[] { "NOTTHERE", "ROW1COL2" } ) );

  }

/*  @Test
  public void databases_WithSameDbConnTypes_AreTheSame() {
    IDatabase mssqlServerDatabaseMeta = new MSSQLServerDatabaseMeta();
    mssqlServerDatabaseMeta.setPluginId( "MSSQL" );
    assertTrue( databaseMeta.databaseForBothDbInterfacesIsTheSame( mssqlServerDatabaseMeta, mssqlServerDatabaseMeta ) );
  }*/

/*  @Test
  public void databases_WithSameDbConnTypes_AreNotSame_IfPluginIdIsNull() {
    IDatabase mssqlServerDatabaseMeta = new MSSQLServerDatabaseMeta();
    mssqlServerDatabaseMeta.setPluginId( null );
    assertFalse(
      databaseMeta.databaseForBothDbInterfacesIsTheSame( mssqlServerDatabaseMeta, mssqlServerDatabaseMeta ) );
  }*/

/*  @Test
  public void databases_WithDifferentDbConnTypes_AreDifferent_IfNonOfThemIsSubsetOfAnother() {
    IDatabase mssqlServerDatabaseMeta = new MSSQLServerDatabaseMeta();
    mssqlServerDatabaseMeta.setPluginId( "MSSQL" );
    IDatabase oracleDatabaseMeta = new OracleDatabaseMeta();
    oracleDatabaseMeta.setPluginId( "ORACLE" );

    assertFalse( databaseMeta.databaseForBothDbInterfacesIsTheSame( mssqlServerDatabaseMeta, oracleDatabaseMeta ) );
  }*/


/*  @Test
  public void databases_WithDifferentDbConnTypes_AreTheSame_IfOneConnTypeIsSubsetOfAnother_3LevelHierarchy() {
    class MSSQLServerNativeDatabaseMetaChild extends MSSQLServerDatabaseMeta {
      @Override
      public String getPluginId() {
        return "MSSQLNATIVE_CHILD";
      }
    }

    IDatabase mssqlServerDatabaseMeta = new MSSQLServerDatabaseMeta();
    mssqlServerDatabaseMeta.setPluginId( "MSSQL" );
    IDatabase mssqlServerNativeDatabaseMetaChild = new MSSQLServerNativeDatabaseMetaChild();

    assertTrue(
      databaseMeta
        .databaseForBothDbInterfacesIsTheSame( mssqlServerDatabaseMeta, mssqlServerNativeDatabaseMetaChild ) );
  }*/

}

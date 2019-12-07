/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class MSSQLServerNativeDatabaseMetaTest {
  @ClassRule
  public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  private DatabaseMeta databaseMeta;
  private DatabaseInterface databaseInterface;

  @Test
  public void testMSSQLOverrides() throws Exception {
    MSSQLServerNativeDatabaseMeta localNativeMeta = new MSSQLServerNativeDatabaseMeta();
    localNativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
    MSSQLServerNativeDatabaseMeta localOdbcMeta = new MSSQLServerNativeDatabaseMeta();
    localOdbcMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_ODBC );

    assertEquals( "com.microsoft.sqlserver.jdbc.SQLServerDriver", localNativeMeta.getDriverClass() );
    assertEquals( "sun.jdbc.odbc.JdbcOdbcDriver", localOdbcMeta.getDriverClass() );
    assertEquals( "jdbc:odbc:WIBBLE", localOdbcMeta.getURL( "FOO", "BAR", "WIBBLE" ) );

    assertEquals( "jdbc:sqlserver://FOO:1234;databaseName=WIBBLE;integratedSecurity=false",
        localNativeMeta.getURL( "FOO", "1234", "WIBBLE" ) );

    Properties attrs = new Properties();
    attrs.put( "MSSQLUseIntegratedSecurity", "false" );
    localNativeMeta.setAttributes( attrs );
    assertEquals( "jdbc:sqlserver://FOO:1234;databaseName=WIBBLE;integratedSecurity=false",
        localNativeMeta.getURL( "FOO", "1234", "WIBBLE" ) );
    attrs.put( "MSSQLUseIntegratedSecurity", "true" );
    assertEquals( "jdbc:sqlserver://FOO:1234;databaseName=WIBBLE;integratedSecurity=true",
        localNativeMeta.getURL( "FOO", "1234", "WIBBLE" ) );

  }

  @Test
  public void setSQLServerInstanceTest() {
    DatabaseMeta dbmeta = new DatabaseMeta();
    DatabaseInterface mssqlServerDatabaseMeta = new MSSQLServerDatabaseMeta();
    mssqlServerDatabaseMeta.setPluginId( "MSSQL" );
    DatabaseInterface mssqlServerNativeDatabaseMeta = new MSSQLServerNativeDatabaseMeta();
    mssqlServerNativeDatabaseMeta.setPluginId( "MSSQLNATIVE" );
    dbmeta.setDatabaseInterface( mssqlServerDatabaseMeta );
    dbmeta.setSQLServerInstance( "" );
    assertEquals( dbmeta.getSQLServerInstance(), null );
    dbmeta.setSQLServerInstance( "instance1" );
    assertEquals( dbmeta.getSQLServerInstance(), "instance1" );
    dbmeta.setDatabaseInterface( mssqlServerNativeDatabaseMeta );
    dbmeta.setSQLServerInstance( "" );
    assertEquals( dbmeta.getSQLServerInstance(), null );
    dbmeta.setSQLServerInstance( "instance1" );
    assertEquals( dbmeta.getSQLServerInstance(), "instance1" );
  }

  @Ignore
  @Test
  public void databases_WithDifferentDbConnTypes_AreTheSame_IfOneConnTypeIsSubsetOfAnother_2LevelHierarchy() {
    DatabaseInterface mssqlServerDatabaseMeta = new MSSQLServerDatabaseMeta();
    mssqlServerDatabaseMeta.setPluginId( "MSSQL" );
    DatabaseInterface mssqlServerNativeDatabaseMeta = new MSSQLServerNativeDatabaseMeta();
    mssqlServerNativeDatabaseMeta.setPluginId( "MSSQLNATIVE" );

    assertTrue( databaseMeta.databaseForBothDbInterfacesIsTheSame( mssqlServerDatabaseMeta,
            mssqlServerNativeDatabaseMeta ) );
  }

}

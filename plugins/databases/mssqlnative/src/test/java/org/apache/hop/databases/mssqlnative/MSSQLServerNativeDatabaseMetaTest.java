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

package org.apache.hop.databases.mssqlnative;

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.databases.mssql.MSSQLServerDatabaseMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MSSQLServerNativeDatabaseMetaTest {
  @ClassRule
  public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  private DatabaseMeta databaseMeta;
  private IDatabase iDatabase;

  @Test
  public void testMSSQLOverrides() throws Exception {
    MSSQLServerNativeDatabaseMeta localNativeMeta = new MSSQLServerNativeDatabaseMeta();
    localNativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
    MSSQLServerNativeDatabaseMeta localOdbcMeta = new MSSQLServerNativeDatabaseMeta();
    localOdbcMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_ODBC );

    assertEquals( "com.microsoft.sqlserver.jdbc.SQLServerDriver", localNativeMeta.getDriverClass() );
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
  public void setSqlServerInstanceTest() {
    DatabaseMeta dbmeta = new DatabaseMeta();
    IDatabase mssqlServerDatabaseMeta = new MSSQLServerDatabaseMeta();
    mssqlServerDatabaseMeta.setPluginId( "MSSQL" );
    IDatabase mssqlServerNativeDatabaseMeta = new MSSQLServerNativeDatabaseMeta();
    mssqlServerNativeDatabaseMeta.setPluginId( "MSSQLNATIVE" );
    dbmeta.setIDatabase( mssqlServerDatabaseMeta );
    dbmeta.setSqlServerInstance( "" );
    assertEquals( null , dbmeta.getSqlServerInstance());
    dbmeta.setSqlServerInstance( "instance1" );
    assertEquals(  "instance1" , dbmeta.getSqlServerInstance());
    dbmeta.setIDatabase( mssqlServerNativeDatabaseMeta );
    dbmeta.setSqlServerInstance( "" );
    assertEquals( null , dbmeta.getSqlServerInstance());
    dbmeta.setSqlServerInstance( "instance1" );
    assertEquals("instance1" , dbmeta.getSqlServerInstance());
  }

/*  @Ignore
  @Test
  public void databases_WithDifferentDbConnTypes_AreTheSame_IfOneConnTypeIsSubsetOfAnother_2LevelHierarchy() {
    IDatabase mssqlServerDatabaseMeta = new MSSQLServerDatabaseMeta();
    mssqlServerDatabaseMeta.setPluginId( "MSSQL" );
    IDatabase mssqlServerNativeDatabaseMeta = new MSSQLServerNativeDatabaseMeta();
    mssqlServerNativeDatabaseMeta.setPluginId( "MSSQLNATIVE" );

    assertTrue( databaseMeta.databaseForBothDbInterfacesIsTheSame( mssqlServerDatabaseMeta,
      mssqlServerNativeDatabaseMeta ) );
  }*/

}

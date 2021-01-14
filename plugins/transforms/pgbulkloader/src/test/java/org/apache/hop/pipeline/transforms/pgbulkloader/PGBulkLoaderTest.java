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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PGBulkLoaderTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  private TransformMockHelper<PGBulkLoaderMeta, PGBulkLoaderData> transformMockHelper;
  private PGBulkLoader pgBulkLoader;

  private static final String CONNECTION_NAME = "PSQLConnect";
  private static final String CONNECTION_DB_NAME = "test1181";
  private static final String CONNECTION_DB_HOST = "localhost";
  private static final String CONNECTION_DB_PORT = "5093";
  private static final String CONNECTION_DB_USERNAME = "postgres";
  private static final String CONNECTION_DB_PASSWORD = "password";
  private static final String DB_NAME_OVVERRIDE = "test1181_2";
  private static final String DB_NAME_EMPTY = "";

  @BeforeClass
  public static void setupBeforeClass() throws HopException {
    HopClientEnvironment.init();
  }

  @Before
  public void setUp() throws Exception {

    PluginRegistry.getInstance().registerPluginClass(
      PostgreSqlDatabaseMeta.class.getName(),
      DatabasePluginType.class,
      DatabaseMetaPlugin.class
    );

    transformMockHelper = new TransformMockHelper<>( "PostgreSQL Bulk Loader", PGBulkLoaderMeta.class, PGBulkLoaderData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn( transformMockHelper.iLogChannel );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    pgBulkLoader = new PGBulkLoader( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
  }

  @After
  public void tearDown() throws Exception {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testCreateCommandLine() throws Exception {
    PGBulkLoaderMeta meta = mock( PGBulkLoaderMeta.class );
    doReturn( new DatabaseMeta() ).when( meta ).getDatabaseMeta();
    doReturn( new String[ 0 ] ).when( meta ).getFieldStream();
    PGBulkLoaderData data = mock( PGBulkLoaderData.class );

    PGBulkLoader spy = spy( new PGBulkLoader( transformMockHelper.transformMeta, meta, data, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline ) );

    doReturn( new Object[ 0 ] ).when( spy ).getRow();
    doReturn( "" ).when( spy ).getCopyCommand();
    doNothing().when( spy ).connect();
    doNothing().when( spy ).checkClientEncoding();
    doNothing().when( spy ).processTruncate();
    spy.init();
    spy.processRow();
    verify( spy ).processTruncate();
  }

  @Test
  public void testDBNameOverridden_IfDbNameOverrideSetUp() throws Exception {
    // Db Name Override is set up
    PGBulkLoaderMeta pgBulkLoaderMock = getPgBulkLoaderMock( DB_NAME_OVVERRIDE );
    Database database = pgBulkLoader.getDatabase( pgBulkLoader, pgBulkLoaderMock );
    assertNotNull( database );
    // Verify DB name is overridden
    assertEquals( DB_NAME_OVVERRIDE, database.getDatabaseMeta().getDatabaseName() );
    // Check additionally other connection information
    assertEquals( CONNECTION_NAME, database.getDatabaseMeta().getName() );
    assertEquals( CONNECTION_DB_HOST, database.getDatabaseMeta().getHostname() );
    assertEquals( CONNECTION_DB_PORT, database.getDatabaseMeta().getPort() );
    assertEquals( CONNECTION_DB_USERNAME, database.getDatabaseMeta().getUsername() );
    assertEquals( CONNECTION_DB_PASSWORD, database.getDatabaseMeta().getPassword() );
  }

  @Test
  public void testDBNameNOTOverridden_IfDbNameOverrideEmpty() throws Exception {
    // Db Name Override is empty
    PGBulkLoaderMeta pgBulkLoaderMock = getPgBulkLoaderMock( DB_NAME_EMPTY );
    Database database = pgBulkLoader.getDatabase( pgBulkLoader, pgBulkLoaderMock );
    assertNotNull( database );
    // Verify DB name is NOT overridden
    assertEquals( CONNECTION_DB_NAME, database.getDatabaseMeta().getDatabaseName() );
    // Check additionally other connection information
    assertEquals( CONNECTION_NAME, database.getDatabaseMeta().getName() );
    assertEquals( CONNECTION_DB_HOST, database.getDatabaseMeta().getHostname() );
    assertEquals( CONNECTION_DB_PORT, database.getDatabaseMeta().getPort() );
    assertEquals( CONNECTION_DB_USERNAME, database.getDatabaseMeta().getUsername() );
    assertEquals( CONNECTION_DB_PASSWORD, database.getDatabaseMeta().getPassword() );
  }

  @Test
  public void testDBNameNOTOverridden_IfDbNameOverrideNull() throws Exception {
    // Db Name Override is null
    PGBulkLoaderMeta pgBulkLoaderMock = getPgBulkLoaderMock( null );
    Database database = pgBulkLoader.getDatabase( pgBulkLoader, pgBulkLoaderMock );
    assertNotNull( database );
    // Verify DB name is NOT overridden
    assertEquals( CONNECTION_DB_NAME, database.getDatabaseMeta().getDatabaseName() );
    // Check additionally other connection information
    assertEquals( CONNECTION_NAME, database.getDatabaseMeta().getName() );
    assertEquals( CONNECTION_DB_HOST, database.getDatabaseMeta().getHostname() );
    assertEquals( CONNECTION_DB_PORT, database.getDatabaseMeta().getPort() );
    assertEquals( CONNECTION_DB_USERNAME, database.getDatabaseMeta().getUsername() );
    assertEquals( CONNECTION_DB_PASSWORD, database.getDatabaseMeta().getPassword() );
  }

  @Test
  public void testProcessRow_StreamIsNull() throws Exception {
    PGBulkLoader pgBulkLoaderStreamIsNull = mock( PGBulkLoader.class );
    doReturn( null ).when( pgBulkLoaderStreamIsNull ).getRow();
    PGBulkLoaderMeta meta = mock( PGBulkLoaderMeta.class );
    PGBulkLoaderData data = mock( PGBulkLoaderData.class );
    assertEquals( false, pgBulkLoaderStreamIsNull.init() );
  }

  /**
   * [PDI-17481] Testing the ability that if no connection is specified, we will mark it as a fail and log the
   * appropriate reason to the user by throwing a HopException.
   */
  @Test
  public void testNoDatabaseConnection() {
    try {
      doReturn( null ).when( transformMockHelper.iTransformMeta ).getDatabaseMeta();
      assertFalse( pgBulkLoader.init() );
      // Verify that the database connection being set to null throws a HopException with the following message.
      pgBulkLoader.verifyDatabaseConnection();
      // If the method does not throw a Hop Exception, then the DB was set and not null for this test. Fail it.
      fail( "Database Connection is not null, this fails the test." );
    } catch ( HopException aHopException ) {
      assertTrue( aHopException.getMessage().contains( "There is no connection defined in this transform." ) );
    }
  }

  private static PGBulkLoaderMeta getPgBulkLoaderMock( String DbNameOverride ) throws HopXmlException {
    PGBulkLoaderMeta pgBulkLoaderMetaMock = mock( PGBulkLoaderMeta.class );
    when( pgBulkLoaderMetaMock.getDbNameOverride() ).thenReturn( DbNameOverride );
    DatabaseMeta databaseMeta = getDatabaseMetaSpy();
    when( pgBulkLoaderMetaMock.getDatabaseMeta() ).thenReturn( databaseMeta );
    return pgBulkLoaderMetaMock;
  }

  private static DatabaseMeta getDatabaseMetaSpy() throws HopXmlException {
    DatabaseMeta databaseMeta = spy( new DatabaseMeta(
      CONNECTION_NAME, "POSTGRESQL", "Native",
      CONNECTION_DB_HOST, CONNECTION_DB_NAME, CONNECTION_DB_PORT,
      CONNECTION_DB_USERNAME, CONNECTION_DB_PASSWORD ) );
    return databaseMeta;
  }
}

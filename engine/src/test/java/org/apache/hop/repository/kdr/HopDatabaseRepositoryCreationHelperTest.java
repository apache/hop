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
package org.apache.hop.repository.kdr;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.OracleDatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.repository.kdr.delegates.HopDatabaseRepositoryConnectionDelegate;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class HopDatabaseRepositoryCreationHelperTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static final int EXPECTED_ORACLE_DB_REPO_STRING = 1999;
  private static final int EXPECTED_DEFAULT_DB_REPO_STRING = HopDatabaseRepository.REP_ORACLE_STRING_LENGTH;
  private HopDatabaseRepositoryMeta repositoryMeta;
  private HopDatabaseRepository repository;

  LogChannelInterface log = LogChannel.GENERAL;
  HopDatabaseRepositoryCreationHelper helper;
  static String INDEX = "INDEX ";

  private AnswerSecondArgument lan = new AnswerSecondArgument();

  @Before
  public void setUp() throws Exception {
    HopLogStore.init();
    HopDatabaseRepositoryConnectionDelegate delegate = mock( HopDatabaseRepositoryConnectionDelegate.class );
    repository = mock( HopDatabaseRepository.class );
    repository.connectionDelegate = delegate;
    helper = new HopDatabaseRepositoryCreationHelper( repository );

    when( repository.getLog() ).thenReturn( log );
  }

  /**
    * PDI-10237 test index name length.
    *
    * @throws HopException
    */
  @Test
  public void testCreateIndexLenghts() throws HopException {
    DatabaseMeta meta = mock( DatabaseMeta.class );
    when( meta.getStartQuote() ).thenReturn( "" );
    when( meta.getEndQuote() ).thenReturn( "" );
    when( meta.getQuotedSchemaTableCombination( anyString(), anyString() ) ).thenAnswer(
        new Answer<String>() {
          @Override
          public String answer( InvocationOnMock invocation ) throws Throwable {
            return invocation.getArguments()[1].toString();
          }
        } );
    when( meta.getDatabaseInterface() ).thenReturn( new OracleDatabaseMeta() );

    Database db = mock( Database.class );

    when( db.getDatabaseMeta() ).thenReturn( meta );
    // always return some create sql.
    when( db.getDDL( anyString(), any( RowMetaInterface.class ), anyString(), anyBoolean(), anyString(), anyBoolean() ) ).thenReturn( "### CREATE TABLE;" );
    when( repository.getDatabase() ).thenReturn( db );
    when( repository.getDatabaseMeta() ).thenReturn( meta );

    when( db.getCreateIndexStatement( anyString(), anyString(), any( String[].class ), anyBoolean(), anyBoolean(), anyBoolean(), anyBoolean() ) ).thenAnswer( lan );

    HopDatabaseRepositoryCreationHelper helper = new HopDatabaseRepositoryCreationHelper( repository );

    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( false );
    String passwordEncoderPluginID = Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );
    Encr.init( passwordEncoderPluginID );

    List<String> statements = new ArrayList<String>();
    helper.createRepositorySchema( null, false, statements, true );

    for ( String st : statements ) {
      if ( st == null || st.startsWith( "#" ) ) {
        continue;
      }
      assertTrue( "Index name is not overlenght!: " + st, st.length() <= 30 );
    }
  }

  @Test
  public void testOracleDBRepoStringLength() throws Exception {

    HopEnvironment.init();
    DatabaseMeta databaseMeta = new DatabaseMeta( "OraRepo", "ORACLE", "JDBC", null, "test", null, null, null );
    repositoryMeta =
        new HopDatabaseRepositoryMeta( "HopDatabaseRepository", "OraRepo", "Ora Repository", databaseMeta );
    repository = new HopDatabaseRepository();
    repository.init( repositoryMeta );
    HopDatabaseRepositoryCreationHelper helper = new HopDatabaseRepositoryCreationHelper( repository );
    int repoStringLength = helper.getRepoStringLength();
    assertEquals( EXPECTED_ORACLE_DB_REPO_STRING, repoStringLength );
  }

  @Test
  public void testDefaultDBRepoStringLength() throws Exception {

    HopEnvironment.init();
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setDatabaseInterface( new TestDatabaseMeta() );
    repositoryMeta =
        new HopDatabaseRepositoryMeta( "HopDatabaseRepository", "TestRepo", "Test Repository", databaseMeta );
    repository = new HopDatabaseRepository();
    repository.init( repositoryMeta );
    HopDatabaseRepositoryCreationHelper helper = new HopDatabaseRepositoryCreationHelper( repository );
    int repoStringLength = helper.getRepoStringLength();
    assertEquals( EXPECTED_DEFAULT_DB_REPO_STRING, repoStringLength );
  }

  class TestDatabaseMeta extends OracleDatabaseMeta {

    @Override
    public int getMaxVARCHARLength() {
      return 1;
    }
  }

  static class AnswerSecondArgument implements Answer<String> {
    @Override
    public String answer( InvocationOnMock invocation ) throws Throwable {
      if ( invocation.getArguments().length < 2 ) {
        throw new RuntimeException( "no cookies!" );
      }
      return String.valueOf( invocation.getArguments()[1] );
    }
  }

}

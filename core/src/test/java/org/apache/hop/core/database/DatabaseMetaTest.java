/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatabaseMetaTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();
  private static final String TABLE_NAME = "tableName";
  private static final String DROP_STATEMENT = "dropStatement";
  private static final String DROP_STATEMENT_FALLBACK = "DROP TABLE IF EXISTS " + TABLE_NAME;

  private DatabaseMeta databaseMeta;
  private IDatabase iDatabase;

  @BeforeClass
  public static void setUpOnce() throws HopPluginException, HopException {
    // Register Natives to create a default DatabaseMeta
    DatabasePluginType.getInstance().searchPlugins();
    ValueMetaPluginType.getInstance().searchPlugins();
    HopClientEnvironment.init();
  }

  @Before
  public void setUp() {
    databaseMeta = new DatabaseMeta();
    iDatabase = mock( IDatabase.class );
    databaseMeta.setIDatabase( iDatabase );
  }

  @Test
  public void testGetDatabaseInterfacesMapWontReturnNullIfCalledSimultaneouslyWithClear()
    throws InterruptedException, ExecutionException {
    final AtomicBoolean done = new AtomicBoolean( false );
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.submit( () -> {
      while ( !done.get() ) {
        DatabaseMeta.clearDatabaseInterfacesMap();
      }
    } );
    Future<Exception> getFuture = executorService.submit( () -> {
      int i = 0;
      while ( !done.get() ) {
        assertNotNull( "Got null on try: " + i++, DatabaseMeta.getIDatabaseMap() );
        if ( i > 30000 ) {
          done.set( true );
        }
      }
      return null;
    } );
    getFuture.get();
  }

  @Test
  public void testApplyingDefaultOptions() throws Exception {
    HashMap<String, String> existingOptions = new HashMap<>();
    existingOptions.put( "type1.extra", "extraValue" );
    existingOptions.put( "type1.existing", "existingValue" );
    existingOptions.put( "type2.extra", "extraValue2" );

    HashMap<String, String> newOptions = new HashMap<>();
    newOptions.put( "type1.new", "newValue" );
    newOptions.put( "type1.existing", "existingDefault" );

    when( iDatabase.getExtraOptions() ).thenReturn( existingOptions );
    when( iDatabase.getDefaultOptions() ).thenReturn( newOptions );

    databaseMeta.applyDefaultOptions( iDatabase );
    verify( iDatabase ).addExtraOption( "type1", "new", "newValue" );
    verify( iDatabase, never() ).addExtraOption( "type1", "existing", "existingDefault" );
  }

  @Test
  public void testGetFeatureSummary() throws Exception {
    DatabaseMeta databaseMeta = mock( DatabaseMeta.class );
    GenericDatabaseMeta odbm = new GenericDatabaseMeta();
    doCallRealMethod().when( databaseMeta ).setIDatabase( any( IDatabase.class ) );
    doCallRealMethod().when( databaseMeta ).getFeatureSummary();
    doCallRealMethod().when( databaseMeta ).getAttributes();
    databaseMeta.setIDatabase( odbm );
    List<RowMetaAndData> result = databaseMeta.getFeatureSummary();
    assertNotNull( result );
    for ( RowMetaAndData rmd : result ) {
      assertEquals( 2, rmd.getRowMeta().size() );
      assertEquals( "Parameter", rmd.getRowMeta().getValueMeta( 0 ).getName() );
      assertEquals( IValueMeta.TYPE_STRING, rmd.getRowMeta().getValueMeta( 0 ).getType() );
      assertEquals( "Value", rmd.getRowMeta().getValueMeta( 1 ).getName() );
      assertEquals( IValueMeta.TYPE_STRING, rmd.getRowMeta().getValueMeta( 1 ).getType() );
    }
  }
  
  @Test
  public void testQuoteReservedWords() {
    DatabaseMeta databaseMeta = mock( DatabaseMeta.class );
    doCallRealMethod().when( databaseMeta ).quoteReservedWords( any( IRowMeta.class ) );
    doCallRealMethod().when( databaseMeta ).quoteField( anyString() );
    doCallRealMethod().when( databaseMeta ).setIDatabase( any( IDatabase.class ) );
    doReturn( "\"" ).when( databaseMeta ).getStartQuote();
    doReturn( "\"" ).when( databaseMeta ).getEndQuote();
    final IDatabase iDatabase = mock( IDatabase.class );
    doReturn( true ).when( iDatabase ).isQuoteAllFields();
    databaseMeta.setIDatabase( iDatabase );

    final RowMeta fields = new RowMeta();
    for ( int i = 0; i < 10; i++ ) {
      final IValueMeta valueMeta = new ValueMetaNone( "test_" + i );
      fields.addValueMeta( valueMeta );
    }

    for ( int i = 0; i < 10; i++ ) {
      databaseMeta.quoteReservedWords( fields );
    }

    for ( int i = 0; i < 10; i++ ) {
      databaseMeta.quoteReservedWords( fields );
      final String name = fields.getValueMeta( i ).getName();
      // check valueMeta index in list
      assertTrue( name.contains( "test_" + i ) );
      // check valueMeta is found by quoted name
      assertNotNull( fields.searchValueMeta( name ) );
    }
  }

  @Test
  public void testModifyingName() throws Exception {
    DatabaseMeta databaseMeta = mock( DatabaseMeta.class );
    GenericDatabaseMeta odbm = new GenericDatabaseMeta();
    doCallRealMethod().when( databaseMeta ).setIDatabase( any( IDatabase.class ) );
    doCallRealMethod().when( databaseMeta ).setName( anyString() );
    doCallRealMethod().when( databaseMeta ).getName();
    databaseMeta.setIDatabase( odbm );
    databaseMeta.setName( "test" );

    List<DatabaseMeta> list = new ArrayList<DatabaseMeta>();
    list.add( databaseMeta );

    DatabaseMeta databaseMeta2 = mock( DatabaseMeta.class );
    GenericDatabaseMeta odbm2 = new GenericDatabaseMeta();
    doCallRealMethod().when( databaseMeta2 ).setIDatabase( any( IDatabase.class ) );
    doCallRealMethod().when( databaseMeta2 ).setName( anyString() );
    doCallRealMethod().when( databaseMeta2 ).getName();
    doCallRealMethod().when( databaseMeta2 ).verifyAndModifyDatabaseName( any( ArrayList.class ), anyString() );
    databaseMeta2.setIDatabase( odbm2 );
    databaseMeta2.setName( "test" );

    databaseMeta2.verifyAndModifyDatabaseName( list, null );

    assertTrue( !databaseMeta.getName().equals( databaseMeta2.getName() ) );
  }



  @Test
  public void indexOfName_NullArray() {
    assertEquals( -1, DatabaseMeta.indexOfName( null, "" ) );
  }

  @Test
  public void indexOfName_NullName() {
    assertEquals( -1, DatabaseMeta.indexOfName( new String[] { "1" }, null ) );
  }

  @Test
  public void indexOfName_ExactMatch() {
    assertEquals( 1, DatabaseMeta.indexOfName( new String[] { "a", "b", "c" }, "b" ) );
  }

  @Test
  public void indexOfName_NonExactMatch() {
    assertEquals( 1, DatabaseMeta.indexOfName( new String[] { "a", "b", "c" }, "B" ) );
  }

  /**
   * Given that the {@link IDatabase} object is of a new extended type.
   * <br/>
   * When {@link DatabaseMeta#getDropTableIfExistsStatement(String)} is called,
   * then the underlying new method of {@link IDatabase} should be used.
   */
  @Test
  public void shouldCallNewMethodWhenDatabaseInterfaceIsOfANewType() {
    IDatabase databaseInterfaceNew = mock( IDatabase.class );
    databaseMeta.setIDatabase( databaseInterfaceNew );
    when( databaseInterfaceNew.getDropTableIfExistsStatement( TABLE_NAME ) ).thenReturn( DROP_STATEMENT );

    String statement = databaseMeta.getDropTableIfExistsStatement( TABLE_NAME );

    assertEquals( DROP_STATEMENT, statement );
  }

  @Test
  public void testCheckParameters() {
    DatabaseMeta meta = mock( DatabaseMeta.class );
    BaseDatabaseMeta iDatabase = mock( BaseDatabaseMeta.class );
    when( iDatabase.requiresName() ).thenReturn( true );
    when( meta.getIDatabase() ).thenReturn( iDatabase );
    when( meta.getName() ).thenReturn( null );
    when( meta.checkParameters() ).thenCallRealMethod();
    assertEquals( 2, meta.checkParameters().length );
  }
}

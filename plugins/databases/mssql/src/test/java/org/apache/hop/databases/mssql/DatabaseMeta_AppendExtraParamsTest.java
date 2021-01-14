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

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.Variables;

import static org.junit.Assert.assertEquals;

/**
 * This test is designed to check that jdbc url (with no extra parameters)
 * remains valid (sure, we check syntax only) after adding any extra parameters
 * in spite of number of this parameters and their validity.
 *
 * @author Ivan_Nikolaichuk
 */
public class DatabaseMeta_AppendExtraParamsTest {
  private static final String CONN_TYPE_MSSQL = "MSSQL";

  private static final String STRING_EXTRA_OPTION = "extraOption";
  private static final String STRING_OPTION_VALUE = "value";
  private static final String STRING_DEFAULT = "<def>";

  private DatabaseMeta meta;
  private IDatabase mssqlServerDatabaseMeta;
  private Variables variables;

  private final String CONN_URL_NO_EXTRA_OPTIONS = "jdbc:sqlserver://127.0.0.1:1433";

/*  @Before
  public void setUp() throws HopDatabaseException {
    meta = mock( DatabaseMeta.class );
    mssqlServerDatabaseMeta = new MSSQLServerDatabaseMeta();
    variables = new Variables();
    mssqlServerDatabaseMeta.setPluginId( CONN_TYPE_MSSQL );
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        return variables.environmentSubstitute( (String) invocation.getArguments()[ 0 ] );
      }
    } ).when( meta ).environmentSubstitute( anyString() );
    doReturn( mssqlServerDatabaseMeta ).when( meta ).getIDatabase();

    doCallRealMethod().when( meta ).appendExtraOptions( anyString(), anyMapOf( String.class, String.class ) );
    doCallRealMethod().when( meta )
      .databaseForBothDbInterfacesIsTheSame( any( IDatabase.class ), any( IDatabase.class ) );
    doCallRealMethod().when( meta ).getExtraOptionIndicator();
    doCallRealMethod().when( meta ).getExtraOptionSeparator();
    doCallRealMethod().when( meta ).getExtraOptionValueSeparator();
    doReturn( mock( ILogChannel.class ) ).when( meta ).getGeneralLogger();
    doReturn( mssqlServerDatabaseMeta ).when( meta ).getDbInterface( CONN_TYPE_MSSQL );

  }*/

/*  @Test
  public void urlNotChanges_WhenNoExtraOptionsGiven() {
    Map<String, String> extraOptions = generateExtraOptions( CONN_TYPE_MSSQL, 0 );

    String connUrlWithExtraOptions = meta.appendExtraOptions( CONN_URL_NO_EXTRA_OPTIONS, extraOptions );
    assertEquals( CONN_URL_NO_EXTRA_OPTIONS, connUrlWithExtraOptions );
  }*/

  /**
   * Extra option key is expected to be in pattern: ConnType.key If ConnType and key are not divided by point, extra
   * option is considered to be invalid
   */
/*
  @Test
  public void urlNotChanges_WhenExtraOptionIsInvalid() {
    Map<String, String> extraOptions = generateExtraOptions( CONN_TYPE_MSSQL, 0 );
    extraOptions.put( STRING_DEFAULT, STRING_DEFAULT );

    String connUrlWithExtraOptions = meta.appendExtraOptions( CONN_URL_NO_EXTRA_OPTIONS, extraOptions );
    assertEquals( CONN_URL_NO_EXTRA_OPTIONS, connUrlWithExtraOptions );
  }
*/

/*
  @Test
  public void extraOptionsAreNotAppended_WhenTheyAreEmpty() {
    Map<String, String> extraOptions = generateExtraOptions( CONN_TYPE_MSSQL, 0 );
    final String validKey = CONN_TYPE_MSSQL + "." + "key";
    extraOptions.put( validKey, StringUtil.EMPTY_STRING );
    extraOptions.put( validKey, DatabaseMeta.EMPTY_OPTIONS_STRING );

    String connUrlWithExtraOptions = meta.appendExtraOptions( CONN_URL_NO_EXTRA_OPTIONS, extraOptions );
    assertEquals( CONN_URL_NO_EXTRA_OPTIONS, connUrlWithExtraOptions );
  }
*/

/*  @Test
  public void extraOptionsAreNotAppended_WhenConnTypePointsToAnotherDataBase() throws Exception {
    Map<String, String> extraOptions = generateExtraOptions( STRING_DEFAULT, 2 );

    // emulate that there is no database with STRING_DEFAULT plugin id.
    doThrow( new HopDatabaseException() ).when( meta ).getDbInterface( STRING_DEFAULT );
    String connUrlWithExtraOptions = meta.appendExtraOptions( CONN_URL_NO_EXTRA_OPTIONS, extraOptions );
    assertEquals( CONN_URL_NO_EXTRA_OPTIONS, connUrlWithExtraOptions );
  }*/


/*  @Test
  public void urlIsValid_AfterAddingValidExtraOptions() {
    Map<String, String> extraOptions = generateExtraOptions( CONN_TYPE_MSSQL, 1 );

    String expectedExtraOptionsUrl =
      STRING_EXTRA_OPTION + 0 + mssqlServerDatabaseMeta.getExtraOptionValueSeparator() + STRING_OPTION_VALUE + 0;
    String expectedUrl =
      CONN_URL_NO_EXTRA_OPTIONS + mssqlServerDatabaseMeta.getExtraOptionSeparator() + expectedExtraOptionsUrl;

    String connUrlWithExtraOptions = meta.appendExtraOptions( CONN_URL_NO_EXTRA_OPTIONS, extraOptions );
    assertEquals( expectedUrl, connUrlWithExtraOptions );
  }*/


/*  @Test
  public void onlyValidExtraOptions_AreAppendedToUrl() {
    Map<String, String> extraOptions = generateExtraOptions( CONN_TYPE_MSSQL, 1 );
    extraOptions.put( STRING_DEFAULT, STRING_DEFAULT );
    extraOptions.put( CONN_TYPE_MSSQL + "." + "key1", StringUtil.EMPTY_STRING );
    extraOptions.put( CONN_TYPE_MSSQL + "." + "key2", DatabaseMeta.EMPTY_OPTIONS_STRING );

    String expectedExtraOptionsUrl =
      STRING_EXTRA_OPTION + 0 + mssqlServerDatabaseMeta.getExtraOptionValueSeparator() + STRING_OPTION_VALUE + 0;
    String expectedUrl =
      CONN_URL_NO_EXTRA_OPTIONS + mssqlServerDatabaseMeta.getExtraOptionSeparator() + expectedExtraOptionsUrl;


    String connUrlWithExtraOptions = meta.appendExtraOptions( CONN_URL_NO_EXTRA_OPTIONS, extraOptions );
    assertEquals( expectedUrl, connUrlWithExtraOptions );
  }*/

  /**
   * Extra option is considered to be valid if it is build in pattern: ConnType.key
   * <b>All generated extra options generated by this method are valid.</b>
   */
/*  public Map<String, String> generateExtraOptions( final String connType, int numberOfOptions ) {
    Map<String, String> map = new HashMap<>( numberOfOptions );
    for ( int i = 0; i < numberOfOptions; i++ ) {
      String uniqueExtraOption = STRING_EXTRA_OPTION + i;
      String optionVal = STRING_OPTION_VALUE + i;
      map.put( connType + "." + uniqueExtraOption, optionVal );
    }

    return map;
  }*/
}

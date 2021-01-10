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
package org.apache.hop.databases.mariadb;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.*;

public class MariaDBDatabaseMetaTest {
  /**
   * @return
   * @throws Exception
   */
  private ResultSetMetaData getResultSetMetaData() throws Exception {
    ResultSetMetaData resultSetMetaData = mock( ResultSetMetaData.class );

    /**
     * Fields setup around the following query:
     *
     * select
     *   CUSTOMERNUMBER as NUMBER
     * , CUSTOMERNAME as NAME
     * , CONTACTLASTNAME as LAST_NAME
     * , CONTACTFIRSTNAME as FIRST_NAME
     * , 'MariaDB' as DB
     * , 'NoAliasText'
     * from CUSTOMERS
     * ORDER BY CUSTOMERNAME;
     */

    doReturn( "NUMBER" ).when( resultSetMetaData ).getColumnLabel( 1 );
    doReturn( "NAME" ).when( resultSetMetaData ).getColumnLabel( 2 );
    doReturn( "LAST_NAME" ).when( resultSetMetaData ).getColumnLabel( 3 );
    doReturn( "FIRST_NAME" ).when( resultSetMetaData ).getColumnLabel( 4 );
    doReturn( "DB" ).when( resultSetMetaData ).getColumnLabel( 5 );
    doReturn( "NoAliasText" ).when( resultSetMetaData ).getColumnLabel( 6 );

    return resultSetMetaData;
  }

  /**
   * @return
   * @throws Exception
   */
  private ResultSetMetaData getResultSetMetaDataException() throws Exception {
    ResultSetMetaData resultSetMetaData = mock( ResultSetMetaData.class );

    doThrow( new SQLException() ).when( resultSetMetaData ).getColumnLabel( 1 );

    return resultSetMetaData;
  }

  @Test
  public void testGetLegacyColumnNameFieldNumber() throws Exception {
    assertEquals( "NUMBER", new MariaDBDatabaseMeta().getLegacyColumnName( mock( DatabaseMetaData.class ), getResultSetMetaData(), 1 ) );
  }

  @Test
  public void testGetLegacyColumnNameFieldName() throws Exception {
    assertEquals( "NAME", new MariaDBDatabaseMeta().getLegacyColumnName( mock( DatabaseMetaData.class ), getResultSetMetaData(), 2 ) );
  }

  @Test
  public void testGetLegacyColumnNameFieldLastName() throws Exception {
    assertEquals( "LAST_NAME", new MariaDBDatabaseMeta().getLegacyColumnName( mock( DatabaseMetaData.class ), getResultSetMetaData(), 3 ) );
  }

  @Test
  public void testGetLegacyColumnNameFieldFirstName() throws Exception {
    assertEquals( "FIRST_NAME", new MariaDBDatabaseMeta().getLegacyColumnName( mock( DatabaseMetaData.class ), getResultSetMetaData(), 4 ) );
  }

  @Test
  public void testGetLegacyColumnNameFieldDB() throws Exception {
    assertEquals( "DB", new MariaDBDatabaseMeta().getLegacyColumnName( mock( DatabaseMetaData.class ), getResultSetMetaData(), 5 ) );
  }

  @Test
  public void testGetLegacyColumnNameNoAliasText() throws Exception {
    assertEquals( "NoAliasText", new MariaDBDatabaseMeta().getLegacyColumnName( mock( DatabaseMetaData.class ), getResultSetMetaData(), 6 ) );
  }

  @Test( expected = HopDatabaseException.class )
  public void testGetLegacyColumnNameNullDBMetaDataException() throws Exception {
    new MariaDBDatabaseMeta().getLegacyColumnName( null, getResultSetMetaData(), 1 );
  }

  @Test( expected = HopDatabaseException.class )
  public void testGetLegacyColumnNameNullRSMetaDataException() throws Exception {
    new MariaDBDatabaseMeta().getLegacyColumnName( mock( DatabaseMetaData.class ), null, 1 );
  }

  @Test( expected = HopDatabaseException.class )
  public void testGetLegacyColumnNameDatabaseException() throws Exception {
    new MariaDBDatabaseMeta().getLegacyColumnName( mock( DatabaseMetaData.class ), getResultSetMetaDataException(), 1 );
  }

  @Test
  public void testMysqlOverrides() {
    MariaDBDatabaseMeta nativeMeta = new MariaDBDatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );

    assertEquals( 3306, nativeMeta.getDefaultDatabasePort() );

    assertEquals( "org.mariadb.jdbc.Driver", nativeMeta.getDriverClass() );
    assertEquals( "jdbc:mariadb://FOO:BAR/WIBBLE", nativeMeta.getURL( "FOO", "BAR", "WIBBLE" ) );
    assertEquals( "jdbc:mariadb://FOO/WIBBLE", nativeMeta.getURL( "FOO", "", "WIBBLE" ) );

    // The fullExceptionLog method is covered by another test case.
  }

  @Ignore
  @Test
  public void testAddOptionsMariaDB() {
    DatabaseMeta databaseMeta = new DatabaseMeta( "", "MariaDB", "JDBC", null, "stub:stub", null, null, null );
    Map<String, String> options = databaseMeta.getExtraOptions();
    if ( !options.keySet().contains( "MARIADB.defaultFetchSize" ) ) {
      fail();
    }
  }
}

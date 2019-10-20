/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.repository.kdr.delegates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.apache.hop.core.ProgressMonitorListener;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.repository.LongObjectId;
import org.apache.hop.repository.kdr.HopDatabaseRepository;

/**
 */
public class HopDatabaseRepositoryConnectionDelegateUnitTest {
  private DatabaseMeta databaseMeta;
  private HopDatabaseRepository repository;
  private Database database;
  private HopDatabaseRepositoryConnectionDelegate kettleDatabaseRepositoryConnectionDelegate;

  @Before
  public void setup() {
    repository = mock( HopDatabaseRepository.class );
    databaseMeta = mock( DatabaseMeta.class );
    database = mock( Database.class );
    kettleDatabaseRepositoryConnectionDelegate =
      new HopDatabaseRepositoryConnectionDelegate( repository, databaseMeta );
    kettleDatabaseRepositoryConnectionDelegate.database = database;
  }

  @Test
  public void createIdsWithsValueQuery() {
    final String table = "table";
    final String id = "id";
    final String lookup = "lookup";
    final String expectedTemplate = String.format( "select %s from %s where %s in ", id, table, lookup ) + "(%s)";

    assertTrue( String.format( expectedTemplate, "?" ).equalsIgnoreCase(
        HopDatabaseRepositoryConnectionDelegate.createIdsWithValuesQuery( table, id, lookup, 1 ) ) );
    assertTrue(
        String.format( expectedTemplate, "?,?" ).equalsIgnoreCase(
          HopDatabaseRepositoryConnectionDelegate.createIdsWithValuesQuery( table, id, lookup, 2 ) ) );
  }

  @Test
  public void testGetValueToIdMap() throws HopException {
    String tablename = "test-tablename";
    String idfield = "test-idfield";
    String lookupfield = "test-lookupfield";
    List<Object[]> rows = new ArrayList<Object[]>();
    int id = 1234;
    LongObjectId longObjectId = new LongObjectId( id );
    rows.add( new Object[] { lookupfield, id } );
    when( database.getRows( eq( "SELECT " + lookupfield + ", " + idfield + " FROM " + tablename ), any(
        RowMetaInterface.class ),
      eq( new Object[] {} ), eq( ResultSet.FETCH_FORWARD ),
      eq( false ), eq( -1 ), eq( (ProgressMonitorListener) null ) ) ).thenReturn( rows );
    Map<String, LongObjectId> valueToIdMap =
      kettleDatabaseRepositoryConnectionDelegate.getValueToIdMap( tablename, idfield, lookupfield );
    assertEquals( 1, valueToIdMap.size() );
    assertEquals( longObjectId, valueToIdMap.get( lookupfield ) );
  }
}

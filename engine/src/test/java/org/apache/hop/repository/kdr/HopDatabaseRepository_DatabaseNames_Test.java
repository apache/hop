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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.StringObjectId;
import org.apache.hop.repository.kdr.delegates.HopDatabaseRepositoryDatabaseDelegate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * @author Andrey Khayrutdinov
 */
public class HopDatabaseRepository_DatabaseNames_Test {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  private HopDatabaseRepository repository;
  private HopDatabaseRepositoryDatabaseDelegate databaseDelegate;

  @Before
  public void setUp() throws Exception {
    repository = spy( new HopDatabaseRepository() );
    databaseDelegate = spy( new HopDatabaseRepositoryDatabaseDelegate( repository ) );
    repository.databaseDelegate = databaseDelegate;
  }


  @Test
  public void getDatabaseId_ExactMatch() throws Exception {
    final String name = UUID.randomUUID().toString();
    final ObjectId expectedId = new StringObjectId( "expected" );
    doReturn( expectedId ).when( databaseDelegate ).getDatabaseID( name );

    ObjectId id = repository.getDatabaseID( name );
    assertEquals( expectedId, id );
  }

  @Test
  public void getDatabaseId_InsensitiveMatch() throws Exception {
    final String name = "databaseWithCamelCase";
    final String lookupName = name.toLowerCase();
    assertNotSame( lookupName, name );

    final ObjectId expected = new StringObjectId( "expected" );
    doReturn( expected ).when( databaseDelegate ).getDatabaseID( name );
    doReturn( null ).when( databaseDelegate ).getDatabaseID( lookupName );

    DatabaseMeta db = new DatabaseMeta();
    db.setName( name );
    db.setObjectId( expected );
    List<DatabaseMeta> dbs = Collections.singletonList( db );
    doReturn( dbs ).when( repository ).getDatabases();

    ObjectId id = repository.getDatabaseID( lookupName );
    assertEquals( expected, id );
  }

  @Test
  public void getDatabaseId_ReturnsExactMatch_PriorToCaseInsensitiveMatch() throws Exception {
    final String exact = "databaseExactMatch";
    final String similar = exact.toLowerCase();
    assertNotSame( similar, exact );

    final ObjectId exactId = new StringObjectId( "exactId" );
    doReturn( exactId ).when( databaseDelegate ).getDatabaseID( exact );
    final ObjectId similarId = new StringObjectId( "similarId" );
    doReturn( similarId ).when( databaseDelegate ).getDatabaseID( similar );

    DatabaseMeta db = new DatabaseMeta();
    db.setName( exact );
    DatabaseMeta another = new DatabaseMeta();
    db.setName( similar );
    List<DatabaseMeta> dbs = Arrays.asList( another, db );
    doReturn( dbs ).when( repository ).getDatabases();

    ObjectId id = this.repository.getDatabaseID( exact );
    assertEquals( exactId, id );
  }

}

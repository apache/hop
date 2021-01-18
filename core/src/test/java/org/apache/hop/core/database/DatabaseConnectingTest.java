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

package org.apache.hop.core.database;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * @author Andrey Khayrutdinov
 */
public class DatabaseConnectingTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  private static final String GROUP = "group";
  private static final String ANOTHER_GROUP = "another-group";

  @BeforeClass
  public static void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @After
  public void removeFromSharedConnectionMap() {
    removeFromSharedConnectionMap( GROUP );
  }

  private void removeFromSharedConnectionMap( String group ) {
    DatabaseConnectionMap.getInstance().removeConnection( group, null, createStubDatabase( null ) );
  }


  @Test
  public void connect_GroupIsNull() throws Exception {
    Connection connection1 = mock( Connection.class );
    DatabaseStub db1 = createStubDatabase( connection1 );

    Connection connection2 = mock( Connection.class );
    DatabaseStub db2 = createStubDatabase( connection2 );

    db1.connect();
    db2.connect();

    assertEquals( connection1, db1.getConnection() );
    assertEquals( connection2, db2.getConnection() );
  }

  @Test
  public void connect_GroupIsEqual_Consequently() throws Exception {
    Connection shared = mock( Connection.class );

    DatabaseStub db1 = createStubDatabase( shared );
    db1.connect( GROUP, null );

    DatabaseStub db2 = createStubDatabase( shared );
    db2.connect( GROUP, null );

    assertSharedAmongTheGroup( shared, db1, db2 );
  }

  @Test
  public void connect_GroupIsEqual_InParallel() throws Exception {
    final Connection shared = mock( Connection.class );
    final int dbsAmount = 300;
    final int threadsAmount = 50;

    List<DatabaseStub> dbs = new ArrayList<>( dbsAmount );
    Set<Integer> copies = new HashSet<>( dbsAmount );
    ExecutorService pool = Executors.newFixedThreadPool( threadsAmount );
    try {
      CompletionService<DatabaseStub> service = new ExecutorCompletionService<>( pool );
      for ( int i = 0; i < dbsAmount; i++ ) {
        service.submit( createStubDatabase( shared ) );
        copies.add( i + 1 );
      }

      for ( int i = 0; i < dbsAmount; i++ ) {
        DatabaseStub db = service.take().get();
        assertEquals( shared, db.getConnection() );
        dbs.add( db );
      }
    } finally {
      pool.shutdown();
    }

    for ( DatabaseStub db : dbs ) {
      String message =
        String.format( "There should be %d shares of the connection, but found %d", dbsAmount, db.getOpened() );
      // 0 is for those instances that use the shared connection
      assertTrue( message, db.getOpened() == 0 || db.getOpened() == dbsAmount );

      assertTrue( "Each instance should have a unique 'copy' value", copies.remove( db.getCopy() ) );
    }
    assertTrue( copies.isEmpty() );
  }

  @Test
  public void connect_TwoGroups() throws Exception {
    try {
      Connection shared1 = mock( Connection.class );
      Connection shared2 = mock( Connection.class );

      DatabaseStub db11 = createStubDatabase( shared1 );
      DatabaseStub db12 = createStubDatabase( shared1 );

      DatabaseStub db21 = createStubDatabase( shared2 );
      DatabaseStub db22 = createStubDatabase( shared2 );

      db11.connect( GROUP, null );
      db12.connect( GROUP, null );

      db21.connect( ANOTHER_GROUP, null );
      db22.connect( ANOTHER_GROUP, null );

      assertSharedAmongTheGroup( shared1, db11, db12 );
      assertSharedAmongTheGroup( shared2, db21, db22 );
    } finally {
      removeFromSharedConnectionMap( ANOTHER_GROUP );
    }
  }

  private void assertSharedAmongTheGroup( Connection shared, DatabaseStub db1, DatabaseStub db2 ) {
    assertEquals( shared, db1.getConnection() );
    assertEquals( shared, db2.getConnection() );
    assertEquals( 2, db1.getOpened() );
    assertEquals( 0, db2.getOpened() );
  }

  @Test
  public void connect_ManyGroups_Simultaneously() throws Exception {
    final int groupsAmount = 30;

    Map<String, Connection> groups = new HashMap<>( groupsAmount );
    for ( int i = 0; i < groupsAmount; i++ ) {
      groups.put( Integer.toString( i ), mock( Connection.class ) );
    }

    try {
      ExecutorService pool = Executors.newFixedThreadPool( groupsAmount );
      try {
        CompletionService<DatabaseStub> service = new ExecutorCompletionService<>( pool );
        Map<DatabaseStub, String> mapping = new HashMap<>( groupsAmount );
        for ( Map.Entry<String, Connection> entry : groups.entrySet() ) {
          DatabaseStub stub = createStubDatabase( entry.getValue() );
          mapping.put( stub, entry.getKey() );
          service.submit( stub );
        }

        Set<String> unmatchedGroups = new HashSet<>( groups.keySet() );
        for ( int i = 0; i < groupsAmount; i++ ) {
          DatabaseStub stub = service.take().get();
          assertTrue( unmatchedGroups.remove( mapping.get( stub ) ) );
        }
        assertTrue( unmatchedGroups.isEmpty() );
      } finally {
        pool.shutdown();
      }
    } finally {
      for ( String group : groups.keySet() ) {
        removeFromSharedConnectionMap( group );
      }
    }
  }


  private DatabaseStub createStubDatabase( Connection sharedConnection ) {
    DatabaseMeta meta = new DatabaseMeta( "test", "None", "", "", "", "", "", "" );
    return new DatabaseStub(null, Variables.getADefaultVariableSpace(), meta, sharedConnection);
  }


  private static class DatabaseStub extends Database implements Callable<DatabaseStub> {

    private final Connection sharedConnection;
    private boolean connected;

    public DatabaseStub( ILoggingObject parentObject,
                         IVariables variables, DatabaseMeta databaseMeta, Connection sharedConnection ) {
      super( parentObject, variables, databaseMeta );
      this.sharedConnection = sharedConnection;
      this.connected = false;
    }

    @Override
    public synchronized void normalConnect( String partitionId ) throws HopDatabaseException {
      if ( !connected ) {
        // make a delay to emulate real scenario
        try {
          Thread.sleep( 250 );
        } catch ( InterruptedException e ) {
          fail( e.getMessage() );
        }

        setConnection( sharedConnection );
        connected = true;
      }
    }

    @Override
    public DatabaseStub call() throws Exception {
      connect( GROUP, null );
      return this;
    }
  }
}

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

package org.apache.hop.core.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.apache.hop.core.util.Utils;

public class SequenceMetaTest {

  @Test
  public void testSupport() {

    DatabaseInterface[] support = new DatabaseInterface[] {
      new OracleDatabaseMeta(),
    };

    DatabaseInterface[] doNotSupport = new DatabaseInterface[] {
      new GenericDatabaseMeta(),
      new MySQLDatabaseMeta()
    };

    for ( DatabaseInterface db : support ) {
      assertSupports( db, true );
    }

    for ( DatabaseInterface db : doNotSupport ) {
      assertSupports( db, false );
    }
  }

  public static void assertSupports( DatabaseInterface db, boolean expected ) {
    String dbType = db.getClass().getSimpleName();
    if ( expected ) {
      assertTrue( dbType, db.supportsSequences() );
      assertFalse( dbType + ": List of Sequences", Utils.isEmpty( db.getSQLListOfSequences() ) );
      assertFalse( dbType + ": Sequence Exists", Utils.isEmpty( db.getSQLSequenceExists( "testSeq" ) ) );
      assertFalse( dbType + ": Current Value", Utils.isEmpty( db.getSQLCurrentSequenceValue( "testSeq" ) ) );
      assertFalse( dbType + ": Next Value", Utils.isEmpty( db.getSQLNextSequenceValue( "testSeq" ) ) );
    } else {
      assertFalse( db.getClass().getSimpleName(), db.supportsSequences() );
      assertTrue( dbType + ": List of Sequences", Utils.isEmpty( db.getSQLListOfSequences() ) );
      assertTrue( dbType + ": Sequence Exists", Utils.isEmpty( db.getSQLSequenceExists( "testSeq" ) ) );
      assertTrue( dbType + ": Current Value", Utils.isEmpty( db.getSQLCurrentSequenceValue( "testSeq" ) ) );
      assertTrue( dbType + ": Next Value", Utils.isEmpty( db.getSQLNextSequenceValue( "testSeq" ) ) );
    }
  }

  @Test
  public void testSQL() {

    DatabaseInterface databaseInterface;
    final String sequenceName = "sequence_name";

    databaseInterface = new OracleDatabaseMeta();
    assertEquals( "SELECT sequence_name.nextval FROM dual", databaseInterface
      .getSQLNextSequenceValue( sequenceName ) );
    assertEquals( "SELECT sequence_name.currval FROM DUAL", databaseInterface
      .getSQLCurrentSequenceValue( sequenceName ) );

    // the rest of the database metas say they don't support sequences

    databaseInterface = new GenericDatabaseMeta();
    assertEquals( "", databaseInterface.getSQLNextSequenceValue( sequenceName ) );
    assertEquals( "", databaseInterface.getSQLCurrentSequenceValue( sequenceName ) );

  }
}

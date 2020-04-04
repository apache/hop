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

package org.apache.hop.databases.greenplum;

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.util.Utils;
import org.junit.Test;

import static org.junit.Assert.*;

public class SequenceMetaTest {

  @Test
  public void testSupport() {

    IDatabase[] support = new IDatabase[] {
      new GreenplumDatabaseMeta(),
    };

    for ( IDatabase db : support ) {
      assertSupports( db, true );
    }
  }

  public static void assertSupports( IDatabase db, boolean expected ) {
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

    IDatabase iDatabase;
    final String sequenceName = "sequence_name";

    iDatabase = new GreenplumDatabaseMeta();
    assertEquals( "SELECT nextval('sequence_name')", iDatabase.getSQLNextSequenceValue( sequenceName ) );
    assertEquals( "SELECT currval('sequence_name')", iDatabase
      .getSQLCurrentSequenceValue( sequenceName ) );
    assertEquals( "SELECT relname AS sequence_name FROM pg_catalog.pg_statio_all_sequences", iDatabase
      .getSQLListOfSequences() );
    assertEquals( "SELECT relname AS sequence_name FROM pg_catalog.pg_statio_all_sequences WHERE relname = 'sequence_name'",
      iDatabase.getSQLSequenceExists( sequenceName ) );

  }
}

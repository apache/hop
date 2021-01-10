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

package org.apache.hop.databases.postgresql;

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.util.Utils;
import org.junit.Test;

import static org.junit.Assert.*;

public class SequenceMetaTest {

  @Test
  public void testSupport() {

    IDatabase[] support = new IDatabase[] {
      new PostgreSqlDatabaseMeta(),
    };

    for ( IDatabase db : support ) {
      assertSupports( db, true );
    }
  }

  public static void assertSupports( IDatabase db, boolean expected ) {
    String dbType = db.getClass().getSimpleName();
    if ( expected ) {
      assertTrue( dbType, db.supportsSequences() );
      assertFalse( dbType + ": List of Sequences", Utils.isEmpty( db.getSqlListOfSequences() ) );
      assertFalse( dbType + ": Sequence Exists", Utils.isEmpty( db.getSqlSequenceExists( "testSeq" ) ) );
      assertFalse( dbType + ": Current Value", Utils.isEmpty( db.getSqlCurrentSequenceValue( "testSeq" ) ) );
      assertFalse( dbType + ": Next Value", Utils.isEmpty( db.getSqlNextSequenceValue( "testSeq" ) ) );
    } else {
      assertFalse( db.getClass().getSimpleName(), db.supportsSequences() );
      assertTrue( dbType + ": List of Sequences", Utils.isEmpty( db.getSqlListOfSequences() ) );
      assertTrue( dbType + ": Sequence Exists", Utils.isEmpty( db.getSqlSequenceExists( "testSeq" ) ) );
      assertTrue( dbType + ": Current Value", Utils.isEmpty( db.getSqlCurrentSequenceValue( "testSeq" ) ) );
      assertTrue( dbType + ": Next Value", Utils.isEmpty( db.getSqlNextSequenceValue( "testSeq" ) ) );
    }
  }

  @Test
  public void testSql() {

    IDatabase iDatabase;
    final String sequenceName = "sequence_name";

    iDatabase = new PostgreSqlDatabaseMeta();
    assertEquals( "SELECT nextval('sequence_name')", iDatabase.getSqlNextSequenceValue( sequenceName ) );
    assertEquals( "SELECT currval('sequence_name')", iDatabase
      .getSqlCurrentSequenceValue( sequenceName ) );
    assertEquals( "SELECT relname AS sequence_name FROM pg_catalog.pg_statio_all_sequences", iDatabase
      .getSqlListOfSequences() );
    assertEquals( "SELECT relname AS sequence_name FROM pg_catalog.pg_statio_all_sequences WHERE relname = 'sequence_name'",
      iDatabase.getSqlSequenceExists( sequenceName ) );
  }
}

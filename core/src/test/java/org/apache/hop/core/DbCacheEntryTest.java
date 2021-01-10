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
package org.apache.hop.core;

import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DbCacheEntryTest {
  @Test
  public void testClass() throws IOException, HopFileException {
    final String dbName = "dbName";
    final String sql = "sql query";
    DbCacheEntry entry = new DbCacheEntry( dbName, sql );
    assertTrue( entry.sameDB( "dbName" ) );
    assertFalse( entry.sameDB( "otherDb" ) );
    assertEquals( dbName.toLowerCase().hashCode() ^ sql.toLowerCase().hashCode(), entry.hashCode() );
    DbCacheEntry otherEntry = new DbCacheEntry();
    assertFalse( otherEntry.sameDB( "otherDb" ) );
    assertEquals( 0, otherEntry.hashCode() );
    assertFalse( entry.equals( otherEntry ) );
    assertFalse( entry.equals( new Object() ) );

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream( baos );

    dos.writeUTF( dbName );
    dos.writeUTF( sql );

    byte[] bytes = baos.toByteArray();
    InputStream is = new ByteArrayInputStream( bytes );
    DataInputStream dis = new DataInputStream( is );
    DbCacheEntry disEntry = new DbCacheEntry( dis );
    assertTrue( disEntry.equals( entry ) );
    try {
      new DbCacheEntry( dis );
      fail( "Should throw HopEofException on EOFException" );
    } catch ( HopEofException keofe ) {
      // Ignore
    }

    baos.reset();

    assertTrue( disEntry.write( dos ) );
    assertArrayEquals( bytes, baos.toByteArray() );
  }
}

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
package org.apache.hop.databases.ingres;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.value.*;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class IngresDatabaseMetaTest {

  private IngresDatabaseMeta nativeMeta;

  @Before
  public void setupBefore() {
    nativeMeta = new IngresDatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
  }

  @Test
  public void testSettings() throws Exception {
    assertArrayEquals( new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE },
      nativeMeta.getAccessTypeList() );
    assertEquals( -1, nativeMeta.getDefaultDatabasePort() ); // pretty sure this is a bug - should be 21064 ( http://community.actian.com/wiki/Ingres_TCP_Ports )
    assertTrue( nativeMeta.supportsAutoInc() );
    assertEquals( 0, nativeMeta.getNotFoundTK( true ) );
    assertEquals( 0, nativeMeta.getNotFoundTK( false ) );
    assertEquals( "com.ingres.jdbc.IngresDriver", nativeMeta.getDriverClass() );
    assertEquals( "jdbc:ingres://FOO:BAR/WIBBLE", nativeMeta.getURL( "FOO", "BAR", "WIBBLE" ) );
    assertEquals( "jdbc:ingres://FOO:II7/WIBBLE", nativeMeta.getURL( "FOO", "", "WIBBLE" ) );

    assertTrue( nativeMeta.isFetchSizeSupported() );
    assertFalse( nativeMeta.supportsBitmapIndex() );
    assertFalse( nativeMeta.supportsSynonyms() );
    assertFalse( nativeMeta.supportsGetBlob() );
  }

  @Test
  public void testSqlStatements() {
    assertEquals( "ALTER TABLE FOO ADD COLUMN BAR VARCHAR(15)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ALTER COLUMN BAR VARCHAR(15)",
      nativeMeta.getModifyColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO DROP COLUMN BAR" + System.getProperty( "line.separator" ),
      nativeMeta.getDropColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", false ) );
    assertEquals( "insert into FOO(FOOKEY, FOOVERSION) values (0, 1)",
      nativeMeta.getSqlInsertAutoIncUnknownDimensionRow( "FOO", "FOOKEY", "FOOVERSION" ) );
    assertEquals( "DELETE FROM FOO",
      nativeMeta.getTruncateTableStatement( "FOO" ) );
  }

  @Test
  public void testGetFieldDefinition() {
    assertEquals( "FOO DATE",
      nativeMeta.getFieldDefinition( new ValueMetaDate( "FOO" ), "", "", false, true, false ) );
    assertEquals( "TIMESTAMP",
      nativeMeta.getFieldDefinition( new ValueMetaTimestamp( "FOO" ), "", "", false, false, false ) );

    assertEquals( "CHAR(1)",
      nativeMeta.getFieldDefinition( new ValueMetaBoolean( "FOO" ), "", "", false, false, false ) );

    assertEquals( "BIGINT PRIMARY KEY IDENTITY(0,1)",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "FOO", 10, 0 ), "FOO", "", true, false, false ) );
    assertEquals( "BIGINT PRIMARY KEY IDENTITY(0,1)",
      nativeMeta.getFieldDefinition( new ValueMetaInteger( "FOO", 10, 0 ), "", "FOO", true, false, false ) );
    assertEquals( "BIGINT PRIMARY KEY NOT NULL",
      nativeMeta.getFieldDefinition( new ValueMetaBigNumber( "FOO", 8, 0 ), "FOO", "", false, false, false ) );
    assertEquals( "BIGINT PRIMARY KEY NOT NULL",
      nativeMeta.getFieldDefinition( new ValueMetaBigNumber( "FOO", 8, 0 ), "", "FOO", false, false, false ) );

    // Integer tests
    assertEquals( "BIGINT",
      nativeMeta.getFieldDefinition( new ValueMetaInteger( "FOO", 10, 0 ), "", "", false, false, false ) );
    assertEquals( "INTEGER",
      nativeMeta.getFieldDefinition( new ValueMetaBigNumber( "FOO", 6, 0 ), "", "", false, false, false ) );
    assertEquals( "SMALLINT",
      nativeMeta.getFieldDefinition( new ValueMetaNumber( "FOO", 3, 0 ), "", "", false, false, false ) );
    assertEquals( "INTEGER1",
      nativeMeta.getFieldDefinition( new ValueMetaInteger( "FOO", 2, 0 ), "", "", false, false, false ) );

    assertEquals( "FLOAT",
      nativeMeta.getFieldDefinition( new ValueMetaBigNumber( "FOO", 6, 3 ), "", "", false, false, false ) );

    // String Types
    assertEquals( "VARCHAR(15)",
      nativeMeta.getFieldDefinition( new ValueMetaString( "FOO", 15, 0 ), "", "", false, false, false ) );
    assertEquals( "VARCHAR(2000)",
      nativeMeta.getFieldDefinition( new ValueMetaString( "FOO" ), "", "", true, false, false ) );
    assertEquals( "VARCHAR(2000)",
      nativeMeta.getFieldDefinition( new ValueMetaString( "FOO", 0, 0 ), "", "", false, false, false ) );
    assertEquals( "VARCHAR(2000)",
      nativeMeta.getFieldDefinition( new ValueMetaString( "FOO", -34, 0 ), "", "", false, false, false ) );

    // Unknown
    assertEquals( " UNKNOWN",
      nativeMeta.getFieldDefinition( new ValueMetaInternetAddress( "FOO" ), "", "", false, false, false ) );

    assertEquals( " UNKNOWN" + System.getProperty( "line.separator" ),
      nativeMeta.getFieldDefinition( new ValueMetaInternetAddress( "FOO" ), "", "", false, false, true ) );
  }


}

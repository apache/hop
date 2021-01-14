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

import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( PowerMockRunner.class )
public class GenericDatabaseMetaTest {
  GenericDatabaseMeta nativeMeta;

  @Mock
  GenericDatabaseMeta mockedMeta;

  @Before
  public void setupBefore() {
    nativeMeta = new GenericDatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
  }

  @Test
  public void testSettings() throws Exception {
    assertArrayEquals( new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE },
      nativeMeta.getAccessTypeList() );
    assertEquals( 1, nativeMeta.getNotFoundTK( true ) );
    assertEquals( 0, nativeMeta.getNotFoundTK( false ) );
    Map<String,String> attrs = new HashMap<>();
    attrs.put( GenericDatabaseMeta.ATRRIBUTE_CUSTOM_DRIVER_CLASS, "foo.bar.wibble" );
    nativeMeta.setManualUrl( "jdbc:foo:bar://foodb" );
    nativeMeta.setAttributes( attrs );
    assertEquals( "foo.bar.wibble", nativeMeta.getDriverClass() );
    assertEquals( "jdbc:foo:bar://foodb", nativeMeta.getURL( "NOT", "GOINGTO", "BEUSED" ) );
    assertFalse( nativeMeta.isFetchSizeSupported() );
    assertFalse( nativeMeta.supportsBitmapIndex() );
    assertFalse( nativeMeta.supportsPreparedStatementMetadataRetrieval() );

  }

  @Test
  public void testSqlStatements() {
    assertEquals( "DELETE FROM FOO", nativeMeta.getTruncateTableStatement( "FOO" ) );
    assertEquals( "SELECT * FROM FOO", nativeMeta.getSqlQueryFields( "FOO" ) );
    assertEquals( "SELECT 1 FROM FOO", nativeMeta.getSqlTableExists( "FOO" ) );

    assertEquals( "ALTER TABLE FOO ADD BAR TIMESTAMP",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaDate( "BAR" ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR TIMESTAMP",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaTimestamp( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR CHAR(1)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBoolean( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 0, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR INTEGER",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 5, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(10, 3)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 10, 3 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(10, 3)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 10, 3 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(21, 4)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 21, 4 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR TEXT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", nativeMeta.getMaxVARCHARLength() + 2, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR VARCHAR(15)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 10, -7 ), "", false, "", false ) ); // Bug here - invalid SQL

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(22, 7)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 22, 7 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", -10, 7 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(5, 7)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 5, 7 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR  UNKNOWN",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInternetAddress( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGSERIAL",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR" ), "BAR", true, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGSERIAL",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 26, 8 ), "BAR", true, "", false ) );

    String lineSep = System.getProperty( "line.separator" );
    assertEquals( "ALTER TABLE FOO DROP BAR" + lineSep,
      nativeMeta.getDropColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", true ) );

    assertEquals( "ALTER TABLE FOO MODIFY BAR VARCHAR(15)",
      nativeMeta.getModifyColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", true ) );

    assertEquals( "ALTER TABLE FOO MODIFY BAR VARCHAR()",
      nativeMeta.getModifyColumnStatement( "FOO", new ValueMetaString( "BAR" ), "", false, "", true ) ); // I think this is a bug ..

    assertEquals( "ALTER TABLE FOO ADD BAR SMALLINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR", 4, 0 ), "", true, "", false ) );


    assertEquals( "ALTER TABLE FOO ADD BAR BIGSERIAL",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR" ), "BAR", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(22, 0)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 22, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR VARCHAR(1)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 1, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR TEXT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 16777250, 0 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR  UNKNOWN",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBinary( "BAR", 16777250, 0 ), "", false, "", false ) );

    assertEquals( "insert into FOO(FOOVERSION) values (1)", nativeMeta.getSqlInsertAutoIncUnknownDimensionRow( "FOO", "FOOKEY", "FOOVERSION" ) );

  }

  @Test
  @PrepareForTest( DatabaseMeta.class )
  public void testSettingDialect() {
    String dialect = "testDialect";
    IDatabase[] dbInterfaces = new IDatabase[] { mockedMeta };
    PowerMockito.mockStatic( DatabaseMeta.class );
    PowerMockito.when( DatabaseMeta.getDatabaseInterfaces() ).thenReturn( dbInterfaces );
    Mockito.when( mockedMeta.getPluginName() ).thenReturn( dialect );
    nativeMeta.addAttribute( "DATABASE_DIALECT_ID", dialect );
    assertEquals( mockedMeta, Whitebox.getInternalState( nativeMeta, "databaseDialect" ) );
  }
  

  
  @Test
  public void testSequence() {
    final String sequenceName = "sequence_name";
    
    IDatabase iDatabase = new GenericDatabaseMeta();
    assertEquals( "", iDatabase.getSqlNextSequenceValue( sequenceName ) );
    assertEquals( "", iDatabase.getSqlCurrentSequenceValue( sequenceName ) );
  }
  
  @Test
  public void testReleaseSavepoint() {
     assertTrue( nativeMeta.releaseSavepoint() );
  }
}

/*!
 * Copyright 2014 - 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.cassandra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.junit.Test;
import org.pentaho.cassandra.driver.datastax.TableMetaData;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CassandraUtilsTest {

  @Test
  public void testRemoveQuotesCQL3() {
    String toTest = "\"AQuotedMixedCaseItentifier\"";

    String result = CassandraUtils.removeQuotes( toTest );

    assertEquals( result, "AQuotedMixedCaseItentifier" );
  }

  @Test
  public void testRemoveQuotesNoQuotesCQL3CaseInsensitive() {

    String toTest = "MixedCaseNoQuotes";
    String result = CassandraUtils.removeQuotes( toTest );

    // Without enclosing quotes Cassandra CQL3 is case insensitive
    assertEquals( result, "mixedcasenoquotes" );
  }

  @Test
  public void testAddQuotesCQL3MixedCase() {
    String toTest = "MixedCaseNoQuotes";

    String result = CassandraUtils.cql3MixedCaseQuote( toTest );

    assertEquals( result, "\"MixedCaseNoQuotes\"" );
  }

  @Test
  public void testAddQuotesCQL3LowerCase() {
    String toTest = "alreadylowercase_noquotesneeded";

    String result = CassandraUtils.cql3MixedCaseQuote( toTest );

    // all lower case does not require enclosing quotes
    assertEquals( result, toTest );
  }

  @Test
  public void testAddQuotesAlreadyQuoted() {
    String toTest = "\"AQuotedMixedCaseItentifier\"";

    String result = CassandraUtils.cql3MixedCaseQuote( toTest );

    // already quoted - should be no change
    assertEquals( result, toTest );
  }

  @Test
  public void testGetPartitionerClassInstance() {
    assertTrue( CassandraUtils.getPartitionerClassInstance( "org.apache.cassandra.dht.Murmur3Partitioner" ).getClass() == Murmur3Partitioner.class );
    assertTrue( CassandraUtils.getPartitionerClassInstance( "org.apache.cassandra.dht.ByteOrderedPartitioner" ).getClass() == ByteOrderedPartitioner.class );
    assertTrue( CassandraUtils.getPartitionerClassInstance( "org.apache.cassandra.dht.RandomPartitioner" ).getClass() == RandomPartitioner.class );
    assertTrue( CassandraUtils.getPartitionerClassInstance( "org.apache.cassandra.dht.OrderPreservingPartitioner" ).getClass() == OrderPreservingPartitioner.class );
  }

  @Test
  public void testGetPartitionKey() {
    String primaryKey = "test1";
    assertNull( CassandraUtils.getPartitionKey( null ) );
    assertNull( CassandraUtils.getPartitionKey( "" ) );
    assertEquals( "test1", CassandraUtils.getPartitionKey( primaryKey ) );

    primaryKey = "test1, test2";
    assertEquals( "test1", CassandraUtils.getPartitionKey( primaryKey ) );

    primaryKey = "(test1, test2), test3";
    assertEquals( "(test1, test2)", CassandraUtils.getPartitionKey( primaryKey ) );

    primaryKey = "(test1, (test2, test3), test4), test5";
    assertEquals( "(test1, (test2, test3), test4)", CassandraUtils.getPartitionKey( primaryKey ) );

    primaryKey = "((test1, test2), test3), test4";
    assertEquals( "((test1, test2), test3)", CassandraUtils.getPartitionKey( primaryKey ) );
  }

  @Test
  public void testKettleToCQLDateAndTimestamp() throws Exception {
    // We will always convert a kettle Date type to a CQL timestamp type
    ValueMetaInterface vmDate = mock( ValueMetaDate.class );
    ValueMetaInterface vmTimestamp = mock( ValueMetaTimestamp.class );
    Date testTimestamp = new Date( 1520816523456L );

    // Kettle Date and Timestamps will produce the same CQL timestamp output result
    when( vmDate.getType() ).thenReturn( ValueMetaInterface.TYPE_DATE );
    when( vmTimestamp.getType() ).thenReturn( ValueMetaInterface.TYPE_TIMESTAMP );
    when( vmDate.getDate( any() ) ).thenReturn( testTimestamp );
    when( vmTimestamp.getDate( any() ) ).thenReturn( testTimestamp );

    assertEquals( "'2018-03-12T01:02:03.456Z'", CassandraUtils.kettleValueToCQL( vmDate, testTimestamp, 3 ) );
    assertEquals( "'2018-03-12T01:02:03.456Z'", CassandraUtils.kettleValueToCQL( vmTimestamp, testTimestamp, 3 ) );
  }

  @Test
  public void testMismatchedCQLDate() {
    Date testDate1 = new Date( 1523542916441L ); // UTC Thu Apr 12 2018 14:21:56
    Date testTimestamp1 = new Date( 1023528397418L ); // UTC Sat Jun 08 2002 09:26:37
    ITableMetaData mockTableMeta = mock( TableMetaData.class );
    RowMetaInterface inputMeta = mock( RowMetaInterface.class );

    List<String> cqlColumnNames = new ArrayList<>();
    cqlColumnNames.add( "date" );
    cqlColumnNames.add( "timestamp" );
    when( mockTableMeta.getColumnNames() ).thenReturn( cqlColumnNames );
    when( mockTableMeta.getColumnCQLType( "date" ) ).thenReturn( DataType.date() );
    when( mockTableMeta.getColumnCQLType( "timestamp" ) ).thenReturn( DataType.timestamp() );

    when( inputMeta.indexOfValue( "date" ) ).thenReturn( 0 );
    when( inputMeta.indexOfValue( "timestamp" ) ).thenReturn( 1 );

    Object[] row = { testDate1, testTimestamp1 };
    List<Object[]> batch = new ArrayList<>();
    batch.add( row );

    LocalDate testLocalDate = LocalDate.fromMillisSinceEpoch( 1523542916441L );

    batch = CassandraUtils.fixBatchMismatchedTypes( batch, inputMeta, mockTableMeta );

    // Fix CQL dates but not timestamps
    assertEquals( testLocalDate, batch.get( 0 )[ 0 ] );
    assertEquals( testTimestamp1, batch.get( 0 )[ 1 ] );
  }
}

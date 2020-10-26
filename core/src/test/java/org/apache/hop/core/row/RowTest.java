/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.core.row;

import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @Test
  public void testNormalStringConversion() throws Exception {
    SimpleDateFormat fmt = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss.SSS" );
    Object[] rowData1 =
      new Object[] { "sampleString", fmt.parse( "2007/05/07 13:04:13.203" ), new Double( 9123.00 ),
        new Long( 12345 ), new BigDecimal( "123456789012345678.9349" ), Boolean.TRUE, };
    IRowMeta rowMeta1 = createTestRowMetaNormalStringConversion1();

    assertEquals( "sampleString", rowMeta1.getString( rowData1, 0 ) );
    assertEquals( "2007/05/07 13:04:13.203", rowMeta1.getString( rowData1, 1 ) );
    assertEquals( "9,123.00", rowMeta1.getString( rowData1, 2 ) );
    assertEquals( "0012345", rowMeta1.getString( rowData1, 3 ) );
    assertEquals( "123456789012345678.9349", rowMeta1.getString( rowData1, 4 ) );
    assertEquals( "Y", rowMeta1.getString( rowData1, 5 ) );

    fmt = new SimpleDateFormat( "yyyyMMddHHmmss" );
    Object[] rowData2 =
      new Object[] { null, fmt.parse( "20070507130413" ), new Double( 9123.9 ), new Long( 12345 ),
        new BigDecimal( "123456789012345678.9349" ), Boolean.FALSE, };
    IRowMeta rowMeta2 = createTestRowMetaNormalStringConversion2();

    assertTrue( rowMeta2.getString( rowData2, 0 ) == null );
    assertEquals( "20070507130413", rowMeta2.getString( rowData2, 1 ) );
    assertEquals( "9.123,9", rowMeta2.getString( rowData2, 2 ) );
    assertEquals( "0012345", rowMeta2.getString( rowData2, 3 ) );
    assertEquals( "123456789012345678.9349", rowMeta2.getString( rowData2, 4 ) );
    assertEquals( "false", rowMeta2.getString( rowData2, 5 ) );
  }

  @Test
  public void testIndexedStringConversion() throws Exception {
    String[] colors = new String[] { "Green", "Red", "Blue", "Yellow", null, };

    // create some timezone friendly dates
    SimpleDateFormat fmt = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss.SSS" );
    Date[] dates =
      new Date[] { fmt.parse( "2007/05/07 13:04:13.203" ), null, fmt.parse( "2007/05/05 05:15:49.349" ),
        fmt.parse( "2007/05/05 19:08:44.736" ), };

    IRowMeta rowMeta = createTestRowMetaIndexedStringConversion1( colors, dates );

    Object[] rowData1 = new Object[] { Integer.valueOf( 0 ), Integer.valueOf( 0 ), };
    Object[] rowData2 = new Object[] { Integer.valueOf( 1 ), Integer.valueOf( 1 ), };
    Object[] rowData3 = new Object[] { Integer.valueOf( 2 ), Integer.valueOf( 2 ), };
    Object[] rowData4 = new Object[] { Integer.valueOf( 3 ), Integer.valueOf( 3 ), };
    Object[] rowData5 = new Object[] { Integer.valueOf( 4 ), Integer.valueOf( 0 ), };

    assertEquals( "Green", rowMeta.getString( rowData1, 0 ) );
    assertEquals( "2007/05/07 13:04:13.203", rowMeta.getString( rowData1, 1 ) );

    assertEquals( "Red", rowMeta.getString( rowData2, 0 ) );
    assertTrue( null == rowMeta.getString( rowData2, 1 ) );

    assertEquals( "Blue", rowMeta.getString( rowData3, 0 ) );
    assertEquals( "2007/05/05 05:15:49.349", rowMeta.getString( rowData3, 1 ) );

    assertEquals( "Yellow", rowMeta.getString( rowData4, 0 ) );
    assertEquals( "2007/05/05 19:08:44.736", rowMeta.getString( rowData4, 1 ) );

    assertTrue( null == rowMeta.getString( rowData5, 0 ) );
    assertEquals( "2007/05/07 13:04:13.203", rowMeta.getString( rowData5, 1 ) );
  }

  @Test
  public void testExtractDataWithTimestampConversion() throws Exception {
    IRowMeta rowMeta = createTestRowMetaNormalTimestampConversion();
    Timestamp constTimestamp = Timestamp.valueOf( "2012-04-05 04:03:02.123456" );
    Timestamp constTimestampForDate = Timestamp.valueOf( "2012-04-05 04:03:02.123" );

    makeTestExtractDataWithTimestampConversion( rowMeta, " Test1", constTimestamp, constTimestamp );
    makeTestExtractDataWithTimestampConversion( rowMeta, " Test2", new Date( constTimestamp.getTime() ),
      constTimestampForDate );
    makeTestExtractDataWithTimestampConversion( rowMeta, " Test3", new java.sql.Date( constTimestamp.getTime() ),
      constTimestampForDate );

  }

  private void makeTestExtractDataWithTimestampConversion( IRowMeta rowMeta, String str, Date date,
                                                           Timestamp constTimestamp ) throws HopEofException, HopFileException, IOException {
    Object[] rowData = new Object[] { str, date };
    byte[] result = RowMeta.extractData( rowMeta, rowData );
    DataInputStream stream = new DataInputStream( new ByteArrayInputStream( result ) );
    String extractedString = (String) new ValueMetaString().readData( stream );
    Timestamp time = (Timestamp) new ValueMetaTimestamp().readData( stream );
    stream.close();
    assertTrue( str.equals( extractedString ) );
    assertTrue( constTimestamp.equals( time ) );

  }

  private IRowMeta createTestRowMetaNormalStringConversion1() {
    IRowMeta rowMeta = new RowMeta();

    // A string object
    IValueMeta meta1 = new ValueMetaString( "stringValue", 30, 0 );
    rowMeta.addValueMeta( meta1 );

    IValueMeta meta2 = new ValueMetaDate( "dateValue", IValueMeta.TYPE_DATE );
    rowMeta.addValueMeta( meta2 );

    IValueMeta meta3 = new ValueMetaNumber( "numberValue", 5, 2 );
    meta3.setConversionMask( "#,##0.00" );
    meta3.setDecimalSymbol( "." );
    meta3.setGroupingSymbol( "," );
    rowMeta.addValueMeta( meta3 );

    IValueMeta meta4 = new ValueMetaInteger( "integerValue", 7, 0 );
    meta4.setConversionMask( "0000000" );
    meta4.setDecimalSymbol( "." );
    meta4.setGroupingSymbol( "," );
    rowMeta.addValueMeta( meta4 );

    IValueMeta meta5 = new ValueMetaBigNumber( "bigNumberValue", 30, 7 );
    meta5.setDecimalSymbol( "." );
    rowMeta.addValueMeta( meta5 );

    IValueMeta meta6 = new ValueMetaBoolean( "booleanValue" );
    rowMeta.addValueMeta( meta6 );

    return rowMeta;
  }

  private IRowMeta createTestRowMetaNormalTimestampConversion() {
    IRowMeta rowMeta = new RowMeta();

    // A string object
    IValueMeta meta1 = new ValueMetaString( "stringValue" );
    rowMeta.addValueMeta( meta1 );

    IValueMeta meta2 = new ValueMetaTimestamp( "timestampValue" );
    rowMeta.addValueMeta( meta2 );

    return rowMeta;
  }

  private IRowMeta createTestRowMetaNormalStringConversion2() {
    IRowMeta rowMeta = new RowMeta();

    // A string object
    IValueMeta meta1 = new ValueMetaString( "stringValue", 30, 0 );
    meta1.setStorageType( IValueMeta.STORAGE_TYPE_INDEXED );
    rowMeta.addValueMeta( meta1 );

    IValueMeta meta2 = new ValueMetaDate( "dateValue" );
    meta2.setConversionMask( "yyyyMMddHHmmss" );
    rowMeta.addValueMeta( meta2 );

    IValueMeta meta3 = new ValueMetaNumber( "numberValue", 5, 2 );
    meta3.setConversionMask( "###,##0.##" );
    meta3.setDecimalSymbol( "," );
    meta3.setGroupingSymbol( "." );
    rowMeta.addValueMeta( meta3 );

    IValueMeta meta4 = new ValueMetaInteger( "integerValue", 7, 0 );
    meta4.setConversionMask( "0000000" );
    meta4.setDecimalSymbol( "," );
    meta4.setGroupingSymbol( "." );
    rowMeta.addValueMeta( meta4 );

    IValueMeta meta5 = new ValueMetaBigNumber( "bigNumberValue", 30, 7 );
    meta5.setDecimalSymbol( "." );
    rowMeta.addValueMeta( meta5 );

    IValueMeta meta6 = new ValueMetaBoolean( "booleanValue", 3, 0 );
    rowMeta.addValueMeta( meta6 );

    return rowMeta;
  }

  private IRowMeta createTestRowMetaIndexedStringConversion1( String[] colors, Date[] dates ) {
    IRowMeta rowMeta = new RowMeta();

    // A string object, indexed.
    IValueMeta meta1 = new ValueMetaString( "stringValue", 30, 0 );
    meta1.setIndex( colors );
    meta1.setStorageType( IValueMeta.STORAGE_TYPE_INDEXED );
    rowMeta.addValueMeta( meta1 );

    IValueMeta meta2 = new ValueMetaDate( "dateValue" );
    meta2.setIndex( dates );
    meta2.setStorageType( IValueMeta.STORAGE_TYPE_INDEXED );
    rowMeta.addValueMeta( meta2 );

    return rowMeta;
  }
}

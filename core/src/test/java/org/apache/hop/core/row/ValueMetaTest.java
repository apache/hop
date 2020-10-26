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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Test functionality in ValueMeta
 */
@SuppressWarnings( "deprecation" )
public class ValueMetaTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  /**
   * Compare to byte arrays for equality.
   *
   * @param b1 1st byte array
   * @param b2 2nd byte array
   * @return true if equal
   */
  private boolean byteCompare( byte[] b1, byte[] b2 ) {
    if ( b1.length != b2.length ) {
      return false;
    }

    int idx = 0;
    while ( idx < b1.length ) {
      if ( b1[ idx ] != b2[ idx ] ) {
        return false;
      }
      idx++;
    }
    return true;
  }

  @Test
  public void testCvtStringToBinaryString() throws Exception {
    ValueMetaString val1 = new ValueMetaString( "STR1" );
    val1.setLength( 6 );
    val1.setStringEncoding( "UTF8" );

    // No truncating or padding!!!
    byte[] b1 = val1.getBinary( "PDI123" );
    assertTrue( byteCompare( b1, new byte[] { 'P', 'D', 'I', '1', '2', '3' } ) );

    byte[] b2 = val1.getBinary( "PDI" );
    assertTrue( byteCompare( b2, new byte[] { 'P', 'D', 'I' } ) );

    byte[] b3 = val1.getBinary( "PDI123456" );
    assertTrue( byteCompare( b3, new byte[] { 'P', 'D', 'I', '1', '2', '3', '4', '5', '6' } ) );

    ValueMetaString val2 = new ValueMetaString( "STR2" );
    val2.setLength( 1 );

    byte[] b4 = val2.getBinary( "PDI123" );
    assertTrue( byteCompare( b4, new byte[] { 'P', 'D', 'I', '1', '2', '3' } ) );

    byte[] b5 = val2.getBinary( "PDI" );
    assertTrue( byteCompare( b5, new byte[] { 'P', 'D', 'I' } ) );

    byte[] b6 = val2.getBinary( "PDI123456" );
    assertTrue( byteCompare( b6, new byte[] { 'P', 'D', 'I', '1', '2', '3', '4', '5', '6' } ) );
  }

  @Test
  public void testCvtStringBinaryString() throws Exception {
    ValueMetaString val1 = new ValueMetaString( "STR1" );
    val1.setLength( 6 );
    val1.setStringEncoding( "UTF8" );

    ValueMetaString val2 = new ValueMetaString( "BINSTR1" );
    val2.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    val2.setStorageMetadata( val1 );
    val2.setLength( 6 );
    val2.setStringEncoding( "UTF8" );

    String str1 = val2.getString( val1.getBinary( "PDI123" ) );
    assertTrue( "PDI123".equals( str1 ) );

    String str2 = val2.getString( val1.getBinary( "PDI" ) );
    assertTrue( "PDI".equals( str2 ) );

    String str3 = val2.getString( val1.getBinary( "PDI123456" ) );
    assertTrue( "PDI123456".equals( str3 ) );
  }

  @Test
  public void testIntegerToStringToInteger() throws Exception {
    IValueMeta intValueMeta = new ValueMetaInteger( "i" );
    intValueMeta.setConversionMask( null );
    intValueMeta.setLength( 7 );

    Long originalValue = new Long( 123L );

    String string = intValueMeta.getString( originalValue );

    assertEquals( " 0000123", string );

    IValueMeta strValueMeta = new ValueMetaString( "str" );
    strValueMeta.setConversionMetadata( intValueMeta );

    Long x = (Long) strValueMeta.convertDataUsingConversionMetaData( string );

    assertEquals( originalValue, x );
  }

  @Test
  public void testNumberToStringToNumber() throws Exception {
    IValueMeta numValueMeta = new ValueMetaNumber( "i" );
    numValueMeta.setConversionMask( null );
    numValueMeta.setLength( 7, 3 );
    numValueMeta.setDecimalSymbol( "," );
    numValueMeta.setGroupingSymbol( "." );

    Double originalValue = new Double( 123.456 );

    String string = numValueMeta.getString( originalValue );

    assertEquals( " 0123,456", string );

    IValueMeta strValueMeta = new ValueMetaString( "str" );
    strValueMeta.setConversionMetadata( numValueMeta );

    Double x = (Double) strValueMeta.convertDataUsingConversionMetaData( string );

    assertEquals( originalValue, x );
  }

  @Test
  public void testBigNumberToStringToBigNumber() throws Exception {
    IValueMeta numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setLength( 42, 9 );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );
    BigDecimal originalValue = new BigDecimal( "34039423484343123.443489056" );

    String string = numValueMeta.getString( originalValue );

    assertEquals( "34039423484343123.443489056", string );

    IValueMeta strValueMeta = new ValueMetaString( "str" );
    strValueMeta.setConversionMetadata( numValueMeta );

    BigDecimal x = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( string );

    assertEquals( originalValue, x );
  }

  @Test
  public void testBigNumberToStringToBigNumberWithByteLimitValues() throws Exception {
    IValueMeta numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );

    String[] strings = new String[] { "-128", "127" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-128" ), new BigDecimal( "127" ) };

    for ( int i = 0; i < values.length; i++ ) {
      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testByteToStringToBigNumberWithByteLimitValues() throws Exception {
    ValueMetaBigNumber numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );
    numValueMeta.setBigNumberFormatting( false );

    String[] input = new String[] { "-128", "127" };
    String[] strings = new String[] { "-128.0", "127.0" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-128" ), new BigDecimal( "127" ) };

    for ( int i = 0; i < values.length; i++ ) {
      String bigNumberToString = numValueMeta.getString( values[ i ] );

      assertEquals( strings[ i ], bigNumberToString );

      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( input[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testBigNumberToStringToBigNumberWithShortLimitValues() throws Exception {
    IValueMeta numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );

    String[] strings = new String[] { "-32768", "32767" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-32768" ), new BigDecimal( "32767" ) };

    for ( int i = 0; i < values.length; i++ ) {
      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testShortToStringToBigNumberWithShortLimitValues() throws Exception {
    ValueMetaBigNumber numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );
    numValueMeta.setBigNumberFormatting( false );

    String[] inputs = new String[] { "-32768", "32767" };
    String[] strings = new String[] { "-32768.0", "32767.0" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-32768" ), new BigDecimal( "32767" ) };

    for ( int i = 0; i < values.length; i++ ) {
      String bigNumberToString = numValueMeta.getString( values[ i ] );

      assertEquals( strings[ i ], bigNumberToString );

      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( inputs[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testBigNumberToStringToBigNumberWithIntLimitValues() throws Exception {
    IValueMeta numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );

    String[] strings = new String[] { "-2147483648", "2147483648" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-2147483648" ), new BigDecimal( "2147483648" ) };

    for ( int i = 0; i < values.length; i++ ) {
      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testIntToStringToBigNumberWithIntLimitValues() throws Exception {
    ValueMetaBigNumber numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );
    numValueMeta.setBigNumberFormatting( false );

    String[] inputs = new String[] { "-2147483648", "2147483648" };
    String[] strings = new String[] { "-2147483648.0", "2147483648.0" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-2147483648" ), new BigDecimal( "2147483648" ) };

    for ( int i = 0; i < values.length; i++ ) {
      String bigNumberToString = numValueMeta.getString( values[ i ] );

      assertEquals( strings[ i ], bigNumberToString );

      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( inputs[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testBigNumberToStringToBigNumberWithLongLimitValues() throws Exception {
    IValueMeta numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );

    String[] strings = new String[] { "-9223372036854775808", "9223372036854775808" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-9223372036854775808" ), new BigDecimal( "9223372036854775808" ) };

    for ( int i = 0; i < values.length; i++ ) {
      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testLongToStringToBigNumberWithLongLimitValues() throws Exception {
    ValueMetaBigNumber numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );
    numValueMeta.setBigNumberFormatting( false );

    String[] inputs = new String[] { "-9223372036854775808", "9223372036854775807" };
    String[] strings = new String[] { "-9223372036854775808.0", "9223372036854775807.0" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-9223372036854775808" ), new BigDecimal( "9223372036854775807" ) };

    for ( int i = 0; i < values.length; i++ ) {
      String bigNumberToString = numValueMeta.getString( values[ i ] );

      assertEquals( strings[ i ], bigNumberToString );

      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( inputs[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testBigNumberToStringToBigNumberWithFloatLimitValues() throws Exception {
    IValueMeta numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );

    String[] strings = new String[] { "-1.4E-45", "3.4028235E38" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-1.4E-45" ), new BigDecimal( "3.4028235E38" ) };

    for ( int i = 0; i < values.length; i++ ) {
      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testFloatToStringToBigNumberWithFloatLimitValues() throws Exception {
    ValueMetaBigNumber numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );
    numValueMeta.setBigNumberFormatting( false );

    String[] strings = new String[] { "-0.0000000000000000000000000000000000000000000014", "340282350000000000000000000000000000000.0" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "-0.0000000000000000000000000000000000000000000014" ), new BigDecimal( "340282350000000000000000000000000000000.0" ) };

    for ( int i = 0; i < values.length; i++ ) {

      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testBigNumberToStringToBigNumberWithDoubleLimitValues() throws Exception {
    IValueMeta numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );

    String[] strings = new String[] { "4.9E-324", "1.7976931348623157E308" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "4.9E-324" ), new BigDecimal( "1.7976931348623157E308" ) };

    for ( int i = 0; i < values.length; i++ ) {
      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testDoubleToStringToBigNumberWithDoubleLimitValues() throws Exception {
    ValueMetaBigNumber numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );
    numValueMeta.setBigNumberFormatting( false );

    String[] strings = new String[] { "4.9E-324", "1.7976931348623157E308" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "4.9E-324" ), new BigDecimal( "1.7976931348623157E308" ) };

    for ( int i = 0; i < values.length; i++ ) {
      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testBigNumberToStringToBigNumberWithNumberCloseToZero() throws Exception {
    IValueMeta numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "," );
    numValueMeta.setGroupingSymbol( "." );

    String[] strings = new String[] { "0,00000000000000000001", "-0,00000000000000000001" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "0.00000000000000000001" ), new BigDecimal( "-0.00000000000000000001" ) };

    for ( int i = 0; i < values.length; i++ ) {
      String bigNumberToString = numValueMeta.getString( values[ i ] );

      assertEquals( strings[ i ], bigNumberToString );

      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testToStringToBigNumberWithNumberCloseToZero() throws Exception {
    ValueMetaBigNumber numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "," );
    numValueMeta.setGroupingSymbol( "." );
    numValueMeta.setBigNumberFormatting( false );

    String[] strings = new String[] { "0,00000000000000000001", "-0,00000000000000000001" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "0.00000000000000000001" ), new BigDecimal( "-0.00000000000000000001" ) };

    for ( int i = 0; i < values.length; i++ ) {
      String bigNumberToString = numValueMeta.getString( values[ i ] );

      assertEquals( strings[ i ], bigNumberToString );

      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ].doubleValue(), stringToBigNumber.doubleValue(), 0 );
    }
  }

  @Test
  public void testPDI17366Conversion() throws Exception {
    ValueMetaBigNumber numValueMeta = new ValueMetaBigNumber( "i" );
    numValueMeta.setDecimalSymbol( "," );
    numValueMeta.setGroupingSymbol( "." );
    numValueMeta.setBigNumberFormatting( false );

    String[] strings = new String[] { "67789,135" };
    BigDecimal[] values = new BigDecimal[] { new BigDecimal( "67789.135" ) };

    for ( int i = 0; i < values.length; i++ ) {
      String bigNumberToString = numValueMeta.getString( values[ i ] );

      assertEquals( strings[ i ], bigNumberToString );

      IValueMeta strValueMeta = new ValueMetaString( "str" );
      strValueMeta.setConversionMetadata( numValueMeta );

      BigDecimal stringToBigNumber = (BigDecimal) strValueMeta.convertDataUsingConversionMetaData( strings[ i ] );

      assertEquals( values[ i ], stringToBigNumber );
    }
  }

  @Test
  public void testDateToStringToDate() throws Exception {
    TimeZone.setDefault( TimeZone.getTimeZone( "CET" ) );

    IValueMeta datValueMeta = new ValueMetaDate( "i" );
    datValueMeta.setConversionMask( "yyyy - MM - dd   HH:mm:ss'('SSS')' z" );
    Date originalValue = new Date( 7258114799999L );

    String string = datValueMeta.getString( originalValue );

    assertEquals( "2199 - 12 - 31   23:59:59(999) CET", string );

    IValueMeta strValueMeta = new ValueMetaString( "str" );
    strValueMeta.setConversionMetadata( datValueMeta );

    Date x = (Date) strValueMeta.convertDataUsingConversionMetaData( string );

    assertEquals( originalValue, x );
  }

  @Test
  public void testDateStringDL8601() throws Exception {
    TimeZone.setDefault( TimeZone.getTimeZone( "America/New_York" ) );

    IValueMeta datValueMeta = new ValueMetaString();
    datValueMeta.setConversionMask( "yyyy-MM-dd'T'HH:mm:ss'.000'XXX" );
    try {
      Date res = datValueMeta.getDate( "2008-03-09T02:34:54.000Z" );
      // make sure it's what we expect...
      Calendar c = Calendar.getInstance();
      c.setTime( res );
      assertEquals( "Month should be 2", 2, c.get( Calendar.MONTH ) );
      assertEquals( "Day should be 8", 8, c.get( Calendar.DAY_OF_MONTH ) );
      assertEquals( "Year should be 2008", 2008, c.get( Calendar.YEAR ) );
    } catch ( Exception ex ) {
      fail( "Error converting date." + ex.getMessage() );
    }

    datValueMeta = new ValueMetaString();
    datValueMeta.setConversionMask( "yyyy-MM-dd'T'HH:mm:ss'.000'XXX" );
    try {
      Date res = datValueMeta.getDate( "2008-03-09T02:34:54.000+01:00" );
      // make sure it's what we expect...
      Calendar c = Calendar.getInstance();
      c.setTime( res );
      assertEquals( "Month should be 2", 2, c.get( Calendar.MONTH ) );
      assertEquals( "Day should be 8", 8, c.get( Calendar.DAY_OF_MONTH ) );
      assertEquals( "Year should be 2008", 2008, c.get( Calendar.YEAR ) );
    } catch ( Exception ex ) {
      fail( "Error converting date." + ex.getMessage() );
    }

    datValueMeta = new ValueMetaString();
    datValueMeta.setConversionMask( "yyyy-MM-dd'T'HH-mm-ss'.000'XXX" );
    try {
      Date res = datValueMeta.getDate( "2008-03-09T02-34-54.000-01:00" );
      // make sure it's what we expect...
      Calendar c = Calendar.getInstance();
      c.setTime( res );
      assertEquals( "Month should be 2", 2, c.get( Calendar.MONTH ) );
      assertEquals( "Day should be 8", 8, c.get( Calendar.DAY_OF_MONTH ) );
      assertEquals( "Year should be 2008", 2008, c.get( Calendar.YEAR ) );
    } catch ( Exception ex ) {
      fail( "Error converting date." + ex.getMessage() );
    }
  }

  @Test
  public void testDateStringUTC() throws Exception {
    TimeZone.setDefault( TimeZone.getTimeZone( "America/New_York" ) );

    IValueMeta datValueMeta = new ValueMetaString();
    datValueMeta.setConversionMask( "yyyy-MM-dd'T'HH:mm:ss'.000'Z" );
    try {
      Date res = datValueMeta.getDate( "2008-03-09T02:34:54.000UTC" );
      // make sure it's what we expect...
      Calendar c = Calendar.getInstance();
      c.setTime( res );
      assertEquals( "Month should be 2", 2, c.get( Calendar.MONTH ) );
      assertEquals( "Day should be 8", 8, c.get( Calendar.DAY_OF_MONTH ) );
      assertEquals( "Year should be 2008", 2008, c.get( Calendar.YEAR ) );
    } catch ( Exception ex ) {
      fail( "Error converting date." + ex.getMessage() );
    }
  }

  @Test
  public void testDateStringOffset() throws Exception {
    TimeZone.setDefault( TimeZone.getTimeZone( "America/New_York" ) );

    IValueMeta datValueMeta = new ValueMetaString();
    datValueMeta.setConversionMask( "yyyy-MM-dd'T'HH:mm:ss'.000'Z" );
    try {
      Date res = datValueMeta.getDate( "2008-03-09T02:34:54.000-0100" );
      // make sure it's what we expect...
      Calendar c = Calendar.getInstance();
      c.setTime( res );
      assertEquals( "Month should be 2", 2, c.get( Calendar.MONTH ) );
      assertEquals( "Day should be 8", 8, c.get( Calendar.DAY_OF_MONTH ) );
      assertEquals( "Year should be 2008", 2008, c.get( Calendar.YEAR ) );
    } catch ( Exception ex ) {
      fail( "Error converting date." + ex.getMessage() );
    }
    datValueMeta = new ValueMetaString();
    datValueMeta.setConversionMask( "yyyy-MM-dd'T'HH:mm:ss'.000'Z" );
    try {
      Date res = datValueMeta.getDate( "2008-03-09T02:34:54.000+0100" );
      // make sure it's what we expect...
      Calendar c = Calendar.getInstance();
      c.setTime( res );
      assertEquals( "Month should be 2", 2, c.get( Calendar.MONTH ) );
      assertEquals( "Day should be 8", 8, c.get( Calendar.DAY_OF_MONTH ) );
      assertEquals( "Year should be 2008", 2008, c.get( Calendar.YEAR ) );
    } catch ( Exception ex ) {
      fail( "Error converting date." + ex.getMessage() );
    }
  }

  @Test
  public void testDateString8601() throws Exception {
    TimeZone.setDefault( TimeZone.getTimeZone( "Europe/Kaliningrad" ) );

    IValueMeta datValueMeta = new ValueMetaString();
    datValueMeta.setConversionMask( "yyyy-MM-dd'T'HH:mm:ss'.000Z'" );
    try {
      Date res = datValueMeta.getDate( "2011-03-13T02:23:18.000Z" );
      Calendar c = Calendar.getInstance();
      c.setTime( res );
      assertEquals( "Month should be 2", 2, c.get( Calendar.MONTH ) );
      assertEquals( "Day should be 13", 13, c.get( Calendar.DAY_OF_MONTH ) );
      assertEquals( "Year should be 2011", 2011, c.get( Calendar.YEAR ) );
    } catch ( Exception ex ) {
      fail( "Error converting date." + ex.getMessage() );
    }

    TimeZone.setDefault( TimeZone.getTimeZone( "America/New_York" ) );
    datValueMeta = new ValueMetaString();
    datValueMeta.setConversionMask( "yyyy-MM-dd'T'HH:mm:ss'.000Z'" );
    try {
      datValueMeta.getDate( "2011-03-13T02:23:18.000Z" );
      fail( "Expected exception when trying to convert date" );
    } catch ( Exception ex ) {
    }
  }

  @Test
  public void testConvertDataDate() throws Exception {
    TimeZone.setDefault( TimeZone.getTimeZone( "CET" ) );

    IValueMeta source = new ValueMetaString( "src" );
    source.setConversionMask( "SSS.ss:mm:HH dd/MM/yyyy z" );
    IValueMeta target = new ValueMetaDate( "tgt" );

    Date date = (Date) target.convertData( source, "999.59:59:23 31/12/2007 CET" );
    assertEquals( new Date( 1199141999999L ), date );

    target.setConversionMask( "yy/MM/dd HH:mm" );

    String string = (String) source.convertData( target, date );
    assertEquals( "07/12/31 23:59", string );

  }

  @Test
  public void testConvertDataInteger() throws Exception {
    IValueMeta source = new ValueMetaString( "src" );
    source.setConversionMask( " #,##0" );
    source.setLength( 12, 3 );
    source.setDecimalSymbol( "," );
    source.setGroupingSymbol( "." );
    IValueMeta target = new ValueMetaInteger( "tgt" );

    Long d = (Long) target.convertData( source, " 2.837" );
    assertEquals( 2837L, d.longValue() );

    target.setConversionMask( "###,###,##0.00" );
    target.setLength( 12, 4 );
    target.setDecimalSymbol( "." );
    target.setGroupingSymbol( "'" );
    String string = (String) source.convertData( target, d );
    assertEquals( "2'837.00", string );
  }

  @Test
  public void testConvertDataNumber() throws Exception {
    IValueMeta source = new ValueMetaString( "src" );
    source.setConversionMask( "###,###,##0.000" );
    source.setLength( 3, 0 );
    source.setDecimalSymbol( "," );
    source.setGroupingSymbol( "." );
    IValueMeta target = new ValueMetaNumber( "tgt" );

    Double d = (Double) target.convertData( source, "123.456.789,012" );
    assertEquals( Double.valueOf( 123456789.012 ), d );

    target.setConversionMask( "###,###,##0.00" );
    target.setLength( 12, 4 );
    target.setDecimalSymbol( "." );
    target.setGroupingSymbol( "'" );

    String string = (String) source.convertData( target, d );
    assertEquals( "123'456'789.01", string );
  }

  /**
   * Lazy conversion is used to read data from disk in a binary format. The data itself is not converted from the byte[]
   * to Integer, rather left untouched until it's needed.
   * <p/>
   * However at that time we do need it we should get the correct value back.
   *
   * @throws Exception
   */
  @Test
  public void testLazyConversionInteger() throws Exception {
    byte[] data = ( "1234" ).getBytes();
    IValueMeta intValueMeta = new ValueMetaInteger( "i" );
    intValueMeta.setConversionMask( null );
    intValueMeta.setLength( 7 );
    intValueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    IValueMeta strValueMeta = new ValueMetaString( "str" );
    intValueMeta.setStorageMetadata( strValueMeta );

    Long integerValue = intValueMeta.getInteger( data );
    assertEquals( new Long( 1234L ), integerValue );
    Double numberValue = intValueMeta.getNumber( data );
    assertEquals( new Double( 1234 ), numberValue );
    BigDecimal bigNumberValue = intValueMeta.getBigNumber( data );
    assertEquals( new BigDecimal( 1234 ), bigNumberValue );
    Date dateValue = intValueMeta.getDate( data );
    assertEquals( new Date( 1234L ), dateValue );
    String string = intValueMeta.getString( data );
    assertEquals( " 0001234", string );
  }

  /**
   * Lazy conversion is used to read data from disk in a binary format. The data itself is not converted from the byte[]
   * to Integer, rather left untouched until it's needed.
   * <p/>
   * However at that time we do need it we should get the correct value back.
   *
   * @throws Exception
   */
  @Test
  public void testLazyConversionNumber() throws Exception {
    byte[] data = ( "1,234.56" ).getBytes();
    IValueMeta numValueMeta = new ValueMetaNumber( "i" );
    numValueMeta.setConversionMask( null );

    // The representation formatting options.
    //
    numValueMeta.setLength( 12, 4 );
    numValueMeta.setDecimalSymbol( "," );
    numValueMeta.setGroupingSymbol( "." );
    numValueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );

    // let's explain to the parser how the input data looks like. (the storage metadata)
    //
    IValueMeta strValueMeta = new ValueMetaString( "str" );
    strValueMeta.setConversionMask( "#,##0.00" );
    strValueMeta.setDecimalSymbol( "." );
    strValueMeta.setGroupingSymbol( "," );
    numValueMeta.setStorageMetadata( strValueMeta );

    Long integerValue = numValueMeta.getInteger( data );
    assertEquals( new Long( 1235L ), integerValue );
    Double numberValue = numValueMeta.getNumber( data );
    assertEquals( new Double( 1234.56 ), numberValue );
    BigDecimal bigNumberValue = numValueMeta.getBigNumber( data );
    assertEquals( BigDecimal.valueOf( 1234.56 ), bigNumberValue );
    Date dateValue = numValueMeta.getDate( data );
    assertEquals( new Date( 1234L ), dateValue );
    String string = numValueMeta.getString( data );
    assertEquals( " 00001234,5600", string );

    // Get the binary data back : has to return exactly the same as we asked ONLY if the formatting options are the same
    // In this unit test they are not!
    //
    byte[] binaryValue = numValueMeta.getBinaryString( data );
    assertTrue( byteCompare( ( " 00001234,5600" ).getBytes(), binaryValue ) );
  }

  /**
   * Lazy conversion is used to read data from disk in a binary format. The data itself is not converted from the byte[]
   * to Integer, rather left untouched until it's needed.
   * <p/>
   * However at that time we do need it we should get the correct value back.
   *
   * @throws Exception
   */
  @Test
  public void testLazyConversionBigNumber() throws Exception {
    String originalValue = "34983433433212304121900934.5634314343";
    byte[] data = originalValue.getBytes();
    IValueMeta numValueMeta = new ValueMetaBigNumber( "i" );
    // The representation formatting options.
    //
    numValueMeta.setLength( 36, 10 );
    numValueMeta.setConversionMask( "#.############" );
    numValueMeta.setDecimalSymbol( "." );
    numValueMeta.setGroupingSymbol( "," );
    numValueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );

    // let's explain to the parser how the input data looks like. (the storage metadata)
    //
    IValueMeta strValueMeta = new ValueMetaString( "str" );
    strValueMeta.setConversionMask( "#.############" );
    strValueMeta.setDecimalSymbol( "." );
    strValueMeta.setGroupingSymbol( "," );

    numValueMeta.setStorageMetadata( strValueMeta );

    // NOTE This is obviously a number that is too large to fit into an Integer or a Number, but this is what we expect
    // to come back.
    // Later it might be better to throw exceptions for big-number to integer conversion.
    // At the time of writing this unit test is not the case.
    // -- Matt

    Long integerValue = numValueMeta.getInteger( data ); // -8165278906150410137
    assertEquals( new Long( -5045838617297571962L ), integerValue );
    Double numberValue = numValueMeta.getNumber( data ); // 3.49834334332123E35
    assertEquals( new Double( "3.4983433433212304E25" ), numberValue );
    BigDecimal bigNumberValue = numValueMeta.getBigNumber( data ); // 349834334332123041219009345634314343
    assertEquals( new BigDecimal( originalValue ), bigNumberValue );
    Date dateValue = numValueMeta.getDate( data );
    assertEquals( new Date( -5045838617297571962L ), dateValue );
    String string = numValueMeta.getString( data );
    assertEquals( originalValue, string );
  }

  @Test
  public void testLazyConversionNullInteger() throws Exception {
    byte[] data = new byte[ 0 ];
    IValueMeta intValueMeta = new ValueMetaBoolean( "i" );
    intValueMeta.setConversionMask( null );
    intValueMeta.setLength( 7 );
    intValueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    IValueMeta strValueMeta = new ValueMetaString( "str" );
    intValueMeta.setStorageMetadata( strValueMeta );

    Double numberValue = intValueMeta.getNumber( data );
    assertEquals( null, numberValue );
    Long integerValue = intValueMeta.getInteger( data );
    assertEquals( null, integerValue );
    BigDecimal bigNumberValue = intValueMeta.getBigNumber( data );
    assertEquals( null, bigNumberValue );
    Date dateValue = intValueMeta.getDate( data );
    assertEquals( null, dateValue );
    String string = intValueMeta.getString( data );
    assertEquals( null, string );
  }

  @Test
  public void testLazyConversionNullNumber() throws Exception {
    byte[] data = new byte[ 0 ];
    IValueMeta intValueMeta = new ValueMetaNumber( "i" );
    intValueMeta.setConversionMask( null );
    intValueMeta.setLength( 7 );
    intValueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    IValueMeta strValueMeta = new ValueMetaString( "str" );
    intValueMeta.setStorageMetadata( strValueMeta );

    Double numberValue = intValueMeta.getNumber( data );
    assertEquals( null, numberValue );
    Long integerValue = intValueMeta.getInteger( data );
    assertEquals( null, integerValue );
    BigDecimal bigNumberValue = intValueMeta.getBigNumber( data );
    assertEquals( null, bigNumberValue );
    Date dateValue = intValueMeta.getDate( data );
    assertEquals( null, dateValue );
    String string = intValueMeta.getString( data );
    assertEquals( null, string );

    Boolean b = intValueMeta.getBoolean( data );
    assertEquals( null, b );
  }

  @Test
  public void testCompareIntegersNormalStorageData() throws Exception {
    Long integer1 = new Long( 1234L );
    Long integer2 = new Long( 1235L );
    Long integer3 = new Long( 1233L );
    Long integer4 = new Long( 1234L );
    Long integer5 = null;
    Long integer6 = null;

    IValueMeta one = new ValueMetaInteger( "one" );
    IValueMeta two = new ValueMetaInteger( "two" );

    assertTrue( one.compare( integer1, integer2 ) < 0 );
    assertTrue( one.compare( integer1, integer3 ) > 0 );
    assertTrue( one.compare( integer1, integer4 ) == 0 );
    assertTrue( one.compare( integer1, integer5 ) != 0 );
    assertTrue( one.compare( integer5, integer6 ) == 0 );

    assertTrue( one.compare( integer1, two, integer2 ) < 0 );
    assertTrue( one.compare( integer1, two, integer3 ) > 0 );
    assertTrue( one.compare( integer1, two, integer4 ) == 0 );
    assertTrue( one.compare( integer1, two, integer5 ) != 0 );
    assertTrue( one.compare( integer5, two, integer6 ) == 0 );
  }

  @Test
  public void testCompareNumbersNormalStorageData() throws Exception {
    Double number1 = new Double( 1234.56 );
    Double number2 = new Double( 1235.56 );
    Double number3 = new Double( 1233.56 );
    Double number4 = new Double( 1234.56 );
    Double number5 = null;
    Double number6 = null;

    IValueMeta one = new ValueMetaNumber( "one" );
    IValueMeta two = new ValueMetaNumber( "two" );

    assertTrue( one.compare( number1, number2 ) < 0 );
    assertTrue( one.compare( number1, number3 ) > 0 );
    assertTrue( one.compare( number1, number4 ) == 0 );
    assertTrue( one.compare( number1, number5 ) != 0 );
    assertTrue( one.compare( number5, number6 ) == 0 );

    assertTrue( one.compare( number1, two, number2 ) < 0 );
    assertTrue( one.compare( number1, two, number3 ) > 0 );
    assertTrue( one.compare( number1, two, number4 ) == 0 );
    assertTrue( one.compare( number1, two, number5 ) != 0 );
    assertTrue( one.compare( number5, two, number6 ) == 0 );
  }

  @Test
  public void testCompareBigNumberNormalStorageData() throws Exception {
    BigDecimal number1 = new BigDecimal( "987908798769876.23943409" );
    BigDecimal number2 = new BigDecimal( "999908798769876.23943409" );
    BigDecimal number3 = new BigDecimal( "955908798769876.23943409" );
    BigDecimal number4 = new BigDecimal( "987908798769876.23943409" );
    BigDecimal number5 = null;
    BigDecimal number6 = null;

    IValueMeta one = new ValueMetaBigNumber( "one" );
    IValueMeta two = new ValueMetaBigNumber( "two" );

    assertTrue( one.compare( number1, number2 ) < 0 );
    assertTrue( one.compare( number1, number3 ) > 0 );
    assertTrue( one.compare( number1, number4 ) == 0 );
    assertTrue( one.compare( number1, number5 ) != 0 );
    assertTrue( one.compare( number5, number6 ) == 0 );

    assertTrue( one.compare( number1, two, number2 ) < 0 );
    assertTrue( one.compare( number1, two, number3 ) > 0 );
    assertTrue( one.compare( number1, two, number4 ) == 0 );
    assertTrue( one.compare( number1, two, number5 ) != 0 );
    assertTrue( one.compare( number5, two, number6 ) == 0 );
  }

  @Test
  public void testCompareDatesNormalStorageData() throws Exception {
    Date date1 = new Date();
    Date date2 = new Date( date1.getTime() + 3600 );
    Date date3 = new Date( date1.getTime() - 3600 );
    Date date4 = new Date( date1.getTime() );
    Date date5 = null;
    Date date6 = null;

    IValueMeta one = new ValueMetaDate( "one" );
    IValueMeta two = new ValueMetaDate( "two" );

    assertTrue( one.compare( date1, date2 ) < 0 );
    assertTrue( one.compare( date1, date3 ) > 0 );
    assertTrue( one.compare( date1, date4 ) == 0 );
    assertTrue( one.compare( date1, date5 ) != 0 );
    assertTrue( one.compare( date5, date6 ) == 0 );

    assertTrue( one.compare( date1, two, date2 ) < 0 );
    assertTrue( one.compare( date1, two, date3 ) > 0 );
    assertTrue( one.compare( date1, two, date4 ) == 0 );
    assertTrue( one.compare( date1, two, date5 ) != 0 );
    assertTrue( one.compare( date5, two, date6 ) == 0 );
  }

  @Test
  public void testCompareBooleanNormalStorageData() throws Exception {
    Boolean boolean1 = new Boolean( false );
    Boolean boolean2 = new Boolean( true );
    Boolean boolean3 = new Boolean( false );
    Boolean boolean4 = null;
    Boolean boolean5 = null;

    IValueMeta one = new ValueMetaBoolean( "one" );
    IValueMeta two = new ValueMetaBoolean( "two" );

    assertTrue( one.compare( boolean1, boolean2 ) < 0 );
    assertTrue( one.compare( boolean1, boolean3 ) == 0 );
    assertTrue( one.compare( boolean1, boolean4 ) != 0 );
    assertTrue( one.compare( boolean4, boolean5 ) == 0 );

    assertTrue( one.compare( boolean1, two, boolean2 ) < 0 );
    assertTrue( one.compare( boolean1, two, boolean3 ) == 0 );
    assertTrue( one.compare( boolean1, two, boolean4 ) != 0 );
    assertTrue( one.compare( boolean4, two, boolean5 ) == 0 );
  }

  @Test
  public void testCompareStringsNormalStorageData() throws Exception {
    String string1 = "bbbbb";
    String string2 = "ccccc";
    String string3 = "aaaaa";
    String string4 = "bbbbb";
    String string5 = null;
    String string6 = null;

    IValueMeta one = new ValueMetaString( "one" );
    IValueMeta two = new ValueMetaString( "two" );

    assertTrue( one.compare( string1, string2 ) < 0 );
    assertTrue( one.compare( string1, string3 ) > 0 );
    assertTrue( one.compare( string1, string4 ) == 0 );
    assertTrue( one.compare( string1, string5 ) != 0 );
    assertTrue( one.compare( string5, string6 ) == 0 );

    assertTrue( one.compare( string1, two, string2 ) < 0 );
    assertTrue( one.compare( string1, two, string3 ) > 0 );
    assertTrue( one.compare( string1, two, string4 ) == 0 );
    assertTrue( one.compare( string1, two, string5 ) != 0 );
    assertTrue( one.compare( string5, two, string6 ) == 0 );
  }

  @Test
  public void testValueMetaInheritance() {
    assertTrue( new ValueMetaBoolean() instanceof IValueMeta );
    assertTrue( new ValueMetaString() instanceof IValueMeta );
    assertTrue( new ValueMetaDate() instanceof IValueMeta );
  }

  @Test
  public void testGetNativeDataTypeClass() throws HopException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init();
    String[] valueMetaNames = ValueMetaFactory.getValueMetaNames();

    for ( int i = 0; i < valueMetaNames.length; i++ ) {
      int vmId = ValueMetaFactory.getIdForValueMeta( valueMetaNames[ i ] );

      IValueMeta vm = ValueMetaFactory.createValueMeta( "", vmId );
      IValueMeta vmi = ValueMetaFactory.createValueMeta( vmId );
      assertTrue( vm.getNativeDataTypeClass().equals( vmi.getNativeDataTypeClass() ) );
    }
  }
}

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
package org.apache.hop.core.row.value;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Created by tkafalas on 12/6/2017.
 */
public class ValueMetaConverterTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  private static final int startSource = 1;
  private static final int endSource = 10;
  private static final int startTarget = 1;
  private static final int endTarget = 10;
  private static final boolean IS_VERBOSE = false; //Change to true to display information

  @Test
  public void convertFromSourceToTargetDataTypeTest() throws Exception {
    //"-", "Number", "String", "Date", "Boolean", "Integer", "BigNumber", "Serializable", "Binary", "Timestamp",
    //  "Internet Address", }
    DateFormat dateFormat = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss.SSS" );
    Date date1 = ( dateFormat.parse( "1999/12/31 00:00:00.000" ) );
    Date timeStamp1 = new Timestamp( dateFormat.parse( "2001/11/01 20:30:15.123" ).getTime() );
    final String inetHost = "127.0.0.1";
    InetAddress inetAddress1 = InetAddress.getByName( inetHost );

    //All combination not listed here should generate ValueMetaConversionExceptions
    // source type, destination type, source object, expected result
    Object[][] tests = new Object[][] {
      { IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_NONE, 1234.56d, null },
      { IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_STRING, 1234.56d, "1234.56" },
      { IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_NUMBER, 1234.56d, 1234.56d },
      { IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, 1234.56d, 1234L },
      { IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER, 1234.56d, new BigDecimal( 1234.56 ) },

      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_NONE, inetHost, null },
      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_STRING, "foobar", "foobar" },
      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_INET, inetHost, inetAddress1 },
      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER, "1234", 1234L },
      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_NUMBER, "1234.56", 1234.56 },
      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_BIGNUMBER, "123456789.123456789",
        new BigDecimal( "123456789.123456789" ) },
      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_TIMESTAMP, "2001/11/01 20:30:15.123", timeStamp1 },
      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_DATE, "1999/12/31 00:00:00.000", date1 },
      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_BOOLEAN, "true", true },
      { IValueMeta.TYPE_STRING, IValueMeta.TYPE_BINARY, "foobar", "foobar".getBytes() },

      { IValueMeta.TYPE_DATE, IValueMeta.TYPE_NONE, date1, null },
      { IValueMeta.TYPE_DATE, IValueMeta.TYPE_DATE, date1, date1 },
      { IValueMeta.TYPE_DATE, IValueMeta.TYPE_INTEGER, date1, date1.getTime() },
      { IValueMeta.TYPE_DATE, IValueMeta.TYPE_STRING, date1, "1999/12/31 00:00:00.000" },
      { IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP, date1, new Timestamp( date1.getTime() ) },

      { IValueMeta.TYPE_BOOLEAN, IValueMeta.TYPE_NONE, true, null },
      { IValueMeta.TYPE_BOOLEAN, IValueMeta.TYPE_STRING, true, "true" },
      { IValueMeta.TYPE_BOOLEAN, IValueMeta.TYPE_BOOLEAN, true, true },

      { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NONE, 1234L, null },
      { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_DATE, date1.getTime(), date1 },
      { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_STRING, 1234L, "1234" },
      { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_INTEGER, 1234L, 1234L },
      { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, 1234L, 1234.0 },
      { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER, 1234L, new BigDecimal( "1234" ) },
      { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_TIMESTAMP, timeStamp1.getTime(), timeStamp1 },

      { IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_NONE, new BigDecimal( "123456.123456" ), null },
      { IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_STRING, new BigDecimal( "123456.123456" ),
        "123456.123456" },
      { IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_NUMBER, new BigDecimal( "123456.123456" ),
        123456.123456d },
      { IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_BIGNUMBER, new BigDecimal( "123456.123456" ),
        new BigDecimal( "123456.123456" ) },

      { IValueMeta.TYPE_SERIALIZABLE, IValueMeta.TYPE_NONE, "foobar", null },
      { IValueMeta.TYPE_SERIALIZABLE, IValueMeta.TYPE_SERIALIZABLE, "foobar", "foobar" },

      { IValueMeta.TYPE_BINARY, IValueMeta.TYPE_NONE, "foobar".getBytes(), null },
      { IValueMeta.TYPE_BINARY, IValueMeta.TYPE_BINARY, "foobar".getBytes(), "foobar".getBytes() },

      { IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_NONE, timeStamp1, null },
      { IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_STRING, timeStamp1, "2001/11/01 20:30:15.123" },
      { IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_INTEGER, timeStamp1, timeStamp1.getTime() },
      { IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_TIMESTAMP, timeStamp1, timeStamp1 },
      { IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_DATE, timeStamp1, new Date( timeStamp1.getTime() ) },

      { IValueMeta.TYPE_INET, IValueMeta.TYPE_NONE, inetAddress1, null },
      { IValueMeta.TYPE_INET, IValueMeta.TYPE_STRING, inetAddress1, inetAddress1.getHostAddress() },
      { IValueMeta.TYPE_INET, IValueMeta.TYPE_INET, inetAddress1, inetAddress1 },

    };

    //Get the tests in a map so that they can be quickly referenced while testing all permutations
    Map<String, Object[]> testMap = new HashMap<>();
    for ( Object[] testSpec : tests ) {
      testMap.put( getKey( (Integer) testSpec[ 0 ], (Integer) testSpec[ 1 ] ), testSpec );
    }

    ValueMetaConverter converter = new ValueMetaConverter();
    for ( int sourceType = startSource; sourceType <= endSource; sourceType++ ) {
      for ( int targetType = startTarget; targetType <= endTarget; targetType++ ) {
        Object[] testSpec = testMap.get( getKey( sourceType, targetType ) );
        if ( testSpec != null ) {
          Object targetValue = converter.convertFromSourceToTargetDataType( sourceType, targetType, testSpec[ 2 ] );
          if ( IS_VERBOSE ) {
            System.out.println(
              "type " + sourceType + "/" + targetType + ":" + testSpec[ 3 ].toString() + "=" + targetValue.toString() );
          }
          if ( targetType == IValueMeta.TYPE_BINARY ) {
            Arrays.equals( (byte[]) testSpec[ 3 ], (byte[]) targetValue );
          } else {
            assertEquals( testSpec[ 3 ], targetValue );
          }
        } else {
          //  Attempt a non-defined conversion.  Should throw an exception.
          try {
            //Get a source object of the correct type
            testSpec = testMap.get( getKey( sourceType, IValueMeta.TYPE_NONE ) );

            if ( IS_VERBOSE ) {
              System.out.println( "type " + sourceType + "/" + targetType + ":" + testSpec[ 2 ].toString() + " should throw Exception" );
            }
            converter.convertFromSourceToTargetDataType( sourceType, targetType, testSpec[ 2 ] );
            fail( "Did not throw exception.  Probably need to make a test entry for this combination." );
          } catch ( ValueMetaConversionException e ) {
            // We are expecting this exception.  Any combination we are not testing should not be supported
            if ( !e.getMessage().contains( "Error.  Can not convert from" ) ) {
              fail( "Got a diferent exception than what was expected" );
            }
          }
        }

        //Now Try and send a null, should always return null
        assertNull( converter.convertFromSourceToTargetDataType( sourceType, targetType, null ) );

        //Now Try to send in a source that is an invalid type - should always fail
        try {
          converter.convertFromSourceToTargetDataType( sourceType, targetType, new Object() );
        } catch ( ValueMetaConversionException e ) {
          // We are expecting this exception.  Any combination we are not testing should not be supported
          if ( !e.getMessage().contains( "Error.  Expecting value of type" ) ) {
            fail( "Got a diferent exception than what was expected" );
          }
        }
      }

    }
  }

  private String getKey( int sourceType, int targetType ) {
    return "" + sourceType + "," + targetType;
  }

}

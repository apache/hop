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

package org.apache.hop.core.util;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for StringEvaluator functionality.
 *
 * @author Alexander Buloichik
 */
public class StringEvaluatorTest {

  private StringEvaluator evaluator;
  private static Locale defaultLocale;

  @BeforeClass
  public static void setUpClass() {
    defaultLocale = Locale.getDefault();
  }

  @Before
  public void setUp() throws HopException {
    HopClientEnvironment.init();
    evaluator = new StringEvaluator();
    Locale.setDefault( Locale.US );
  }

  @AfterClass
  public static void tearDown() {
    Locale.setDefault( defaultLocale );
  }

  /////////////////////////////////////
  // common
  ////////////////////////////////////
  @Test
  public void testEmpty() {
    evaluator.evaluateString( "" );
    assertTrue( evaluator.getStringEvaluationResults().isEmpty() );

    evaluator.evaluateString( "  " );
    assertTrue( evaluator.getStringEvaluationResults().isEmpty() );
  }

  @Test
  public void testGetValues_WithoutDublicate() {
    List<String> strings = Arrays.asList( "name1", "name2" );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertEquals( strings.size(), evaluator.getValues().size() );
    List<String> actualStrings = new ArrayList<>( evaluator.getValues() );
    Collections.sort( strings );
    Collections.sort( actualStrings );

    Iterator<String> exIterator = strings.iterator();
    Iterator<String> iterator = actualStrings.iterator();
    while ( iterator.hasNext() ) {
      assertEquals( exIterator.next(), iterator.next() );
    }
  }

  @Test
  public void testGetValues_Duplicate() {
    String dublicatedString = "name1";
    List<String> strings = Arrays.asList( dublicatedString );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    evaluator.evaluateString( dublicatedString );
    assertEquals( strings.size(), evaluator.getValues().size() );
    Iterator<String> exIterator = strings.iterator();
    Iterator<String> iterator = evaluator.getValues().iterator();
    while ( iterator.hasNext() ) {
      assertEquals( exIterator.next(), iterator.next() );
    }
  }

  @Test
  public void testGetCount() {
    List<String> strings = Arrays.asList( "02/29/2000", "03/29/2000" );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertEquals( strings.size(), evaluator.getCount() );
  }

  @Test
  public void testGetAdvicedResult_NullInput() {
    String expectedNull = "";
    List<String> strings = Arrays.asList( expectedNull );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    StringEvaluationResult result = evaluator.getAdvicedResult();
    assertEquals( 1, result.getNrNull() );
  }

  @Test
  public void testGetAdvicedResult_MinMaxInput() {
    String expectedMin = "500";
    String expectedMax = "1000";
    List<String> strings = Arrays.asList( expectedMax, expectedMin );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    StringEvaluationResult result = evaluator.getAdvicedResult();
    assertEquals( Long.parseLong( expectedMax ), result.getMax() );
    assertEquals( Long.parseLong( expectedMin ), result.getMin() );
    assertEquals( expectedMax.length(), evaluator.getMaxLength() );
  }

  /////////////////////////////////////
  // mixed types
  ////////////////////////////////////
  @Test
  public void testIntegerWithNumber() {
    List<String> strings = Arrays.asList( "1", "1.1" );
    String mask = "#.#";
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isNumber() );
    assertTrue( mask.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
  }

  /////////////////////////////////////
  // number types
  ////////////////////////////////////
  @Test
  public void testNumberWithPoint() {
    testNumber( "#.#", "1.1" );
  }

  @Test
  public void testNumberWithGroupAndPoint() {
    testNumber( "#,###,###.#", "1,111,111.1" );
  }

  @Test
  public void testNumbers() {
    testNumber( "#,###,###.#", "1,111,111.1", "1,111" );
  }

  @Test
  public void testStringAsNumber() {
    evaluator.evaluateString( "1" );
    evaluator.evaluateString( "1.111111111" );
    evaluator.evaluateString( "1,111" );
    evaluator.evaluateString( "1,111.11111111" );
    evaluator.evaluateString( "1,111,111.1111" );
    evaluator.evaluateString( "1,111.111.1111" );
    evaluator.evaluateString( "1,111,111.111.111" );
    assertTrue( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isString() );
  }

  private void testNumber( String mask, String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isNumber() );
    assertTrue( mask.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
  }

  /////////////////////////////////////
  // integer types
  ////////////////////////////////////
  @Test
  public void testInteger() {
    testInteger( "#", "1" );
  }

  @Test
  public void testIntegerWithGroup() {
    testInteger( "#", "1111" );
  }

  private void testInteger( String mask, String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isInteger() );
    assertTrue( mask.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
  }

  /////////////////////////////////////
  // currency types
  ////////////////////////////////////
  @Test
  public void testCurrency() {
    testCurrencyBasic( "+123", "-123", "(123)" );
  }

  private void testCurrencyBasic( String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertTrue( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isString() );
  }

  /////////////////////////////////////
  // boolean types
  ////////////////////////////////////
  @Test
  public void testBooleanY() {
    testBoolean( "Y" );
  }

  @Test
  public void testBooleanN() {
    testBoolean( "N" );
  }

  @Test
  public void testBooleanTrue() {
    testBoolean( "True" );
  }

  @Test
  public void testBooleanFalse() {
    testBoolean( "False" );
  }

  private void testBoolean( String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isBoolean() );
  }

  /////////////////////////////////////
  // Date types, use default USA format
  ////////////////////////////////////
  @Test
  public void testDate() {
    testDefaultDateFormat( "MM/dd/yyyy", "10/10/2000" );
  }

  @Test
  public void testDateArray() {
    testDefaultDateFormat( "MM/dd/yyyy", "10/10/2000", "11/10/2000", "12/10/2000" );
  }

  @Test
  public void testTimeStamp() {
    testDefaultDateFormat( "MM/dd/yyyy HH:mm:ss", "10/10/2000 00:00:00" );
  }

  @Test
  public void testTimeStampSeconds() {
    testDefaultDateFormat( "MM/dd/yyyy HH:mm:ss", "10/10/2000 00:00:00" );
  }

  private void testDefaultDateFormat( String maskEn, String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isDate() );
    String actualMask = evaluator.getAdvicedResult().getConversionMeta().getConversionMask();
    assertTrue( maskEn.equals( actualMask ) );
  }

  @Test
  public void testDate2YearDigits() {
    testDefaultDateFormat( "MM/dd/yy", "10/10/20", "11/10/20", "12/10/20" );
  }

  @Test
  public void testCustomDateFormat() {
    String sampleFormat = "MM/dd/yyyy HH:mm:ss";
    ArrayList<String> dateFormats = new ArrayList<>();
    dateFormats.add( sampleFormat );
    StringEvaluator evaluator = new StringEvaluator( true, new ArrayList<>(), dateFormats );
    evaluator.evaluateString( "02/29/2000 00:00:00" );
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isDate() );
    assertTrue( sampleFormat.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
  }

  @Test
  public void testAdviceedOneDateFormat() {
    String sampleLongFormat = "MM/dd/yyyy HH:mm:ss";
    String sampleShortFormat = "MM/dd/yy HH:mm:ss";
    ArrayList<String> dateFormats = new ArrayList<>();
    dateFormats.add( sampleLongFormat );
    dateFormats.add( sampleShortFormat );
    StringEvaluator evaluator = new StringEvaluator( true, new ArrayList<>(), dateFormats );
    evaluator.evaluateString( "02/29/20 00:00:00" );
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isDate() );
    assertFalse( sampleLongFormat.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
    //should advice short format
    assertTrue( sampleShortFormat.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
  }

}

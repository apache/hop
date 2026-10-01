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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test class for StringEvaluator functionality. */
class StringEvaluatorTest {

  private StringEvaluator evaluator;
  private static Locale defaultLocale;

  @BeforeAll
  static void setUpClass() {
    defaultLocale = Locale.getDefault();
  }

  @BeforeEach
  void setUp() throws HopException {
    HopClientEnvironment.init();
    TestUtil.registerTestPluginTypes();

    evaluator = new StringEvaluator();
    Locale.setDefault(Locale.US);
  }

  @AfterAll
  static void tearDown() {
    Locale.setDefault(defaultLocale);
  }

  // ///////////////////////////////////
  // common
  // /////////////////////////////////

  @Test
  void testEmpty() {
    evaluator.evaluateString("");
    assertTrue(evaluator.getStringEvaluationResults().isEmpty());

    evaluator.evaluateString("  ");
    assertTrue(evaluator.getStringEvaluationResults().isEmpty());
  }

  @Test
  void testGetValues_WithoutDuplicate() {
    List<String> strings = Arrays.asList("name1", "name2");
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    assertEquals(strings.size(), evaluator.getValues().size());
    List<String> actualStrings = new ArrayList<>(evaluator.getValues());
    Collections.sort(strings);
    Collections.sort(actualStrings);

    Iterator<String> exIterator = strings.iterator();
    for (String actualString : actualStrings) {
      assertEquals(exIterator.next(), actualString);
    }
  }

  @Test
  void testGetValues_Duplicate() {
    String dublicatedString = "name1";
    List<String> strings = List.of(dublicatedString);
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    evaluator.evaluateString(dublicatedString);
    assertEquals(strings.size(), evaluator.getValues().size());
    Iterator<String> exIterator = strings.iterator();
    for (String s : evaluator.getValues()) {
      assertEquals(exIterator.next(), s);
    }
  }

  @Test
  void testGetCount() {
    List<String> strings = Arrays.asList("02/29/2000", "03/29/2000");
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    assertEquals(strings.size(), evaluator.getCount());
  }

  @Test
  void testGetAdvicedResult_NullInput() {
    String expectedNull = "";
    List<String> strings = List.of(expectedNull);
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    StringEvaluationResult result = evaluator.getAdvicedResult();
    assertEquals(1, result.getNrNull());
  }

  @Test
  void testGetAdvicedResult_MinMaxInput() {
    String expectedMin = "500";
    String expectedMax = "1000";
    List<String> strings = Arrays.asList(expectedMax, expectedMin);
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    StringEvaluationResult result = evaluator.getAdvicedResult();
    assertEquals(Long.parseLong(expectedMax), result.getMax());
    assertEquals(Long.parseLong(expectedMin), result.getMin());
    assertEquals(expectedMax.length(), evaluator.getMaxLength());
  }

  // ///////////////////////////////////
  // mixed types
  // //////////////////////////////////

  @Test
  void testIntegerWithNumber() {
    List<String> strings = Arrays.asList("1", "1.1");
    String mask = "#.#";
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    assertFalse(evaluator.getStringEvaluationResults().isEmpty());
    assertTrue(evaluator.getAdvicedResult().getConversionMeta().isNumber());
    assertEquals(mask, evaluator.getAdvicedResult().getConversionMeta().getConversionMask());
  }

  // ///////////////////////////////////
  // number types
  // / /////////////////////////////////

  @Test
  void testNumberWithPoint() {
    testNumber("#.#", "1.1");
  }

  @Test
  void testNumberWithGroupAndPoint() {
    testNumber("#,##0.00", "1,111,111.1");
  }

  @Test
  void testNumbers() {
    testNumber("#,##0.00", "1,111,111.1", "1,111");
  }

  @Test
  void testStringAsNumber() {
    evaluator.evaluateString("1");
    evaluator.evaluateString("1.111111111");
    evaluator.evaluateString("1,111");
    evaluator.evaluateString("1,111.11111111");
    evaluator.evaluateString("1,111,111.1111");
    evaluator.evaluateString("1,111.111.1111");
    evaluator.evaluateString("1,111,111.111.111");
    assertTrue(evaluator.getStringEvaluationResults().isEmpty());
    assertTrue(evaluator.getAdvicedResult().getConversionMeta().isString());
  }

  /** #5609: the shortest mask (#.#) used to win and round the amounts to one decimal. */
  @Test
  void testNumberKeepsAllDecimals() {
    for (String string : Arrays.asList("123.58", "90524.10", "9872.52", "130011.63")) {
      evaluator.evaluateString(string);
    }
    IValueMeta advised = evaluator.getAdvicedResult().getConversionMeta();
    assertTrue(advised.isNumber());
    assertEquals("#.00", advised.getConversionMask());
    assertEquals(".", advised.getDecimalSymbol());
    assertEquals(2, advised.getPrecision());
  }

  @Test
  void testNumberPrecisionFollowsTheMostDecimals() {
    for (String string : Arrays.asList("1.5", "2.125", "3")) {
      evaluator.evaluateString(string);
    }
    IValueMeta advised = evaluator.getAdvicedResult().getConversionMeta();
    assertTrue(advised.isNumber());
    assertEquals("#.000", advised.getConversionMask());
    assertEquals(3, advised.getPrecision());
  }

  @Test
  void testNumberWithMoreDecimalsThanAnyMask() {
    evaluator.evaluateString("1.123456789");
    evaluator.evaluateString(null);
    IValueMeta advised = evaluator.getAdvicedResult().getConversionMeta();
    assertTrue(advised.isNumber());
    assertEquals("#.000000000", advised.getConversionMask());
    assertEquals(9, advised.getPrecision());
  }

  /** Grouped values only parse with a grouped mask, none of which has three decimals. */
  @Test
  void testGroupedNumberKeepsAllDecimals() {
    evaluator.evaluateString("1,234.567");
    IValueMeta advised = evaluator.getAdvicedResult().getConversionMeta();
    assertTrue(advised.isNumber());
    assertEquals("#,##0.000", advised.getConversionMask());
    assertEquals(3, advised.getPrecision());
  }

  /** A currency mask keeps the precision of the currency. */
  @Test
  void testCurrencyKeepsItsMask() {
    StringEvaluator usEvaluator = new StringEvaluator();
    usEvaluator.evaluateString("$1,234.5");
    usEvaluator.evaluateString("$12.50");
    IValueMeta advised = usEvaluator.getAdvicedResult().getConversionMeta();
    assertTrue(advised.isNumber());
    assertEquals("$#,##0.00", advised.getConversionMask());
    assertEquals(2, advised.getPrecision());
  }

  @Test
  void testWidenMask() {
    assertEquals("#.000", StringEvaluator.widenMask("#.#", 3));
    assertEquals("#,##0.000", StringEvaluator.widenMask("#,##0.00", 3));
    assertEquals(" #.0000", StringEvaluator.widenMask(" #.0#", 4));
    assertEquals("#,##0.00;-#,##0.00", StringEvaluator.widenMask("#,##0.0;-#,##0.0", 2));
    assertEquals("#.00", StringEvaluator.widenMask("#", 2));
    assertEquals("#.00 EUR", StringEvaluator.widenMask("#.0 EUR", 2));
  }

  /** Masks always use a dot, the locale's decimal separator must not change the precision. */
  @Test
  void testNumberPrecisionDoesNotDependOnLocale() {
    Locale.setDefault(Locale.GERMANY);
    StringEvaluator germanEvaluator = new StringEvaluator();
    for (String string : Arrays.asList("123.58", "9872.52")) {
      germanEvaluator.evaluateString(string);
    }
    IValueMeta advised = germanEvaluator.getAdvicedResult().getConversionMeta();
    assertTrue(advised.isNumber());
    assertEquals("#.00", advised.getConversionMask());
    assertEquals(2, advised.getPrecision());
  }

  @Test
  void testDeterminePrecision() {
    assertEquals(2, StringEvaluator.determinePrecision("#.00"));
    assertEquals(2, StringEvaluator.determinePrecision(" #.0#"));
    assertEquals(0, StringEvaluator.determinePrecision("#"));
    assertEquals(0, StringEvaluator.determinePrecision(null));
    assertEquals(3, StringEvaluator.determinePrecision("1.234,567", ','));
    assertEquals(0, StringEvaluator.determinePrecision("1.234", ','));
  }

  private void testNumber(String mask, String... strings) {
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    assertFalse(evaluator.getStringEvaluationResults().isEmpty());
    assertTrue(evaluator.getAdvicedResult().getConversionMeta().isNumber());
    assertEquals(mask, evaluator.getAdvicedResult().getConversionMeta().getConversionMask());
  }

  // ///////////////////////////////////
  // integer types
  // //////////////////////////////////

  @Test
  void testInteger() {
    testInteger("#", "1");
  }

  @Test
  void testIntegerWithGroup() {
    testInteger("#", "1111");
  }

  private void testInteger(String mask, String... strings) {
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    assertFalse(evaluator.getStringEvaluationResults().isEmpty());
    assertTrue(evaluator.getAdvicedResult().getConversionMeta().isInteger());
    assertEquals(mask, evaluator.getAdvicedResult().getConversionMeta().getConversionMask());
  }

  // ///////////////////////////////////
  // currency types

  // / /////////////////////////////////
  @Test
  void testCurrency() {
    testCurrencyBasic("+123", "-123", "(123)");
  }

  private void testCurrencyBasic(String... strings) {
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    assertTrue(evaluator.getStringEvaluationResults().isEmpty());
    assertTrue(evaluator.getAdvicedResult().getConversionMeta().isString());
  }

  // ///////////////////////////////////
  // boolean types

  // / /////////////////////////////////
  @Test
  void testBooleanY() {
    testBoolean("Y");
  }

  @Test
  void testBooleanN() {
    testBoolean("N");
  }

  @Test
  void testBooleanTrue() {
    testBoolean("True");
  }

  @Test
  void testBooleanFalse() {
    testBoolean("False");
  }

  private void testBoolean(String... strings) {
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    assertFalse(evaluator.getStringEvaluationResults().isEmpty());
    assertTrue(evaluator.getAdvicedResult().getConversionMeta().isBoolean());
  }

  // ///////////////////////////////////
  // Date types, use default USA format

  // / /////////////////////////////////
  @Test
  void testDate() {
    testDefaultDateFormat("MM/dd/yyyy", "10/10/2000");
  }

  @Test
  void testDateArray() {
    testDefaultDateFormat("MM/dd/yyyy", "10/10/2000", "11/10/2000", "12/10/2000");
  }

  @Test
  void testTimeStamp() {
    testDefaultDateFormat("MM/dd/yyyy HH:mm:ss", "10/10/2000 00:00:00");
  }

  @Test
  void testTimeStampSeconds() {
    testDefaultDateFormat("MM/dd/yyyy HH:mm:ss", "10/10/2000 00:00:00");
  }

  private void testDefaultDateFormat(String maskEn, String... strings) {
    for (String string : strings) {
      evaluator.evaluateString(string);
    }
    assertFalse(evaluator.getStringEvaluationResults().isEmpty());
    assertTrue(evaluator.getAdvicedResult().getConversionMeta().isDate());
    String actualMask = evaluator.getAdvicedResult().getConversionMeta().getConversionMask();
    assertEquals(maskEn, actualMask);
  }

  @Test
  void testDate2YearDigits() {
    testDefaultDateFormat("MM/dd/yy", "10/10/20", "11/10/20", "12/10/20");
  }

  @Test
  void testCustomDateFormat() {
    String sampleFormat = "MM/dd/yyyy HH:mm:ss";
    ArrayList<String> dateFormats = new ArrayList<>();
    dateFormats.add(sampleFormat);
    StringEvaluator strEvaluator = new StringEvaluator(true, new ArrayList<>(), dateFormats);
    strEvaluator.evaluateString("02/29/2000 00:00:00");
    assertFalse(strEvaluator.getStringEvaluationResults().isEmpty());
    assertTrue(strEvaluator.getAdvicedResult().getConversionMeta().isDate());
    assertEquals(
        sampleFormat, strEvaluator.getAdvicedResult().getConversionMeta().getConversionMask());
  }

  @Test
  void testAdviceOneDateFormat() {
    String sampleLongFormat = "MM/dd/yyyy HH:mm:ss";
    String sampleShortFormat = "MM/dd/yy HH:mm:ss";
    ArrayList<String> dateFormats = new ArrayList<>();
    dateFormats.add(sampleLongFormat);
    dateFormats.add(sampleShortFormat);
    StringEvaluator strEvaluator = new StringEvaluator(true, new ArrayList<>(), dateFormats);
    strEvaluator.evaluateString("02/29/20 00:00:00");
    assertFalse(strEvaluator.getStringEvaluationResults().isEmpty());
    assertTrue(strEvaluator.getAdvicedResult().getConversionMeta().isDate());
    assertNotEquals(
        sampleLongFormat, strEvaluator.getAdvicedResult().getConversionMeta().getConversionMask());
    // should advise short format
    assertEquals(
        sampleShortFormat, strEvaluator.getAdvicedResult().getConversionMeta().getConversionMask());
  }
}

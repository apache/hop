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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import org.apache.commons.collections4.BidiMap;
import org.junit.BeforeClass;
import org.junit.Test;

public class DateDetectorTest {

  private static final String SAMPLE_REGEXP = "^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$";

  private static final String SAMPLE_DATE_FORMAT = "dd/MM/yyyy HH:mm:ss";

  private static Date sampleDate;

  private static String sampleDateString;

  private static final String LOCALE_EN_US = "en_US";

  private static final String LOCALE_ES = "es";

  private static final String SAMPLE_REGEXP_US =
      "^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$";

  private static final String SAMPLE_DATE_FORMAT_US = "MM/dd/yyyy HH:mm:ss";

  private static Date sampleDateUs;

  private static String sampleDateStringUs;

  @BeforeClass
  public static void setUpClass() {
    SimpleDateFormat format = new SimpleDateFormat(SAMPLE_DATE_FORMAT);
    sampleDate = new Date(0);
    sampleDateString = format.format(sampleDate);

    SimpleDateFormat formatUs = new SimpleDateFormat(SAMPLE_DATE_FORMAT_US);
    sampleDateUs = new Date(0);
    sampleDateStringUs = formatUs.format(sampleDateUs);
  }

  @Test
  public void testGetRegexpByDateFormat() {
    assertNull(DateDetector.getRegexpByDateFormat(null));
    assertEquals(SAMPLE_REGEXP, DateDetector.getRegexpByDateFormat(SAMPLE_DATE_FORMAT));
  }

  @Test
  public void testGetRegexpByDateFormatLocale() {
    assertNull(DateDetector.getRegexpByDateFormat(null, null));
    assertNull(DateDetector.getRegexpByDateFormat(null, LOCALE_EN_US));
    // return null if we pass US dateformat without locale
    assertNull(DateDetector.getRegexpByDateFormat(SAMPLE_DATE_FORMAT_US));
    assertEquals(
        SAMPLE_REGEXP_US, DateDetector.getRegexpByDateFormat(SAMPLE_DATE_FORMAT_US, LOCALE_EN_US));
  }

  @Test
  public void testGetDateFormatByRegex() {
    assertNull(DateDetector.getDateFormatByRegex(null));
    assertEquals(SAMPLE_DATE_FORMAT, DateDetector.getDateFormatByRegex(SAMPLE_REGEXP));
  }

  @Test
  public void testGetDateFormatByRegexLocale() {
    assertNull(DateDetector.getDateFormatByRegex(null, null));
    assertNull(DateDetector.getDateFormatByRegex(null, LOCALE_EN_US));
    // return eu if we pass en_US regexp without locale
    assertEquals(SAMPLE_DATE_FORMAT, DateDetector.getDateFormatByRegex(SAMPLE_REGEXP_US));
    assertEquals(
        SAMPLE_DATE_FORMAT_US, DateDetector.getDateFormatByRegex(SAMPLE_REGEXP_US, LOCALE_EN_US));
  }

  @Test
  public void testGetDateFromString() throws ParseException {
    assertEquals(sampleDateUs, DateDetector.getDateFromString(sampleDateStringUs));
    try {
      DateDetector.getDateFromString(null);
    } catch (ParseException e) {
      // expected exception
    }
  }

  @Test
  public void testGetDateFromStringLocale() throws ParseException {
    assertEquals(sampleDateUs, DateDetector.getDateFromString(sampleDateStringUs, LOCALE_EN_US));
    try {
      DateDetector.getDateFromString(null);
    } catch (ParseException e) {
      // expected exception
    }
    try {
      DateDetector.getDateFromString(null, null);
    } catch (ParseException e) {
      // expected exception
    }
  }

  @Test
  public void testGetDateFromStringByFormat() throws ParseException {
    assertEquals(
        sampleDate, DateDetector.getDateFromStringByFormat(sampleDateString, SAMPLE_DATE_FORMAT));
    try {
      DateDetector.getDateFromStringByFormat(sampleDateString, null);
    } catch (ParseException e) {
      // expected exception
    }
    try {
      DateDetector.getDateFromStringByFormat(null, SAMPLE_DATE_FORMAT);
    } catch (ParseException e) {
      // expected exception
    }
  }

  @Test
  public void testDetectDateFormat() {
    assertEquals(SAMPLE_DATE_FORMAT, DateDetector.detectDateFormat(sampleDateString, LOCALE_ES));
    assertNull(DateDetector.detectDateFormat(null));
  }

  @Test
  public void testIsValidDate() {
    assertTrue(DateDetector.isValidDate(sampleDateStringUs));
    assertFalse(DateDetector.isValidDate(null));
    assertTrue(DateDetector.isValidDate(sampleDateString, SAMPLE_DATE_FORMAT));
    assertFalse(DateDetector.isValidDate(sampleDateString, null));
  }

  @Test
  public void testIsValidDateFormatToStringDate() {
    assertTrue(
        DateDetector.isValidDateFormatToStringDate(SAMPLE_DATE_FORMAT_US, sampleDateStringUs));
    assertFalse(DateDetector.isValidDateFormatToStringDate(null, sampleDateStringUs));
    assertFalse(DateDetector.isValidDateFormatToStringDate(SAMPLE_DATE_FORMAT_US, null));
  }

  @Test
  public void testIsValidDateFormatToStringDateLocale() {
    assertTrue(
        DateDetector.isValidDateFormatToStringDate(
            SAMPLE_DATE_FORMAT_US, sampleDateStringUs, LOCALE_EN_US));
    assertFalse(DateDetector.isValidDateFormatToStringDate(null, sampleDateString, LOCALE_EN_US));
    assertFalse(
        DateDetector.isValidDateFormatToStringDate(SAMPLE_DATE_FORMAT_US, null, LOCALE_EN_US));
    assertTrue(
        DateDetector.isValidDateFormatToStringDate(
            SAMPLE_DATE_FORMAT_US, sampleDateStringUs, null));
  }

  @Test
  public void testAllPatterns() {
    testPatternsFrom(DateDetector.DATE_FORMAT_TO_REGEXPS_US, LOCALE_EN_US);
    testPatternsFrom(DateDetector.DATE_FORMAT_TO_REGEXPS, LOCALE_ES);
  }

  private void testPatternsFrom(BidiMap formatToRegExps, String locale) {
    Iterator iterator = formatToRegExps.keySet().iterator();
    while (iterator.hasNext()) {
      String pattern = (String) iterator.next();
      String dateString = buildTestDate(pattern);
      assertEquals(
          "Did not detect a matching date pattern using the date \"" + dateString + "\"",
          pattern,
          DateDetector.detectDateFormatBiased(dateString, locale, pattern));
    }
  }

  private String buildTestDate(String pattern) {
    return pattern
        .replace("dd", "31")
        .replace("yyyy", "2015")
        .replace("MMMM", "Decr")
        .replace("MMM", "Dec")
        .replace("MM", "12")
        .replace("yy", "15")
        .replace("HH", "12")
        .replace("mm", "00")
        .replace("ss", "00")
        .replace("SSS", "123");
  }
}

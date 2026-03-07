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
package org.apache.hop.core.row.value.timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** User: Dzmitry Stsiapanau Date: 3/17/14 Time: 4:46 PM */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class SimpleTimestampFormatTest {
  private static Locale formatLocale;
  private final Set<Locale> locales =
      new HashSet<>(Arrays.asList(Locale.US, Locale.GERMANY, Locale.JAPANESE, Locale.CHINESE));
  private ResourceBundle tdb;

  private final String stringNinePrecision = "2014-03-15 15:30:45.123456789";
  private final String stringFourPrecision = "2014-03-15 15:30:45.1234";
  private final String stringThreePrecision = "2014-03-15 15:30:45.123";
  private final String stringWithoutPrecision = "2014-03-15 15:30:45";
  private final String stringWithoutPrecisionWithDot = "2014-11-15 15:30:45.000";
  private final Timestamp timestampNinePrecision = Timestamp.valueOf(stringNinePrecision);
  private final Timestamp timestampFourPrecision = Timestamp.valueOf(stringFourPrecision);
  private final Timestamp timestampThreePrecision = Timestamp.valueOf(stringThreePrecision);
  private final Timestamp timestampWithoutPrecision = Timestamp.valueOf(stringWithoutPrecision);
  private final Timestamp timestampWithoutPrecisionWithDot =
      Timestamp.valueOf(stringWithoutPrecisionWithDot);
  private final Date dateThreePrecision = new Date(timestampThreePrecision.getTime());
  private final Date dateWithoutPrecision = new Date(timestampWithoutPrecision.getTime());

  @BeforeEach
  void setUp() {
    formatLocale = Locale.getDefault(Locale.Category.FORMAT);
  }

  @AfterEach
  void tearDown() {
    Locale.setDefault(Locale.Category.FORMAT, formatLocale);
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void testFormat() {
    for (Locale locale : locales) {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      tdb =
          ResourceBundle.getBundle(
              "org.apache.hop/core/row/value/timestamp/messages/testdates", locale);
      checkFormat("HOP.LONG");
      checkFormat("LOCALE.DATE", new SimpleTimestampFormat(new SimpleDateFormat().toPattern()));
      checkFormat("HOP");
      checkFormat("DB.DEFAULT");
      checkFormat("LOCALE.DEFAULT");
    }
  }

  private void checkFormat(String patternName) {
    SimpleTimestampFormat stf = new SimpleTimestampFormat(tdb.getString("PATTERN." + patternName));
    checkFormat(patternName, stf);
  }

  private void checkFormat(String patternName, SimpleTimestampFormat stf) {
    String localeForErrorMSG = Locale.getDefault(Locale.Category.FORMAT).toLanguageTag();
    assertEquals(
        tdb.getString("TIMESTAMP.NINE." + patternName),
        (stf.format(timestampNinePrecision)),
        localeForErrorMSG + "=locale localized pattern= " + stf.toLocalizedPattern());
    assertEquals(
        tdb.getString("TIMESTAMP.THREE." + patternName),
        (stf.format(timestampThreePrecision)),
        localeForErrorMSG + "=locale localized pattern= " + stf.toLocalizedPattern());
    assertEquals(
        tdb.getString("TIMESTAMP.ZERO." + patternName),
        (stf.format(timestampWithoutPrecision)),
        localeForErrorMSG + "=locale localized pattern= " + stf.toLocalizedPattern());
    assertEquals(
        tdb.getString("TIMESTAMP.DOT." + patternName),
        (stf.format(timestampWithoutPrecisionWithDot)),
        localeForErrorMSG + "=locale localized pattern= " + stf.toLocalizedPattern());
    assertEquals(
        tdb.getString("DATE.THREE." + patternName),
        (stf.format(dateThreePrecision)),
        localeForErrorMSG + "=locale localized pattern= " + stf.toLocalizedPattern());
    assertEquals(
        tdb.getString("DATE.ZERO." + patternName),
        (stf.format(dateWithoutPrecision)),
        localeForErrorMSG + "=locale localized pattern= " + stf.toLocalizedPattern());
  }

  @Test
  void testParse() throws Exception {
    for (Locale locale : locales) {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      tdb =
          ResourceBundle.getBundle(
              "org.apache.hop/core/row/value/timestamp/messages/testdates", locale);

      checkParseHop();
      checkParseHopLong();
      checkParseDbDefault();
      checkParseLocaleDefault();
    }
  }

  private void checkParseHop() throws ParseException {
    String patternName = "HOP";
    SimpleTimestampFormat stf = new SimpleTimestampFormat(tdb.getString("PATTERN." + patternName));
    String localeForErrorMSG = Locale.getDefault(Locale.Category.FORMAT).toLanguageTag();
    parseUnit(
        "TIMESTAMP.NINE." + patternName,
        stf,
        localeForErrorMSG,
        timestampThreePrecision); // ThreePrecision only for Hop
    parseUnit("TIMESTAMP.THREE." + patternName, stf, localeForErrorMSG, timestampThreePrecision);
    parseUnit("TIMESTAMP.ZERO." + patternName, stf, localeForErrorMSG, timestampWithoutPrecision);
    parseUnit(
        "TIMESTAMP.DOT." + patternName, stf, localeForErrorMSG, timestampWithoutPrecisionWithDot);
    parseUnit("DATE.THREE." + patternName, stf, localeForErrorMSG, dateThreePrecision);
    parseUnit("DATE.ZERO." + patternName, stf, localeForErrorMSG, dateWithoutPrecision);
  }

  private void checkParseHopLong() throws ParseException {
    String patternName = "HOP.LONG";
    SimpleTimestampFormat stf = new SimpleTimestampFormat(tdb.getString("PATTERN." + patternName));
    String localeForErrorMSG = Locale.getDefault(Locale.Category.FORMAT).toLanguageTag();
    parseUnit(
        "TIMESTAMP.NINE." + patternName,
        stf,
        localeForErrorMSG,
        timestampFourPrecision); // FourPrecision only for Hop long
    parseUnit("TIMESTAMP.THREE." + patternName, stf, localeForErrorMSG, timestampThreePrecision);
    parseUnit("TIMESTAMP.ZERO." + patternName, stf, localeForErrorMSG, timestampWithoutPrecision);
    parseUnit(
        "TIMESTAMP.DOT." + patternName, stf, localeForErrorMSG, timestampWithoutPrecisionWithDot);
    parseUnit("DATE.THREE." + patternName, stf, localeForErrorMSG, dateThreePrecision);
    parseUnit("DATE.ZERO." + patternName, stf, localeForErrorMSG, dateWithoutPrecision);
  }

  private void checkParseDbDefault() throws ParseException {
    String patternName = "DB.DEFAULT";
    SimpleTimestampFormat stf = new SimpleTimestampFormat(tdb.getString("PATTERN." + patternName));
    String localeForErrorMSG = Locale.getDefault(Locale.Category.FORMAT).toLanguageTag();
    parseUnit("TIMESTAMP.NINE." + patternName, stf, localeForErrorMSG, timestampNinePrecision);
    parseUnit("TIMESTAMP.THREE." + patternName, stf, localeForErrorMSG, timestampThreePrecision);
    parseUnit("TIMESTAMP.ZERO." + patternName, stf, localeForErrorMSG, timestampWithoutPrecision);
    parseUnit(
        "TIMESTAMP.DOT." + patternName, stf, localeForErrorMSG, timestampWithoutPrecisionWithDot);
    parseUnit("DATE.THREE." + patternName, stf, localeForErrorMSG, dateThreePrecision);
    parseUnit("DATE.ZERO." + patternName, stf, localeForErrorMSG, dateWithoutPrecision);
  }

  private void checkParseLocaleDefault() throws ParseException {
    String patternName = "LOCALE.DEFAULT";
    SimpleTimestampFormat stf = new SimpleTimestampFormat(tdb.getString("PATTERN." + patternName));
    String localeForErrorMSG = Locale.getDefault(Locale.Category.FORMAT).toLanguageTag();
    parseUnit("TIMESTAMP.NINE." + patternName, stf, localeForErrorMSG, timestampNinePrecision);
    parseUnit("TIMESTAMP.THREE." + patternName, stf, localeForErrorMSG, timestampThreePrecision);
    parseUnit("TIMESTAMP.ZERO." + patternName, stf, localeForErrorMSG, timestampWithoutPrecision);
    parseUnit(
        "TIMESTAMP.DOT." + patternName, stf, localeForErrorMSG, timestampWithoutPrecisionWithDot);
    parseUnit("DATE.THREE." + patternName, stf, localeForErrorMSG, dateThreePrecision);
    parseUnit("DATE.ZERO." + patternName, stf, localeForErrorMSG, dateWithoutPrecision);
  }

  protected void checkParseLocalTimestamp() throws ParseException {
    String patternName = "LOCALE.TIMESTAMP";
    SimpleTimestampFormat stf = new SimpleTimestampFormat(tdb.getString("PATTERN." + patternName));
    String localeForErrorMSG = Locale.getDefault(Locale.Category.FORMAT).toLanguageTag();
    parseUnit("TIMESTAMP.NINE." + patternName, stf, localeForErrorMSG, timestampThreePrecision);
    parseUnit("TIMESTAMP.THREE." + patternName, stf, localeForErrorMSG, timestampThreePrecision);
    parseUnit("TIMESTAMP.ZERO." + patternName, stf, localeForErrorMSG, timestampWithoutPrecision);
    parseUnit(
        "TIMESTAMP.DOT." + patternName, stf, localeForErrorMSG, timestampWithoutPrecisionWithDot);
    parseUnit("DATE.THREE." + patternName, stf, localeForErrorMSG, dateThreePrecision);
    parseUnit("DATE.ZERO." + patternName, stf, localeForErrorMSG, dateWithoutPrecision);
  }

  private void parseUnit(
      String patternName, SimpleTimestampFormat stf, String localeForErrorMSG, Date date)
      throws ParseException {
    if (date instanceof Timestamp) {
      assertEquals(
          date,
          (stf.parse(tdb.getString(patternName))),
          localeForErrorMSG + "=locale localized pattern= " + stf.toLocalizedPattern());
    } else {
      assertEquals(
          date,
          (stf.parse(tdb.getString(patternName))),
          localeForErrorMSG + "=locale localized pattern= " + stf.toLocalizedPattern());
    }
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void testToPattern() {
    for (Locale locale : locales) {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      tdb =
          ResourceBundle.getBundle(
              "org/apache/hop/core/row/value/timestamp/messages/testdates", locale);
      String patternExample = tdb.getString("PATTERN.HOP");
      SimpleTimestampFormat stf = new SimpleTimestampFormat(new SimpleDateFormat().toPattern());
      assertEquals(locale.toLanguageTag(), tdb.getString("PATTERN.LOCALE.DATE"), stf.toPattern());
      stf = new SimpleTimestampFormat(patternExample, Locale.GERMANY);
      assertEquals(locale.toLanguageTag(), patternExample, stf.toPattern());
      stf = new SimpleTimestampFormat(patternExample, Locale.US);
      assertEquals(locale.toLanguageTag(), patternExample, stf.toPattern());
    }
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void testToLocalizedPattern() {
    for (Locale locale : locales) {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      tdb =
          ResourceBundle.getBundle(
              "org/apache/hop/core/row/value/timestamp/messages/testdates", locale);
      SimpleTimestampFormat stf = new SimpleTimestampFormat(new SimpleDateFormat().toPattern());
      assertEquals(
          locale.toLanguageTag(),
          tdb.getString("PATTERN.LOCALE.COMPILED"),
          stf.toLocalizedPattern());
      String patternExample = tdb.getString("PATTERN.HOP");
      stf = new SimpleTimestampFormat(patternExample);
      assertEquals(
          locale.toLanguageTag(),
          tdb.getString("PATTERN.LOCALE.COMPILED_DATE"),
          stf.toLocalizedPattern());
    }
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void testApplyPattern() {
    for (Locale locale : locales) {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      tdb =
          ResourceBundle.getBundle(
              "org/apache/hop/core/row/value/timestamp/messages/testdates", locale);
      String patternExample = tdb.getString("PATTERN.HOP");
      SimpleTimestampFormat stf = new SimpleTimestampFormat(new SimpleDateFormat().toPattern());
      assertEquals(locale.toLanguageTag(), tdb.getString("PATTERN.LOCALE.DATE"), stf.toPattern());
      stf.applyPattern(patternExample);
      checkFormat("HOP", stf);
    }
  }

  @Test
  void testApplyLocalizedPattern() {
    Locale.setDefault(Locale.Category.FORMAT, Locale.US);
    SimpleTimestampFormat stf = new SimpleTimestampFormat(new SimpleDateFormat().toPattern());
    for (Locale locale : locales) {
      Locale.setDefault(Locale.Category.FORMAT, locale);
      tdb =
          ResourceBundle.getBundle(
              "org/apache/hop/core/row/value/timestamp/messages/testdates", locale);
      stf.applyLocalizedPattern(tdb.getString("PATTERN.LOCALE.DEFAULT"));
      assertEquals(
          tdb.getString("PATTERN.LOCALE.DEFAULT"),
          stf.toLocalizedPattern(),
          locale.toLanguageTag());
      checkFormat("LOCALE.DEFAULT", stf);
    }
  }

  @Test
  void testParseNull() {
    String pattern = "yyyy-MM-dd HH:mm:ss.S";
    SimpleTimestampFormat stf = new SimpleTimestampFormat(pattern);
    // This value mimics a bad parse that might return null from super.parse()
    // but previously caused an NPE in SimpleTimestampFormat
    String invalidValue = "this-is-not-a-date";
    ParsePosition pos = new ParsePosition(0);
    java.util.Date result = stf.parse(invalidValue, pos);
    // Should return null (and set error index) instead of throwing NPE
    assertNull(result);
  }
}

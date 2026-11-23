/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;
import java.util.function.Supplier;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Whenever {@link FastValueFormatter} renders a value, the text must be exactly what {@link
 * IValueMeta#getString(Object)} or {@link SimpleDateFormat#format(Date)} gives, in any default
 * locale and time zone.
 */
class FastValueFormatterTest {

  private static final Locale[] LOCALES = {
    Locale.US,
    Locale.GERMANY,
    Locale.FRANCE,
    Locale.forLanguageTag("sv-SE"),
    Locale.forLanguageTag("ar-EG"),
    Locale.forLanguageTag("ar-SA"),
    Locale.forLanguageTag("fa-IR"),
    Locale.forLanguageTag("th-TH"),
    Locale.forLanguageTag("th-TH-u-nu-thai"),
    Locale.forLanguageTag("ja-JP-u-ca-japanese"),
    Locale.forLanguageTag("hi-IN"),
    Locale.forLanguageTag("de-CH")
  };

  private static final String[] ZONES = {
    "UTC",
    "Europe/Brussels",
    "America/New_York",
    "Asia/Kolkata",
    "Pacific/Apia",
    "Australia/Lord_Howe"
  };

  private Locale savedLocale;
  private TimeZone savedZone;

  @BeforeAll
  static void setUpOnce() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void saveDefaults() {
    savedLocale = Locale.getDefault();
    savedZone = TimeZone.getDefault();
  }

  @AfterEach
  void restoreDefaults() {
    Locale.setDefault(savedLocale);
    TimeZone.setDefault(savedZone);
  }

  /** Checks one value: the fast text, when there is one, equals the slow text. */
  private static boolean check(FastValueFormatter fast, Supplier<String> slow, Object value) {
    if (fast == null) {
      return false;
    }
    StringBuilder sb = new StringBuilder("prefix");
    boolean done = fast.append(sb, value);
    if (done) {
      assertEquals(slow.get(), sb.substring(6), "value " + value);
    } else {
      assertEquals("prefix", sb.toString(), "nothing is appended when the slow path is needed");
    }
    return done;
  }

  private static List<Double> doubles(Random random) {
    List<Double> values = new ArrayList<>();
    double[] fixed = {
      0.0,
      -0.0,
      1.0,
      -1.0,
      0.1,
      0.2,
      0.3,
      0.1 + 0.2,
      12.99,
      -3.5,
      1.0 / 3,
      2.0 / 3,
      0.001,
      0.0015,
      0.000999,
      262143.9999999999,
      262144.0,
      -262143.75,
      123456.123456789,
      99999.99999999995,
      1.0E7,
      1.0E-5,
      4.35,
      2.675,
      1.005,
      1234.5678901234,
      Double.MIN_VALUE,
      Double.MAX_VALUE,
      Double.NaN,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY,
      0.30000000000000004,
      5.0E-4,
      9.999999999E-4
    };
    for (double d : fixed) {
      values.add(d);
    }
    for (int i = 0; i < 20_000; i++) {
      switch (i % 5) {
        case 0 -> values.add(random.nextDouble() * 300_000 - 150_000);
        case 1 -> values.add(Math.round(random.nextDouble() * 1_000_000) / 100.0); // prices
        case 2 -> values.add(Math.round(random.nextDouble() * 1_000) / 1000.0);
        case 3 -> values.add(Double.longBitsToDouble(random.nextLong()));
        default -> values.add((random.nextInt(2_000_000) - 1_000_000) / 1e4);
      }
    }
    return values;
  }

  @Test
  void numbersInDefaultMaskMatchGetString() throws Exception {
    Random random = new Random(42);
    List<Double> values = doubles(random);
    for (Locale locale : LOCALES) {
      Locale.setDefault(locale);
      ValueMetaNumber meta = new ValueMetaNumber("n");
      FastValueFormatter fast = FastValueFormatter.forValueMeta(meta);
      int done = 0;
      for (Double value : values) {
        if (check(fast, () -> getString(meta, value), value)) {
          done++;
        }
      }
      if (locale.equals(Locale.US)) {
        assertNotNull(fast);
        assertTrue(done > values.size() / 2, "the fast path is used for most values: " + done);
      }
    }
  }

  @Test
  void numbersWithAnExplicitDotDecimalSymbolMatchGetString() throws Exception {
    Random random = new Random(7);
    List<Double> values = doubles(random);
    for (Locale locale : LOCALES) {
      Locale.setDefault(locale);
      ValueMetaNumber meta = new ValueMetaNumber("n");
      meta.setDecimalSymbol(".");
      meta.setGroupingSymbol(",");
      FastValueFormatter fast = FastValueFormatter.forValueMeta(meta);
      for (Double value : values) {
        check(fast, () -> getString(meta, value), value);
      }
    }
  }

  @Test
  void integersMatchGetString() throws Exception {
    Random random = new Random(3);
    List<Long> values = new ArrayList<>(List.of(0L, -1L, 1L, Long.MAX_VALUE, Long.MIN_VALUE));
    for (int i = 0; i < 5_000; i++) {
      values.add(random.nextLong() >> random.nextInt(64));
    }
    for (Locale locale : LOCALES) {
      Locale.setDefault(locale);
      ValueMetaInteger meta = new ValueMetaInteger("i");
      FastValueFormatter fast = FastValueFormatter.forValueMeta(meta);
      if (locale.equals(Locale.US)) {
        assertNotNull(fast);
      }
      for (Long value : values) {
        check(fast, () -> getString(meta, value), value);
      }
    }
  }

  @Test
  void bigNumbersMatchGetString() throws Exception {
    Random random = new Random(11);
    List<BigDecimal> values = new ArrayList<>();
    for (String s :
        new String[] {
          "0",
          "0.00",
          "0E-25",
          "-0.00",
          "1E+3",
          "1E-30",
          "0.1",
          "-7.25",
          "12345.678",
          "0.00000000000000000001",
          "0.000000000000000000001",
          "123456789012345678901234567890.5",
          "100",
          "-100.000"
        }) {
      values.add(new BigDecimal(s));
    }
    for (int i = 0; i < 5_000; i++) {
      values.add(new BigDecimal(new BigInteger(80, random), random.nextInt(30) - 5).negate());
      values.add(new BigDecimal(new BigInteger(40, random), random.nextInt(25)));
    }
    for (Locale locale : LOCALES) {
      Locale.setDefault(locale);
      ValueMetaBigNumber meta = new ValueMetaBigNumber("b");
      FastValueFormatter fast = FastValueFormatter.forValueMeta(meta);
      if (locale.equals(Locale.US)) {
        assertNotNull(fast);
      }
      for (BigDecimal value : values) {
        check(fast, () -> getString(meta, value), value);
      }
    }
  }

  @Test
  void datesMatchGetStringInManyZonesLocalesAndMasks() throws Exception {
    String[] masks = {
      null,
      "yyyy-MM-dd",
      "yyyy-MM-dd HH:mm:ss.SSS",
      "yyyy/MM/dd HH:mm:ss",
      "yyyyMMdd",
      "y-M-d H:m:s.S",
      "yyyy-MM-dd'T'HH:mm:ss",
      "yyyy-MM-dd'T'HH:mm:ss''SSSSSS",
      "dd.MM.yyyy",
      "dd-MMM-yyyy",
      "yy-MM-dd",
      "yyyy-MM-dd hh:mm a",
      "yyyy-MM-dd HH:mm:ss XXX"
    };
    Random random = new Random(2024);
    List<Date> dates = new ArrayList<>();
    for (int i = 0; i < 1_500; i++) {
      double years = i % 3 == 0 ? random.nextDouble() * 16_000 - 6_000 : random.nextDouble() * 200;
      dates.add(new Date((long) ((years - 70) * 365.2425 * 86_400_000L)));
    }
    dates.add(new Date(0));
    dates.add(new Date(-1));
    dates.add(new Date(Long.MAX_VALUE));
    dates.add(new Date(Long.MIN_VALUE));
    dates.add(new java.sql.Timestamp(1_700_000_000_123L));
    for (String zone : ZONES) {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      for (Locale locale : LOCALES) {
        Locale.setDefault(locale);
        for (String mask : masks) {
          ValueMetaDate meta = new ValueMetaDate("d");
          meta.setConversionMask(mask);
          FastValueFormatter fast = FastValueFormatter.forValueMeta(meta);
          int done = 0;
          for (Date date : dates) {
            if (check(fast, () -> getString(meta, date), date)) {
              done++;
            }
          }
          if (locale.equals(Locale.US) && (mask == null || mask.startsWith("yyyy-MM-dd'T'"))) {
            assertTrue(done > dates.size() / 2, "fast path used for " + mask + ": " + done);
          }
          if (mask != null) {
            SimpleDateFormat format = new SimpleDateFormat(mask);
            FastValueFormatter direct = FastValueFormatter.forDateFormat(format);
            for (Date date : dates) {
              check(direct, () -> format.format(date), date);
            }
          }
        }
      }
    }
  }

  @Test
  void theFastPathIsUsedForTheUsualConfigurations() {
    Locale.setDefault(Locale.US);
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));
    ValueMetaDate date = new ValueMetaDate("d");
    date.setConversionMask("yyyy-MM-dd");
    FastValueFormatter fast = FastValueFormatter.forValueMeta(date);
    assertNotNull(fast);
    StringBuilder sb = new StringBuilder();
    assertTrue(fast.append(sb, new Date(1_700_000_000_000L)));
    assertEquals("2023-11-14", sb.toString());

    assertNotNull(FastValueFormatter.forValueMeta(new ValueMetaDate("default mask")));
    assertNotNull(FastValueFormatter.forDateFormat(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")));
  }

  @Test
  void configurationsThatNeedTheSlowPath() {
    Locale.setDefault(Locale.US);
    // explicit masks, padding, other storage, other types
    ValueMetaNumber masked = new ValueMetaNumber("n");
    masked.setConversionMask("#,##0.00");
    assertNull(FastValueFormatter.forValueMeta(masked));
    ValueMetaInteger padded = new ValueMetaInteger("i", 5, 0);
    padded.setOutputPaddingEnabled(true);
    assertNull(FastValueFormatter.forValueMeta(padded));
    ValueMetaInteger lazy = new ValueMetaInteger("i");
    lazy.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    lazy.setStorageMetadata(new ValueMetaString("i"));
    assertNull(FastValueFormatter.forValueMeta(lazy));
    ValueMetaNumber comma = new ValueMetaNumber("n");
    comma.setDecimalSymbol(",");
    assertNull(FastValueFormatter.forValueMeta(comma));
    assertNull(FastValueFormatter.forValueMeta(new ValueMetaTimestamp("t")));
    assertNull(FastValueFormatter.forValueMeta(new ValueMetaString("s")));
    // month names, two digit years, time zones and AM/PM markers
    for (String mask : new String[] {"dd-MMM-yyyy", "yy-MM-dd", "HH:mm z", "hh:mm a", "'open"}) {
      ValueMetaDate date = new ValueMetaDate("d");
      date.setConversionMask(mask);
      assertNull(FastValueFormatter.forValueMeta(date), mask);
    }
    // Buddhist years
    assertNull(
        FastValueFormatter.forDateFormat(
            new SimpleDateFormat("yyyy-MM-dd", Locale.forLanguageTag("th-TH"))));
    // values of an unexpected class, or outside the supported range
    FastValueFormatter number = FastValueFormatter.forValueMeta(new ValueMetaNumber("n"));
    assertNotNull(number);
    assertFalse(number.append(new StringBuilder(), "12"));
    assertFalse(number.append(new StringBuilder(), 1.0E-4));
    assertFalse(number.append(new StringBuilder(), 1.0E6));
  }

  private static String getString(IValueMeta meta, Object value) {
    try {
      return meta.getString(value);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}

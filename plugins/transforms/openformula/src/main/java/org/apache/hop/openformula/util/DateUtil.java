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

package org.apache.hop.openformula.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/** Date calculation and parsing utilities for OpenFormula expressions. */
public final class DateUtil {
  public static final long MILLIS_PER_DAY = 86_400_000L;

  private DateUtil() {}

  /** Returns the OpenFormula zero-date (January 1, 1900). */
  public static Date getZeroDate(TimeZone timeZone) {
    Calendar cal =
        new GregorianCalendar(timeZone != null ? timeZone : TimeZone.getDefault(), Locale.US);
    cal.clear();
    cal.set(1900, Calendar.JANUARY, 1, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  /** Converts a Date into an OpenFormula serial day number (fractional days). */
  public static BigDecimal dateToSerial(Date date, TimeZone timeZone) {
    if (date == null) {
      return BigDecimal.ZERO;
    }
    Date zero = getZeroDate(timeZone);
    long diffMillis = date.getTime() - zero.getTime();
    return BigDecimal.valueOf(diffMillis)
        .divide(BigDecimal.valueOf(MILLIS_PER_DAY), 40, RoundingMode.HALF_UP)
        .stripTrailingZeros();
  }

  /** Converts an OpenFormula serial day number into a Date. */
  public static Date serialToDate(BigDecimal serial, TimeZone timeZone) {
    if (serial == null) {
      return getZeroDate(timeZone);
    }
    BigDecimal millis = serial.multiply(BigDecimal.valueOf(MILLIS_PER_DAY));
    long offsetMillis = millis.setScale(0, RoundingMode.HALF_UP).longValue();
    Date zero = getZeroDate(timeZone);
    return new Date(zero.getTime() + offsetMillis);
  }

  public static Date createDate(int year, int month, int day, TimeZone tz) {
    Calendar cal = new GregorianCalendar(tz != null ? tz : TimeZone.getDefault(), Locale.US);
    cal.clear();
    cal.set(year, month - 1, day, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  public static Date createDateTime(
      int year,
      int month,
      int day,
      int hour,
      int minute,
      int second,
      int millisecond,
      TimeZone tz) {
    Calendar cal = new GregorianCalendar(tz != null ? tz : TimeZone.getDefault(), Locale.US);
    cal.clear();
    cal.set(year, month - 1, day, hour, minute, second);
    cal.set(Calendar.MILLISECOND, millisecond);
    return cal.getTime();
  }

  private static final String[] DATE_PATTERNS = {
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ssZ",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
    "yyyy/MM/dd HH:mm:ss",
    "yyyy/MM/dd",
    "MM/dd/yyyy HH:mm:ss",
    "MM/dd/yyyy",
    "dd/MM/yyyy HH:mm:ss",
    "dd/MM/yyyy",
    "d/M/yyyy HH:mm:ss",
    "d/M/yyyy",
    "dd-MM-yyyy HH:mm:ss",
    "dd-MM-yyyy",
    "EEE MMM dd HH:mm:ss zzz yyyy",
    "EEE MMM dd HH:mm:ss z yyyy"
  };

  private static final String[] TIME_PATTERNS = {"HH:mm:ss.SSS", "HH:mm:ss", "HH:mm"};

  public static Date parseDate(String text, Locale locale, TimeZone timeZone) {
    if (text == null || text.isBlank()) {
      return null;
    }
    String trimmed = text.trim();
    for (String pattern : DATE_PATTERNS) {
      SimpleDateFormat sdf = new SimpleDateFormat(pattern, locale != null ? locale : Locale.US);
      if (timeZone != null) {
        sdf.setTimeZone(timeZone);
      }
      sdf.setLenient(false);
      try {
        return sdf.parse(trimmed);
      } catch (ParseException ignored) {
      }
    }

    // Try time-only patterns relative to epoch 1899-12-30
    for (String pattern : TIME_PATTERNS) {
      SimpleDateFormat sdf = new SimpleDateFormat(pattern, locale != null ? locale : Locale.US);
      if (timeZone != null) {
        sdf.setTimeZone(timeZone);
      }
      sdf.setLenient(false);
      try {
        Date parsedTime = sdf.parse(trimmed);
        Calendar tCal = Calendar.getInstance(timeZone != null ? timeZone : TimeZone.getDefault());
        tCal.setTime(parsedTime);
        Calendar zeroCal =
            Calendar.getInstance(timeZone != null ? timeZone : TimeZone.getDefault());
        zeroCal.setTime(getZeroDate(timeZone));
        zeroCal.set(Calendar.HOUR_OF_DAY, tCal.get(Calendar.HOUR_OF_DAY));
        zeroCal.set(Calendar.MINUTE, tCal.get(Calendar.MINUTE));
        zeroCal.set(Calendar.SECOND, tCal.get(Calendar.SECOND));
        zeroCal.set(Calendar.MILLISECOND, tCal.get(Calendar.MILLISECOND));
        return zeroCal.getTime();
      } catch (ParseException ignored) {
      }
    }
    return null;
  }
}

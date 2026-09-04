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

package org.apache.hop.pipeline.transforms.calculator.calculations.date;

import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.calculator.ICalculation;

/**
 * Converts between a {@link Date} and a JD Edwards Julian date stored as CYYDDD.
 *
 * <p>C is the century relative to 1900 (0 = 1900-1999, 1 = 2000-2099), YY is the two-digit year,
 * and DDD is the day of the year (001-366). Examples: 95001 is 1995-01-01, 109001 is 2009-01-01.
 */
public final class JdeJulian {
  private JdeJulian() {}

  public static Long fromDate(Date date) throws HopValueException {
    if (date == null) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    int year = calendar.get(Calendar.YEAR);
    if (year < 1900) {
      throw new HopValueException(
          BaseMessages.getString(
              ICalculation.PKG, "Calculator.Error.DateBeforeJdeEpoch", formatDate(date)));
    }

    return (year - 1900) * 1000L + calendar.get(Calendar.DAY_OF_YEAR);
  }

  public static Date toDate(Long julian) throws HopValueException {
    if (julian == null) {
      return null;
    }

    long yearLong = 1900L + julian / 1000L;
    int dayOfYear = (int) (julian % 1000L);
    if (julian < 1L || yearLong < 1900L || yearLong > LocalDate.MAX.getYear()) {
      throw invalidJulian(julian);
    }

    try {
      LocalDate localDate = LocalDate.ofYearDay((int) yearLong, dayOfYear);
      return Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
    } catch (DateTimeException e) {
      throw invalidJulian(julian);
    }
  }

  private static HopValueException invalidJulian(long julian) {
    return new HopValueException(
        BaseMessages.getString(ICalculation.PKG, "Calculator.Error.InvalidJdeJulian", julian));
  }

  private static String formatDate(Date date) {
    return new SimpleDateFormat("yyyy-MM-dd").format(date);
  }
}

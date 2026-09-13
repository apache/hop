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

package org.apache.hop.i18n;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.Locale;

/**
 * The sample values shown in the regional settings preview, computed for an arbitrary locale
 * without touching the JVM default. Deliberately free of any UI dependency so it can be tested
 * headlessly.
 */
public class RegionalSettingsPreview {

  /** A fixed sample number, chosen to exercise both the grouping and the decimal separator. */
  private static final double SAMPLE_NUMBER = 10000.23d;

  private static final double SAMPLE_CURRENCY = 1234.56d;
  private static final double SAMPLE_PERCENT = 0.85d;

  private final String shortDate;
  private final String longDate;
  private final String shortTime;
  private final String longTime;
  private final String number;
  private final String negativeNumber;
  private final String currency;
  private final String percent;

  private RegionalSettingsPreview(Locale locale, Date now) {
    this.shortDate = DateFormat.getDateInstance(DateFormat.SHORT, locale).format(now);
    this.longDate = DateFormat.getDateInstance(DateFormat.LONG, locale).format(now);
    this.shortTime = DateFormat.getTimeInstance(DateFormat.SHORT, locale).format(now);
    this.longTime = DateFormat.getTimeInstance(DateFormat.MEDIUM, locale).format(now);
    this.number = NumberFormat.getNumberInstance(locale).format(SAMPLE_NUMBER);
    this.negativeNumber = NumberFormat.getNumberInstance(locale).format(-SAMPLE_NUMBER);
    this.currency = NumberFormat.getCurrencyInstance(locale).format(SAMPLE_CURRENCY);
    this.percent = NumberFormat.getPercentInstance(locale).format(SAMPLE_PERCENT);
  }

  /** Builds the preview values for the given locale, using the current date and time. */
  public static RegionalSettingsPreview of(Locale locale) {
    return new RegionalSettingsPreview(locale, new Date());
  }

  /**
   * Builds the preview values for a fixed instant. Package-private on purpose: comparing how two
   * locales render the same instant is only meaningful if it really is the same instant, and some
   * calendar dates render identically in two locales that normally differ — the Italian and US
   * short formats coincide on 10/10, 11/11 and 12/12, for instance.
   */
  static RegionalSettingsPreview of(Locale locale, Date instant) {
    return new RegionalSettingsPreview(locale, instant);
  }

  public String getShortDate() {
    return shortDate;
  }

  public String getLongDate() {
    return longDate;
  }

  public String getShortTime() {
    return shortTime;
  }

  public String getLongTime() {
    return longTime;
  }

  public String getNumber() {
    return number;
  }

  public String getNegativeNumber() {
    return negativeNumber;
  }

  public String getCurrency() {
    return currency;
  }

  public String getPercent() {
    return percent;
  }
}

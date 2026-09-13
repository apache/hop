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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class RegionalSettingsPreviewTest {

  @Test
  void formatsNumbersWithTheGivenLocaleRegardlessOfTheJvmDefault() {
    RegionalSettingsPreview italian = RegionalSettingsPreview.of(Locale.ITALY);
    RegionalSettingsPreview american = RegionalSettingsPreview.of(Locale.US);

    assertEquals("10.000,23", italian.getNumber());
    assertEquals("10,000.23", american.getNumber());
  }

  @Test
  void formatsNegativeNumbersAndPercentages() {
    assertEquals("-10.000,23", RegionalSettingsPreview.of(Locale.ITALY).getNegativeNumber());
    assertEquals("-10,000.23", RegionalSettingsPreview.of(Locale.US).getNegativeNumber());
    // The sample percentage has no fractional part, so it reads the same in both of the locales
    // above; assert the exact expected value rather than merely that a percent sign is present.
    assertEquals("85%", RegionalSettingsPreview.of(Locale.ITALY).getPercent());
  }

  @Test
  void currencyUsesTheLocaleCurrencySymbol() {
    assertTrue(
        RegionalSettingsPreview.of(Locale.US).getCurrency().contains("$"),
        RegionalSettingsPreview.of(Locale.US).getCurrency());
    assertTrue(
        RegionalSettingsPreview.of(Locale.ITALY).getCurrency().contains("€"),
        RegionalSettingsPreview.of(Locale.ITALY).getCurrency());
  }

  /**
   * Dates and times must follow the locale that was passed in. Asserting merely that the strings
   * are non-empty would pass against an implementation that quietly used the JVM default locale,
   * which is the one mistake this class exists to avoid, so two locales rendering the same instant
   * must disagree instead.
   *
   * <p>The instant is fixed rather than "now" for two reasons. The Italian short format is {@code
   * dd/MM/yy} and the US one {@code M/d/yy}, so they render identically whenever day and month are
   * equal and both are at least ten — 10/10, 11/11 and 12/12 — and the test would fail on those
   * three days a year against perfectly correct code. A fixed instant also removes the (tiny)
   * chance of the two preview objects straddling a day boundary and appearing to differ for the
   * wrong reason.
   */
  @Test
  void dateAndTimeFollowTheGivenLocale() {
    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set(2026, Calendar.JANUARY, 15, 14, 30, 45);
    Date instant = calendar.getTime();

    RegionalSettingsPreview italian = RegionalSettingsPreview.of(Locale.ITALY, instant);
    RegionalSettingsPreview american = RegionalSettingsPreview.of(Locale.US, instant);

    assertNotEquals(italian.getShortDate(), american.getShortDate());
    assertNotEquals(italian.getLongDate(), american.getLongDate());
    assertNotEquals(italian.getShortTime(), american.getShortTime());
    assertNotEquals(italian.getLongTime(), american.getLongTime());
  }
}

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

package org.apache.hop.core.row.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;
import java.util.Locale;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The interface language and the regional settings are separate since the FORMAT category was
 * introduced, so a locale set explicitly on a field has to be compared against the regional
 * settings rather than against the language.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaDateFormatLocaleTest {

  /** 1 January 2025, so the month name differs per locale. */
  private static final Date DATE = new Date(1735689600000L);

  private static final String MASK = "MMMM yyyy";

  private static void languageAndRegionalSettings(Locale language, Locale regional) {
    // Order matters: setDefault(Locale) writes all three categories, so the FORMAT one goes last.
    Locale.setDefault(language);
    Locale.setDefault(Locale.Category.FORMAT, regional);
  }

  /**
   * The regression this test exists for: the locale picked on the field happens to be the same as
   * the interface language. Comparing it against the language would classify it as "no locale
   * chosen" and fall back to the regional settings, silently discarding the user's choice.
   */
  @Test
  void explicitDateLocaleIsHonouredEvenWhenItEqualsTheInterfaceLanguage() {
    languageAndRegionalSettings(Locale.ITALY, Locale.US);

    ValueMetaDate valueMeta = new ValueMetaDate("d");
    valueMeta.setConversionMask(MASK);
    valueMeta.setDateFormatLocale(Locale.ITALY);

    assertEquals("gennaio 2025", valueMeta.getDateFormat().format(DATE));
  }

  /** The same choice, when it does not collide with the language, always worked. */
  @Test
  void explicitDateLocaleIsHonouredWhenItDiffersFromTheInterfaceLanguage() {
    languageAndRegionalSettings(Locale.ITALY, Locale.US);

    ValueMetaDate valueMeta = new ValueMetaDate("d");
    valueMeta.setConversionMask(MASK);
    valueMeta.setDateFormatLocale(Locale.FRANCE);

    assertEquals("janvier 2025", valueMeta.getDateFormat().format(DATE));
  }

  /** With no explicit choice the field follows the regional settings, not the language. */
  @Test
  void withoutAnExplicitDateLocaleTheRegionalSettingsWin() {
    languageAndRegionalSettings(Locale.US, Locale.ITALY);

    ValueMetaDate valueMeta = new ValueMetaDate("d");
    valueMeta.setConversionMask(MASK);
    valueMeta.setDateFormatLocale(null);

    assertEquals("gennaio 2025", valueMeta.getDateFormat().format(DATE));
  }

  @Test
  void timestampExplicitDateLocaleIsHonouredEvenWhenItEqualsTheInterfaceLanguage() {
    languageAndRegionalSettings(Locale.ITALY, Locale.US);

    ValueMetaTimestamp valueMeta = new ValueMetaTimestamp("t");
    valueMeta.setConversionMask(MASK);
    valueMeta.setDateFormatLocale(Locale.ITALY);

    assertEquals("gennaio 2025", valueMeta.getDateFormat().format(DATE));
  }

  @Test
  void timestampWithoutAnExplicitDateLocaleFollowsTheRegionalSettings() {
    languageAndRegionalSettings(Locale.US, Locale.ITALY);

    ValueMetaTimestamp valueMeta = new ValueMetaTimestamp("t");
    valueMeta.setConversionMask(MASK);
    valueMeta.setDateFormatLocale(null);

    assertEquals("gennaio 2025", valueMeta.getDateFormat().format(DATE));
  }
}

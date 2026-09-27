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

package org.apache.hop.core.row.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.Locale;
import org.apache.hop.core.exception.HopValueException;
import org.junit.jupiter.api.Test;

/** ASCII and Unicode minus signs must parse in locales whose negative prefix is not '-'. */
class ValueMetaBaseLocaleMinusTest {

  @Test
  void asciiMinusParsesInLocalesWithADifferentNegativePrefix() {
    assertParsesNegativeTwo(Locale.forLanguageTag("nb-NO"));
    assertParsesNegativeTwo(Locale.forLanguageTag("sv-SE"));
    assertParsesNegativeTwo(Locale.forLanguageTag("fi-FI"));
    assertParsesNegativeTwo(Locale.forLanguageTag("ar-SA"));
  }

  @Test
  void unicodeMinusParsesWithAnAsciiNegativePrefix() throws Exception {
    Locale original = Locale.getDefault();
    Locale originalFormat = Locale.getDefault(Locale.Category.FORMAT);
    try {
      Locale.setDefault(Locale.US);
      Locale.setDefault(Locale.Category.FORMAT, Locale.US);
      ValueMetaInteger meta = new ValueMetaInteger("balance");
      assertEquals(-2L, meta.convertStringToInteger("-2"));
      assertEquals(-2L, meta.convertStringToInteger("\u22122"));
    } finally {
      Locale.setDefault(original);
      Locale.setDefault(Locale.Category.FORMAT, originalFormat);
    }
  }

  @Test
  void nonNumericTextStillFails() {
    withLocale(
        Locale.forLanguageTag("nb-NO"),
        () -> {
          ValueMetaInteger meta = new ValueMetaInteger("balance");
          assertThrows(HopValueException.class, () -> meta.convertStringToInteger("x"));
          assertThrows(HopValueException.class, () -> meta.convertStringToInteger("-2x"));
        });
  }

  private static void assertParsesNegativeTwo(Locale locale) {
    withLocale(
        locale,
        () -> {
          assertEquals(-2L, new ValueMetaInteger("balance").convertStringToInteger("-2"));
          assertEquals(2L, new ValueMetaInteger("balance").convertStringToInteger("2"));
          assertEquals(-2L, new ValueMetaInteger("balance").convertStringToInteger("\u22122"));
          assertEquals(-2.0d, new ValueMetaNumber("balance").convertStringToNumber("-2"));
          assertEquals(
              0,
              new BigDecimal("-2")
                  .compareTo(new ValueMetaBigNumber("balance").convertStringToBigNumber("-2")));
        });
  }

  private static void withLocale(Locale locale, LocaleCheck check) {
    Locale original = Locale.getDefault();
    Locale originalFormat = Locale.getDefault(Locale.Category.FORMAT);
    try {
      Locale.setDefault(locale);
      Locale.setDefault(Locale.Category.FORMAT, locale);
      check.run();
    } catch (Exception e) {
      throw new AssertionError(locale.toString(), e);
    } finally {
      Locale.setDefault(original);
      Locale.setDefault(Locale.Category.FORMAT, originalFormat);
    }
  }

  @FunctionalInterface
  private interface LocaleCheck {
    void run() throws Exception;
  }
}

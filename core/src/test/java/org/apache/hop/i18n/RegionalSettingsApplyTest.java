/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class RegionalSettingsApplyTest {

  // RestoreHopEnvironmentExtension restores locales and LanguageChoice but knows nothing about
  // the RegionalSettings singleton, so reset it explicitly to avoid leaking state into whatever
  // test class runs next in the same JVM fork.
  @AfterAll
  static void resetRegionalSettingsSingleton() {
    RegionalSettings.getInstance().reload();
  }

  @BeforeEach
  void resetConfiguration() {
    HopConfig.getInstance().saveOption(RegionalSettings.STRING_REGIONAL_SETTINGS_SOURCE, null);
    HopConfig.getInstance().saveOption(RegionalSettings.STRING_REGIONAL_SETTINGS_LOCALE, null);
    LanguageChoice.getInstance().setDefaultLocale(Locale.forLanguageTag("en-US"));
    RegionalSettings.getInstance().reload();
  }

  /**
   * With no regional configuration at all, a headless run puts the operating system locale on the
   * FORMAT category and leaves the interface language alone.
   *
   * <p>This is not a behaviour change: the operating system locale is the JVM's own initial
   * default, which is exactly what a headless run already carries, so writing it back changes
   * nothing that an existing installation observes.
   */
  @Test
  void headlessAppliesTheOperatingSystemLocaleWhenNothingIsConfigured() {
    Locale.setDefault(Locale.GERMANY);
    Locale.setDefault(Locale.Category.FORMAT, Locale.GERMANY);
    RegionalSettings settings = RegionalSettings.getInstance();

    settings.applyHeadless();

    assertEquals(Locale.GERMANY, Locale.getDefault());
    assertEquals(settings.getOperatingSystemLocale(), Locale.getDefault(Locale.Category.FORMAT));
  }

  @Test
  void headlessAppliesTheCustomLocaleToTheFormatCategoryOnly() {
    Locale.setDefault(Locale.GERMANY);
    RegionalSettings settings = RegionalSettings.getInstance();
    settings.setSource(RegionalSettings.Source.CUSTOM);
    settings.setCustomLocale(Locale.ITALY);

    settings.applyHeadless();

    assertEquals(Locale.GERMANY, Locale.getDefault());
    assertEquals(Locale.ITALY, Locale.getDefault(Locale.Category.FORMAT));
  }

  /**
   * With no regional configuration the GUI keeps the interface language while deriving the formats
   * from the operating system, exactly as a headless run does.
   */
  @Test
  void guiDerivesFormatsFromTheOperatingSystemByDefault() {
    Locale.setDefault(Locale.GERMANY);
    RegionalSettings settings = RegionalSettings.getInstance();
    Locale osLocale = settings.getOperatingSystemLocale();
    // Pick a language that cannot coincide with this machine's locale, or the assertion below
    // would hold whichever of the two the code actually used.
    Locale language = Locale.ITALY.equals(osLocale) ? Locale.US : Locale.ITALY;
    LanguageChoice.getInstance().setDefaultLocale(language);

    settings.applyGui();

    assertEquals(language, Locale.getDefault());
    assertEquals(osLocale, Locale.getDefault(Locale.Category.FORMAT));
    assertNotEquals(language, Locale.getDefault(Locale.Category.FORMAT));
  }

  /**
   * With the language chosen as the source, every category follows the interface language, so the
   * GUI, hop-run and hop-server all agree.
   */
  @Test
  void guiDerivesFormatsFromTheLanguageWhenThatSourceIsChosen() {
    Locale.setDefault(Locale.GERMANY);
    RegionalSettings settings = RegionalSettings.getInstance();
    Locale osLocale = settings.getOperatingSystemLocale();
    // Pick a language that cannot coincide with this machine's locale, or the assertion below
    // would hold whichever of the two the code actually used.
    Locale language = Locale.ITALY.equals(osLocale) ? Locale.US : Locale.ITALY;
    LanguageChoice.getInstance().setDefaultLocale(language);
    settings.setSource(RegionalSettings.Source.LANGUAGE);

    settings.applyGui();

    assertEquals(language, Locale.getDefault());
    assertEquals(language, Locale.getDefault(Locale.Category.FORMAT));
    assertNotEquals(osLocale, Locale.getDefault(Locale.Category.FORMAT));
  }

  /**
   * The feature this issue asks for: an English interface with Italian regional settings. The
   * language governs the messages, the regional locale governs the numbers.
   */
  @Test
  void guiKeepsTheLanguageWhileOverridingTheRegionalSettings() throws HopValueException {
    LanguageChoice.getInstance().setDefaultLocale(Locale.forLanguageTag("en-US"));
    RegionalSettings settings = RegionalSettings.getInstance();
    settings.setSource(RegionalSettings.Source.CUSTOM);
    settings.setCustomLocale(Locale.ITALY);

    settings.applyGui();

    // The interface language, which is what ResourceBundle resolves messages with.
    assertEquals(Locale.forLanguageTag("en-US"), Locale.getDefault());
    // The regional settings.
    assertEquals(Locale.ITALY, Locale.getDefault(Locale.Category.FORMAT));
    assertEquals(',', new DecimalFormatSymbols().getDecimalSeparator());

    // And the engine follows: the exact literal depends on the default conversion mask, so we
    // assert on the separator that actually reaches the converted value.
    IValueMeta valueMeta = new ValueMetaNumber("n");
    String converted = valueMeta.getString(10000.23d);
    assertTrue(converted.contains(","), "Expected an Italian decimal separator, got: " + converted);
  }
}

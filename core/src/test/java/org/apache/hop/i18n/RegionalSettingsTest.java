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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Locale;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class RegionalSettingsTest {

  @BeforeEach
  void clearConfiguration() {
    HopConfig.getInstance().saveOption(RegionalSettings.STRING_REGIONAL_SETTINGS_SOURCE, null);
    HopConfig.getInstance().saveOption(RegionalSettings.STRING_REGIONAL_SETTINGS_LOCALE, null);
    LanguageChoice.getInstance().setDefaultLocale(Locale.forLanguageTag("en-US"));
    RegionalSettings.getInstance().reload();
  }

  @Test
  void defaultsToTheOperatingSystemWhenNothingIsConfigured() {
    RegionalSettings settings = RegionalSettings.getInstance();

    assertEquals(RegionalSettings.Source.OPERATING_SYSTEM, settings.getSource());
    assertEquals(settings.getOperatingSystemLocale(), settings.getEffectiveLocale());
  }

  @Test
  void effectiveLocaleFollowsTheOperatingSystemWhenSelected() {
    RegionalSettings settings = RegionalSettings.getInstance();
    settings.setSource(RegionalSettings.Source.OPERATING_SYSTEM);

    assertNotNull(settings.getOperatingSystemLocale());
    assertEquals(settings.getOperatingSystemLocale(), settings.getEffectiveLocale());
  }

  @Test
  void effectiveLocaleIsTheCustomOneWhenOverridden() {
    RegionalSettings settings = RegionalSettings.getInstance();
    settings.setSource(RegionalSettings.Source.CUSTOM);
    settings.setCustomLocale(Locale.ITALY);

    assertEquals(Locale.ITALY, settings.getEffectiveLocale());
  }

  @Test
  void unknownSourceValueFallsBackToTheOperatingSystem() {
    HopConfig.getInstance()
        .saveOption(RegionalSettings.STRING_REGIONAL_SETTINGS_SOURCE, "NOT_A_SOURCE");
    RegionalSettings.getInstance().reload();

    assertEquals(
        RegionalSettings.Source.OPERATING_SYSTEM, RegionalSettings.getInstance().getSource());
  }

  @Test
  void customSourceWithUnusableLocaleFallsBackToTheOperatingSystem() {
    HopConfig.getInstance()
        .saveOption(
            RegionalSettings.STRING_REGIONAL_SETTINGS_SOURCE,
            RegionalSettings.Source.CUSTOM.name());
    HopConfig.getInstance().saveOption(RegionalSettings.STRING_REGIONAL_SETTINGS_LOCALE, null);
    RegionalSettings.getInstance().reload();

    RegionalSettings settings = RegionalSettings.getInstance();
    assertEquals(RegionalSettings.Source.OPERATING_SYSTEM, settings.getSource());
    assertEquals(settings.getOperatingSystemLocale(), settings.getEffectiveLocale());
  }

  @Test
  void saveAndReloadRoundTripsTheConfiguration() {
    RegionalSettings settings = RegionalSettings.getInstance();
    settings.setSource(RegionalSettings.Source.CUSTOM);
    settings.setCustomLocale(Locale.ITALY);
    settings.save();

    settings.reload();

    assertEquals(RegionalSettings.Source.CUSTOM, settings.getSource());
    assertEquals(Locale.ITALY, settings.getCustomLocale());
  }
}

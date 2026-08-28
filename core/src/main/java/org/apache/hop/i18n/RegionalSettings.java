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

import java.util.Arrays;
import java.util.Locale;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;

/**
 * Holds the regional settings (decimal and grouping separators, currency, date formats) as a
 * concern separate from the interface language, which stays under {@link LanguageChoice}.
 *
 * <p>The effective locale is installed in the JVM as {@link Locale.Category#FORMAT}, while {@link
 * Locale#getDefault()} — the locale {@code ResourceBundle} resolves messages with — keeps carrying
 * the interface language.
 */
public class RegionalSettings {

  /** Where the regional settings come from. */
  public enum Source {
    /**
     * Follow the selected interface language, so that changing the language changes the formats
     * with it. This is a deliberate choice a user makes, not the source an unconfigured
     * installation falls back to.
     */
    LANGUAGE,
    /** Inherit them from the operating system Hop is running on. */
    OPERATING_SYSTEM,
    /** Use an explicitly selected locale. */
    CUSTOM
  }

  public static final String STRING_REGIONAL_SETTINGS_SOURCE = "RegionalSettingsSource";
  public static final String STRING_REGIONAL_SETTINGS_LOCALE = "RegionalSettingsLocale";

  /**
   * The locale the JVM started with, captured before anything can overwrite it. The first {@code
   * Locale.setDefault(language)} destroys this value and it cannot be recovered afterwards, so
   * {@link Source#OPERATING_SYSTEM} would have nothing to read without this field.
   */
  private static final Locale OPERATING_SYSTEM_LOCALE = Locale.getDefault();

  private static RegionalSettings instance;

  private Source source;
  private Locale customLocale;

  private RegionalSettings() {
    reload();
  }

  public static synchronized RegionalSettings getInstance() {
    if (instance == null) {
      instance = new RegionalSettings();
    }
    return instance;
  }

  /**
   * Re-reads the configuration, degrading to {@link Source#OPERATING_SYSTEM} on anything unusable.
   */
  public void reload() {
    String sourceValue =
        HopConfig.readOptionString(STRING_REGIONAL_SETTINGS_SOURCE, Source.OPERATING_SYSTEM.name());
    try {
      source = Source.valueOf(sourceValue);
    } catch (IllegalArgumentException e) {
      LogChannel.GENERAL.logBasic(
          "Unknown value '"
              + sourceValue
              + "' for option "
              + STRING_REGIONAL_SETTINGS_SOURCE
              + ", deriving regional settings from the operating system instead.");
      source = Source.OPERATING_SYSTEM;
    }

    String localeValue = HopConfig.readOptionString(STRING_REGIONAL_SETTINGS_LOCALE, null);
    customLocale = Utils.isEmpty(localeValue) ? null : EnvUtil.createLocale(localeValue);

    if (source == Source.CUSTOM && !isUsable(customLocale)) {
      LogChannel.GENERAL.logBasic(
          "Regional settings locale '"
              + localeValue
              + "' is not available in this JVM, deriving regional settings from the operating"
              + " system instead.");
      source = Source.OPERATING_SYSTEM;
    }
  }

  /** Persists the current source and custom locale. */
  public void save() {
    HopConfig.getInstance().saveOption(STRING_REGIONAL_SETTINGS_SOURCE, source.name());
    HopConfig.getInstance()
        .saveOption(
            STRING_REGIONAL_SETTINGS_LOCALE, customLocale == null ? null : customLocale.toString());
  }

  /** The locale actually used to format numbers, currencies and dates. */
  public Locale getEffectiveLocale() {
    return switch (source) {
      case OPERATING_SYSTEM -> OPERATING_SYSTEM_LOCALE;
      case CUSTOM -> customLocale;
      case LANGUAGE -> LanguageChoice.getInstance().getDefaultLocale();
    };
  }

  /**
   * Applies the regional settings for a headless run (hop-run, hop-server, REST), so those runs
   * honour the configuration of the machine they run on.
   *
   * <p>Distributed Beam and Spark workers are not covered by this method: they never load a {@code
   * hop-config.json} in the first place, so they fall back to the default source and format with
   * their own operating system locale regardless of what this method would apply.
   */
  public void applyHeadless() {
    // Under the default source this writes OPERATING_SYSTEM_LOCALE, which was captured from the
    // JVM's own initial default — precisely what a headless run already carries, including when it
    // was set with -Duser.language. Writing it back is therefore a no-op in practice.
    Locale formatLocale = getEffectiveLocale();
    if (formatLocale == null) {
      LogChannel.GENERAL.logBasic(
          "No usable regional settings locale is configured; leaving the format settings alone.");
      return;
    }
    Locale.setDefault(Locale.Category.FORMAT, formatLocale);
  }

  /**
   * Applies the interface language and then the regional settings, in that order.
   *
   * <p>The order is mandatory: {@code Locale.setDefault(Locale)} writes all three categories, so
   * setting the language after the regional settings would wipe the FORMAT category. For the same
   * reason the FORMAT category is always written back, even when the regional settings are derived
   * from the language and the two carry the same value.
   */
  public void applyGui() {
    Locale.setDefault(LanguageChoice.getInstance().getDefaultLocale());
    Locale formatLocale = getEffectiveLocale();
    if (formatLocale == null) {
      LogChannel.GENERAL.logBasic(
          "No usable regional settings locale is configured; leaving the format settings alone.");
      return;
    }
    Locale.setDefault(Locale.Category.FORMAT, formatLocale);
  }

  private static boolean isUsable(Locale locale) {
    return locale != null && Arrays.asList(Locale.getAvailableLocales()).contains(locale);
  }

  public Source getSource() {
    return source;
  }

  public void setSource(Source source) {
    this.source = source;
  }

  public Locale getCustomLocale() {
    return customLocale;
  }

  public void setCustomLocale(Locale customLocale) {
    this.customLocale = customLocale;
  }

  public Locale getOperatingSystemLocale() {
    return OPERATING_SYSTEM_LOCALE;
  }
}

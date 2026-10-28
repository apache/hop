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

package org.apache.hop.projects.environment;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.RegionalSettings;

/**
 * Applies a lifecycle environment's FORMAT locale and timezone on top of the installation regional
 * settings. Empty fields inherit. Invalid values are logged and inherit too, rather than aborting
 * environment enablement.
 *
 * <p>Inheriting means actively putting the installation values back: the locale and timezone are
 * JVM-wide, so an environment that leaves them alone would keep whatever the previously enabled
 * environment installed.
 */
public final class EnvironmentRegionalSettings {

  private EnvironmentRegionalSettings() {
    // utility
  }

  /**
   * Installs the environment's regional settings, or the installation ones for the fields the
   * environment leaves empty, and publishes the effective FORMAT locale and timezone as variables
   * so pipelines can see what they are running under.
   */
  public static void apply(
      ILogChannel log, LifecycleEnvironment environment, IVariables variables) {
    RegionalSettings installation = RegionalSettings.getInstance();
    String envName = environment == null ? "" : Const.NVL(environment.getName(), "");

    Locale formatLocale =
        environment == null ? null : parseFormatLocale(log, environment.getFormatLocale(), envName);
    TimeZone timeZone =
        environment == null ? null : parseTimeZone(log, environment.getTimeZone(), envName);

    Locale effectiveLocale =
        formatLocale != null ? formatLocale : installation.getEffectiveLocale();
    if (effectiveLocale != null) {
      Locale.setDefault(Locale.Category.FORMAT, effectiveLocale);
    }
    TimeZone effectiveTimeZone =
        timeZone != null ? timeZone : installation.getOperatingSystemTimeZone();
    TimeZone.setDefault(effectiveTimeZone);
    System.setProperty("user.timezone", effectiveTimeZone.getID());

    if (variables != null) {
      variables.setVariable(
          Const.HOP_FORMAT_LOCALE, Locale.getDefault(Locale.Category.FORMAT).toString());
      variables.setVariable(Const.HOP_TIMEZONE, TimeZone.getDefault().getID());
    }

    String source =
        formatLocale != null || timeZone != null
            ? "environment:" + envName
            : "installation:" + installation.getSource().name();
    RegionalSettings.logEffective(log, source);
  }

  private static Locale parseFormatLocale(ILogChannel log, String formatLocale, String envName) {
    if (StringUtils.isEmpty(formatLocale)) {
      return null;
    }
    Locale parsed = EnvUtil.createLocale(formatLocale);
    if (parsed == null || !Arrays.asList(Locale.getAvailableLocales()).contains(parsed)) {
      if (log != null) {
        log.logBasic(
            "Environment '"
                + envName
                + "' format locale '"
                + formatLocale
                + "' is not available in this JVM; inheriting installation regional settings.");
      }
      return null;
    }
    return parsed;
  }

  private static TimeZone parseTimeZone(ILogChannel log, String timeZoneId, String envName) {
    if (StringUtils.isEmpty(timeZoneId)) {
      return null;
    }
    if (!ZoneId.getAvailableZoneIds().contains(timeZoneId)) {
      if (log != null) {
        log.logBasic(
            "Environment '"
                + envName
                + "' timezone '"
                + timeZoneId
                + "' is not a recognised IANA id; inheriting the operating system timezone.");
      }
      return null;
    }
    return TimeZone.getTimeZone(timeZoneId);
  }
}

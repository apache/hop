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
 * settings. Empty fields inherit. Invalid values are logged and skipped rather than aborting
 * environment enablement.
 */
public final class EnvironmentRegionalSettings {

  private EnvironmentRegionalSettings() {
    // utility
  }

  /**
   * Installs the environment's regional settings (when present) and publishes the effective FORMAT
   * locale and timezone as variables so pipelines can see what they are running under.
   */
  public static void apply(
      ILogChannel log, LifecycleEnvironment environment, IVariables variables) {
    String source = "installation:" + RegionalSettings.getInstance().getSource().name();

    if (environment != null) {
      String envName = Const.NVL(environment.getName(), "");
      if (applyFormatLocale(log, environment.getFormatLocale(), envName)) {
        source = "environment:" + envName;
      }
      if (applyTimeZone(log, environment.getTimeZone(), envName)
          && source.startsWith("installation:")) {
        source = "environment:" + envName;
      }
    }

    if (variables != null) {
      variables.setVariable(
          Const.HOP_FORMAT_LOCALE, Locale.getDefault(Locale.Category.FORMAT).toString());
      variables.setVariable(Const.HOP_TIMEZONE, TimeZone.getDefault().getID());
    }

    RegionalSettings.logEffective(log, source);
  }

  private static boolean applyFormatLocale(ILogChannel log, String formatLocale, String envName) {
    if (StringUtils.isEmpty(formatLocale)) {
      return false;
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
      return false;
    }
    Locale.setDefault(Locale.Category.FORMAT, parsed);
    return true;
  }

  private static boolean applyTimeZone(ILogChannel log, String timeZoneId, String envName) {
    if (StringUtils.isEmpty(timeZoneId)) {
      return false;
    }
    if (!ZoneId.getAvailableZoneIds().contains(timeZoneId)) {
      if (log != null) {
        log.logBasic(
            "Environment '"
                + envName
                + "' timezone '"
                + timeZoneId
                + "' is not a recognised IANA id; leaving the JVM default timezone in place.");
      }
      return false;
    }
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneId);
    TimeZone.setDefault(timeZone);
    System.setProperty("user.timezone", timeZone.getID());
    return true;
  }
}

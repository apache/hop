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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import java.util.TimeZone;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class EnvironmentRegionalSettingsTest {

  private static final ILogChannel LOG = LogChannel.GENERAL;

  @Test
  void italianEnvironmentFormatsNumbersWithAComma() throws HopValueException {
    LifecycleEnvironment environment = new LifecycleEnvironment("it", "Testing", "p", null);
    environment.setFormatLocale("it_IT");

    EnvironmentRegionalSettings.apply(LOG, environment, new Variables());

    assertEquals(Locale.ITALY, Locale.getDefault(Locale.Category.FORMAT));
    IValueMeta valueMeta = new ValueMetaNumber("n");
    String converted = valueMeta.getString(10000.23d);
    assertTrue(converted.contains(","), "Expected an Italian decimal separator, got: " + converted);
  }

  @Test
  void switchingEnvironmentFlipsTheDecimalSeparator() throws HopValueException {
    LifecycleEnvironment italian = new LifecycleEnvironment("it", "Testing", "p", null);
    italian.setFormatLocale("it_IT");
    EnvironmentRegionalSettings.apply(LOG, italian, new Variables());
    assertTrue(new ValueMetaNumber("n").getString(10000.23d).contains(","));

    LifecycleEnvironment us = new LifecycleEnvironment("us", "Testing", "p", null);
    us.setFormatLocale("en_US");
    EnvironmentRegionalSettings.apply(LOG, us, new Variables());
    assertTrue(new ValueMetaNumber("n").getString(10000.23d).contains("."));
    assertEquals(Locale.US, Locale.getDefault(Locale.Category.FORMAT));
  }

  @Test
  void emptyEnvironmentFieldsLeaveTheInstallationSettingsInPlace() {
    Locale.setDefault(Locale.Category.FORMAT, Locale.GERMANY);
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));

    LifecycleEnvironment environment = new LifecycleEnvironment("dev", "Testing", "p", null);
    EnvironmentRegionalSettings.apply(LOG, environment, new Variables());

    assertEquals(Locale.GERMANY, Locale.getDefault(Locale.Category.FORMAT));
    assertEquals("Europe/Berlin", TimeZone.getDefault().getID());
  }

  @Test
  void invalidLocaleAndTimezoneAreIgnored() {
    Locale.setDefault(Locale.Category.FORMAT, Locale.US);
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    LifecycleEnvironment environment = new LifecycleEnvironment("bad", "Testing", "p", null);
    environment.setFormatLocale("not_A_Locale");
    environment.setTimeZone("Not/AZone");
    EnvironmentRegionalSettings.apply(LOG, environment, new Variables());

    assertEquals(Locale.US, Locale.getDefault(Locale.Category.FORMAT));
    assertEquals("UTC", TimeZone.getDefault().getID());
  }

  @Test
  void timezoneIsInstalledAndPublishedAsAVariable() {
    LifecycleEnvironment environment = new LifecycleEnvironment("be", "Testing", "p", null);
    environment.setTimeZone("Europe/Brussels");
    Variables variables = new Variables();

    EnvironmentRegionalSettings.apply(LOG, environment, variables);

    assertEquals("Europe/Brussels", TimeZone.getDefault().getID());
    assertEquals("Europe/Brussels", variables.getVariable(Const.HOP_TIMEZONE));
  }

  @Test
  void formatLocaleIsPublishedAsAVariable() {
    LifecycleEnvironment environment = new LifecycleEnvironment("be", "Testing", "p", null);
    environment.setFormatLocale("nl_BE");
    Variables variables = new Variables();

    EnvironmentRegionalSettings.apply(LOG, environment, variables);

    assertEquals("nl_BE", variables.getVariable(Const.HOP_FORMAT_LOCALE));
  }
}

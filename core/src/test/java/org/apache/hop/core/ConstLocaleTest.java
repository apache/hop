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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Locale;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ConstLocaleTest {

  @Test
  void separatorsFollowTheFormatCategoryAtCallTime() {
    Locale.setDefault(Locale.Category.FORMAT, Locale.US);
    assertEquals('.', Const.getDefaultDecimalSeparator());
    assertEquals(',', Const.getDefaultGroupingSeparator());

    Locale.setDefault(Locale.Category.FORMAT, Locale.ITALY);
    assertEquals(',', Const.getDefaultDecimalSeparator());
    assertEquals('.', Const.getDefaultGroupingSeparator());
  }

  @Test
  void currencySymbolFollowsTheFormatCategoryAtCallTime() {
    Locale.setDefault(Locale.Category.FORMAT, Locale.US);
    assertEquals("$", Const.getDefaultCurrencySymbol());

    Locale.setDefault(Locale.Category.FORMAT, Locale.ITALY);
    assertEquals("€", Const.getDefaultCurrencySymbol());
  }

  /**
   * The symbols are cached because these accessors sit on the ValueMetaBase construction path.
   * Repeated reads without a locale change must stay stable, and a locale change must still be
   * picked up - that second half is what makes the cache safe rather than merely fast.
   */
  @Test
  void cachedSymbolsAreStableAcrossRepeatedReadsAndStillFollowALocaleChange() {
    Locale.setDefault(Locale.Category.FORMAT, Locale.ITALY);
    char first = Const.getDefaultDecimalSeparator();
    char second = Const.getDefaultDecimalSeparator();
    assertEquals(first, second);
    assertEquals(',', first);

    Locale.setDefault(Locale.Category.FORMAT, Locale.US);
    assertEquals('.', Const.getDefaultDecimalSeparator());

    Locale.setDefault(Locale.Category.FORMAT, Locale.ITALY);
    assertEquals(',', Const.getDefaultDecimalSeparator());
  }
}

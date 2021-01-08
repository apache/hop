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

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.util.EnvUtil;

import java.util.Locale;

public class LanguageChoice {
  private static final String STRING_DEFAULT_LOCALE = "LocaleDefault";

  private static LanguageChoice choice;

  private Locale defaultLocale;

  private LanguageChoice() {
    String defaultLocaleString = HopConfig.readOptionString( STRING_DEFAULT_LOCALE, null );
    if ( defaultLocaleString == null ) {
      defaultLocale = Locale.getDefault();
      HopConfig.getInstance().saveOption( STRING_DEFAULT_LOCALE, defaultLocale.toString() );
    } else {
      defaultLocale = EnvUtil.createLocale( defaultLocaleString );
    }
  }

  public static final LanguageChoice getInstance()  {
    if ( choice == null ) {
      choice = new LanguageChoice();
    }
    return choice;
  }

  /**
   * @return Returns the defaultLocale.
   */
  public Locale getDefaultLocale() {
    return defaultLocale;
  }

  /**
   * @param defaultLocale The defaultLocale to set.
   */
  public void setDefaultLocale( Locale defaultLocale ) {
    this.defaultLocale = defaultLocale;
    HopConfig.getInstance().saveOption( STRING_DEFAULT_LOCALE, defaultLocale.toString() );
  }
}

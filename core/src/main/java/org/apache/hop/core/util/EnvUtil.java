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

package org.apache.hop.core.util;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;

public class EnvUtil {
  private static Properties env = null;

  private EnvUtil() {
  }

  /**
   * This method is written especially for weird JVM's like
   *
   * @param key The key, the name of the environment variable to return
   * @param def The default value to return in case the key can't be found
   * @return The value of a System environment variable in the java virtual machine. If the key is not present, the
   * variable is not defined and the default value is returned.
   */
  public static final String getSystemProperty( String key, String def ) {
    return System.getProperty( key, def );
  }

  /**
   * @param key The key, the name of the environment variable to return
   * @return The value of a System environment variable in the java virtual machine. If the key is not present, the
   * variable is not defined and null returned.
   */
  public static final String getSystemProperty( String key ) {
    return getSystemProperty( key, null );
  }

  /**
   * @param key The key, the name of the environment variable to clear
   * @return The value of a System environment variable in the java virtual machine. If the key is not present, the
   * variable is not defined and null returned.
   */
  public static final String clearSystemProperty( String key ) {
    return System.clearProperty( key );
  }

  /**
   * Returns an available java.util.Locale object for the given localeCode.
   * <p>
   * The localeCode code can be case insensitive, if it is available the method will find it and return it.
   *
   * @param localeCode
   * @return java.util.Locale.
   */
  public static Locale createLocale( String localeCode ) {
    if ( Utils.isEmpty( localeCode ) ) {
      return null;
    }
    StringTokenizer parser = new StringTokenizer( localeCode, "_" );
    if ( parser.countTokens() == 2 ) {
      return new Locale( parser.nextToken(), parser.nextToken() );
    }
    if ( parser.countTokens() == 3 ) {
      return new Locale( parser.nextToken(), parser.nextToken(), parser.nextToken() );
    }
    return new Locale( localeCode );
  }

  public static TimeZone createTimeZone( String timeZoneId ) {

    TimeZone resultTimeZone = null;
    if ( !Utils.isEmpty( timeZoneId ) ) {
      return TimeZone.getTimeZone( timeZoneId );
    } else {
      resultTimeZone = TimeZone.getDefault();
    }
    return resultTimeZone;
  }

  public static String[] getTimeZones() {
    String[] timeZones = TimeZone.getAvailableIDs();
    Arrays.sort( timeZones );
    return timeZones;
  }

  public static String[] getLocaleList() {
    Locale[] locales = Locale.getAvailableLocales();
    String[] strings = new String[ locales.length ];
    for ( int i = 0; i < strings.length; i++ ) {
      strings[ i ] = locales[ i ].toString();
    }
    Arrays.sort( strings );
    return strings;
  }
}

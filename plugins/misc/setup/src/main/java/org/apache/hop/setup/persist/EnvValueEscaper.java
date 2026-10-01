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

package org.apache.hop.setup.persist;

import org.apache.hop.setup.HopSetupException;

/** Quote / validate values written into shell, cmd, and PowerShell snippets. */
public final class EnvValueEscaper {

  private EnvValueEscaper() {}

  public static String shellSingleQuoted(String name, String value) throws HopSetupException {
    rejectControlOrQuote(name, value, '\'');
    return "'" + value + "'";
  }

  /**
   * Renders a {@code set} statement assigning the value in a cmd script. Values without a double
   * quote are wrapped in quotes, which already neutralises {@code & | < > ^}. A value carrying its
   * own quotes cannot be wrapped, so the unquoted form is used and cmd metacharacters are rejected.
   */
  public static String cmdAssignment(String name, String value) throws HopSetupException {
    rejectNewlines(name, value);
    if (value.indexOf('!') >= 0) {
      throw new HopSetupException(
          "Value for "
              + name
              + " must not contain '!': launchers run with delayed expansion enabled");
    }
    String escaped = value.replace("%", "%%");
    if (escaped.indexOf('"') < 0) {
      return "set \"" + name + "=" + escaped + "\"";
    }
    if (containsAny(escaped, "&|<>^")) {
      throw new HopSetupException(
          "Value for "
              + name
              + " combines double quotes with characters that cannot be written safely to a"
              + " Windows cmd script");
    }
    return "set " + name + "=" + escaped;
  }

  private static boolean containsAny(String value, String characters) {
    for (int i = 0; i < characters.length(); i++) {
      if (value.indexOf(characters.charAt(i)) >= 0) {
        return true;
      }
    }
    return false;
  }

  public static String powershellSingleQuoted(String name, String value) throws HopSetupException {
    rejectNewlines(name, value);
    return "'" + value.replace("'", "''") + "'";
  }

  public static void rejectNewlines(String name, String value) throws HopSetupException {
    if (value != null && (value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0)) {
      throw new HopSetupException("Value for " + name + " must not contain a newline");
    }
  }

  private static void rejectControlOrQuote(String name, String value, char quote)
      throws HopSetupException {
    rejectNewlines(name, value);
    if (value != null && value.indexOf(quote) >= 0) {
      throw new HopSetupException(
          "Value for " + name + " must not contain the character '" + quote + "'");
    }
  }
}
